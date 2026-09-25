package io.github.wpunit13.mutator.junit5;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.AppliedMutantTracker;
import io.github.wpunit13.mutator.MutantBootstrap;
import io.github.wpunit13.mutator.MutantRegistry;
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess;
import io.github.wpunit13.mutator.catalog.MutationCatalogIo;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.StructuralInvalidation;
import io.github.wpunit13.mutator.report.AppliedMarkerStore;
import io.github.wpunit13.mutator.report.ReportSink;
import io.github.wpunit13.mutator.report.ReportWriter;
import io.github.wpunit13.mutator.reset.SessionResetFacade;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

/**
 * Direct JUnit 5 extension orchestrating in-process Spark mutation testing.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@code BeforeAll}: ensures {@code MutatorSparkExtension} is registered for SparkSession.</li>
 *   <li>{@code BeforeEach}: if driving a mutation run, sets the active mutant on {@link MutantRegistry}.</li>
 *   <li>{@code AfterEach}: clears active mutant via {@link MutantRegistry#clearActiveMutant(String)}
 *       and cleans Catalyst plan and table caches.</li>
 *   <li>{@code TestExecutionExceptionHandler}: catches {@link AssertionError} in mutation mode and records KILLED.</li>
 *   <li>{@code AfterAll}: in baseline direct runs, iterates discovered mutants and emits final reports.</li>
 * </ol>
 */
public class SparkMutatorExtension implements
        BeforeAllCallback,
        BeforeEachCallback,
        AfterEachCallback,
        AfterAllCallback,
        TestExecutionExceptionHandler {

    public static final String PROPERTY_MUTATOR_DISABLED = "spark.mutator.disabled";
    public static final String PROPERTY_MUTATOR_ENABLED = "spark.mutator.enabled";
    public static final String PROPERTY_ACTIVE_MUTANT = "spark.mutator.active.mutant";
    public static final String EXTENSION_CLASS = "io.github.wpunit13.mutator.MutatorSparkExtension";

    private static final String SPARK_SQL_EXTENSIONS = "spark.sql.extensions";
    private static final String KEY_TEST_START_TIME = "testStartTime";
    private static final String KEY_HANDLED_FAILURE = "handledFailure";
    private static final String KEY_BASELINE_START = "baselineStartTime";
    private static final String STATUS_ERRORED = "ERRORED";
    private static final String PROP_TIMEOUT_MULTIPLIER = "spark.mutator.timeoutMultiplier";
    private static final String PROP_TIMEOUT_ENFORCED = "spark.mutator.timeoutEnforced";

    /**
     * Grace period per escalation channel (docs/ARCHITECTURE.md §6.2 ladder,
     * same defaults as the PySpark watchdog). Package-private mutable only so
     * tests can shrink it; production code must not write it.
     */
    static volatile long watchdogGraceMillis = 5_000L;

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(SparkMutatorExtension.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(NAMESPACE);
    }

    private boolean isDisabled(ExtensionContext context) {
        if ("true".equalsIgnoreCase(System.getProperty(PROPERTY_MUTATOR_DISABLED))
                || "false".equalsIgnoreCase(System.getProperty(PROPERTY_MUTATOR_ENABLED))) {
            return true;
        }
        return context.getTestClass()
                .map(c -> c.getAnnotation(EnableSparkMutationTesting.class))
                .map(ann -> !ann.enabled())
                .orElse(false);
    }

    private void ensureSparkExtensionActive() {
        String existing = System.getProperty(SPARK_SQL_EXTENSIONS);
        if (existing == null || existing.isBlank()) {
            System.setProperty(SPARK_SQL_EXTENSIONS, EXTENSION_CLASS);
        } else if (!existing.contains(EXTENSION_CLASS)) {
            System.setProperty(SPARK_SQL_EXTENSIONS, existing + "," + EXTENSION_CLASS);
        }
    }

    /**
     * Cleans Catalyst caches across active/default SparkSessions using SessionResetFacade.
     */
    public static void cleanCatalystCache() {
        try {
            Object spark = activeOrDefaultSessionOrNull();
            if (spark != null) {
                SessionResetFacade.resetSessionState(spark, 0L);
            }
        } catch (Throwable ignored) {
            // Spark may not be present or no active session; safely continue
        }
    }

    private static Object activeOrDefaultSessionOrNull() throws Exception {
        Class<?> sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession");
        Object opt = sparkSessionClass.getMethod("getActiveSession").invoke(null);
        Method isDefined = opt.getClass().getMethod("isDefined");
        if ((boolean) isDefined.invoke(opt)) {
            return opt.getClass().getMethod("get").invoke(opt);
        }
        Object defaultOpt = sparkSessionClass.getMethod("getDefaultSession").invoke(null);
        if ((boolean) isDefined.invoke(defaultOpt)) {
            return defaultOpt.getClass().getMethod("get").invoke(defaultOpt);
        }
        return null;
    }

    // 1. BeforeAll: ensure MutatorSparkExtension is active on JVM SparkSession
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (isDisabled(context)) {
            return;
        }
        ensureSparkExtensionActive();

        MutantBootstrap.activateFromSystemProperties();

        boolean isBaseline = (MutantRegistry.getInstance().getActiveMutantOrNull() == null);
        if (isBaseline && MutantBootstrap.phaseOrNull() == null) {
            // In-process standalone: scope this class's loop to its own
            // discoveries AND its own outcomes. The JVM-wide catalog singleton
            // would otherwise leak earlier annotated classes' mutants into
            // this class's loop (cross-class contamination), and the JVM-wide
            // outcome sink would leak their verdicts into this class's gate
            // counts. Fork mode is excluded: its baseline fork runs the whole
            // suite and must accumulate every class's mutants.
            MutationCatalogAccess.resetForDiscovery();
            ReportSink.clearForTesting();
        }
        getStore(context).put("baselineMode", isBaseline);
        getStore(context).put(KEY_BASELINE_START, System.currentTimeMillis());
    }

    // 2. BeforeEach: if driving mutation run, set active mutant
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (isDisabled(context)) {
            return;
        }

        MutantBootstrap.activateFromSystemProperties();

        getStore(context).put(KEY_TEST_START_TIME, System.currentTimeMillis());
        getStore(context).remove(KEY_HANDLED_FAILURE);
        getStore(context).remove("failureDetail");
    }

    // 4. TestExecutionExceptionHandler: catch AssertionError -> record KILLED
    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        if (isDisabled(context)) {
            throw throwable;
        }

        // External orchestration: never suppress a test failure. The Mojo
        // classifies the mutant from Surefire's non-zero exit code; swallowing
        // the assertion here would make a killed mutant look like a SURVIVED.
        if (MutantBootstrap.phaseOrNull() != null) {
            throw throwable;
        }

        String activeMutant = MutantRegistry.getInstance().getActiveMutantOrNull();
        if (activeMutant != null) {
            Throwable root = unwrap(throwable);
            Long start = getStore(context).get(KEY_TEST_START_TIME, Long.class);
            long elapsed = start != null ? (System.currentTimeMillis() - start) : 0L;
            String failureDetail = root.getMessage() != null ? root.getMessage() : root.toString();
            getStore(context).put(KEY_HANDLED_FAILURE, true);

            if (root instanceof AssertionError) {
                recordOutcomeQuietly(activeMutant, "KILLED", elapsed, failureDetail);
                // Suppress AssertionError so test runner indicates mutant was successfully killed
                return;
            } else {
                // A mutation that was applied but structurally invalidated the
                // plan (the mutated node stopped producing a column the rest
                // of the plan references) is a designed skip, not an engine
                // failure: excluded from the score and the gate's not-applied
                // denominator, visible in the report with the reason.
                boolean mutationApplied = activeMutant.equals(AppliedMutantTracker.lastOrNull());
                if (mutationApplied && StructuralInvalidation.matches(
                        StructuralInvalidation.renderChain(throwable))) {
                    recordOutcomeQuietly(activeMutant, "SKIPPED", elapsed,
                            "mutation structurally invalidated the plan: " + failureDetail);
                } else {
                    recordOutcomeQuietly(activeMutant, STATUS_ERRORED, elapsed, failureDetail);
                }
                throw throwable;
            }
        }

        // In baseline mode, record baseline failure and let exception surface normally
        getStore(context).put("baselineFailed", true);
        throw throwable;
    }

    private void recordOutcomeQuietly(String mutantId, String status, long elapsed, String detail) {
        try {
            ReportSink.recordOutcome(mutantId, status, elapsed, detail);
        } catch (IllegalStateException ignored) {
            // Already recorded (write-once sink)
        }
    }

    // 3. AfterEach: clear active mutant and clean Catalyst cache
    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        if (isDisabled(context)) {
            return;
        }

        String activeMutant = MutantRegistry.getInstance().getActiveMutantOrNull();
        try {
            if (activeMutant != null) {
                recordSurvivedIfUnhandled(context, activeMutant);
            }
        } finally {
            try {
                if (activeMutant != null) {
                    MutantRegistry.getInstance().clearActiveMutant(activeMutant);
                }
            } finally {
                cleanCatalystCache();
            }
        }
    }

    private void recordSurvivedIfUnhandled(ExtensionContext context, String activeMutant) {
        Boolean handled = getStore(context).get(KEY_HANDLED_FAILURE, Boolean.class);
        if ((handled == null || !handled) && MutantBootstrap.phaseOrNull() == null) {
            Long start = getStore(context).get(KEY_TEST_START_TIME, Long.class);
            long elapsed = start != null ? (System.currentTimeMillis() - start) : 0L;
            recordOutcomeQuietly(activeMutant, "SURVIVED", elapsed, null);
        }
    }

    // 5. AfterAll: if all mutants tested, generate mutation-report.json
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (isDisabled(context)) {
            return;
        }

        // External orchestration (Maven Mojo): the extension is a bridge, not
        // an orchestrator. On baseline completion it serializes the discovered
        // catalog to the shared output directory so the Mojo (a different JVM)
        // can read it back. On mutant completion it persists the applied
        // marker so the Mojo can distinguish "a test killed the mutant" from
        // "the mutation never executed". Outcomes and reports are the Mojo's
        // responsibility.
        if (MutantBootstrap.phaseOrNull() != null) {
            if (MutantBootstrap.PHASE_BASELINE.equals(MutantBootstrap.phaseOrNull())) {
                String outputDir = MutantBootstrap.outputDirectoryOrNull();
                if (outputDir != null) {
                    MutationCatalogIo.writeCatalogJson(Path.of(outputDir), MutationCatalogAccess.allEntries());
                }
            } else if (MutantBootstrap.PHASE_MUTANT.equals(MutantBootstrap.phaseOrNull())) {
                writeAppliedMarkerOrThrow();
            }
            return;
        }

        // Standalone in-process mode (no external phase).
        Boolean baselineMode = getStore(context).get("baselineMode", Boolean.class);
        Boolean baselineFailed = getStore(context).get("baselineFailed", Boolean.class);

        try {
            if (Boolean.TRUE.equals(baselineMode) && !Boolean.TRUE.equals(baselineFailed)) {
                Long baselineStart = getStore(context).get(KEY_BASELINE_START, Long.class);
                long baselineElapsed = baselineStart != null
                        ? System.currentTimeMillis() - baselineStart
                        : 0L;
                runInProcessMutations(context, baselineElapsed);
            }
        } finally {
            // WP-25: the report must survive a watchdog abandon (partial
            // results with a TIMED_OUT tail beat no artifact), so finalize
            // runs even when the loop threw.
            ReportSink.finalizeAndWriteReports();
        }

        // WP-24 governance gate: real-failure ERRORED is zero-tolerance (a
        // dead session or shim violation means the harness/engine is broken);
        // the designed not-applied population is ratio-gated, optionally scoped
        // to an accountable set of operator families via
        // spark.mutator.notAppliedExemptMutators. Runs after the report is
        // written so the artifact survives the failure.
        List<String> populationViolations = ReportWriter.evaluateGateViolations(
                MutationCatalogAccess.allEntries().size(),
                ReportSink.countByStatus(MutantStatus.ERRORED),
                ReportSink.countByStatus(MutantStatus.NOT_APPLIED),
                ReportSink.countByStatus(MutantStatus.SKIPPED),
                intProperty(MutantBootstrap.PROP_MAX_ERRORED_COUNT,
                        ReportWriter.DEFAULT_MAX_ERRORED_COUNT),
                doubleProperty(MutantBootstrap.PROP_MAX_NOT_APPLIED_RATIO,
                        ReportWriter.DEFAULT_MAX_NOT_APPLIED_RATIO),
                ReportSink.familyStats(),
                ReportWriter.parseOperatorTypesCsv(
                        System.getProperty(MutantBootstrap.PROP_NOT_APPLIED_EXEMPT_MUTATORS)));
        if (!populationViolations.isEmpty()) {
            throw new IllegalStateException(
                    "WP-24 governance gate failed: " + String.join("; ", populationViolations));
        }

        // Optional in-process quality gate. Only enforced when the caller
        // explicitly sets a minimum (via surefire systemPropertyVariables or
        // argLine); unset means off, preserving non-blocking IDE / plain
        // `mvn test` behavior. The report is written above even when the gate
        // fails, so CI still has the artifact explaining the shortfall.
        String minScoreProp = System.getProperty(MutantBootstrap.PROP_MIN_MUTATION_SCORE);
        if (minScoreProp != null && !minScoreProp.isBlank()) {
            double minScore = Double.parseDouble(minScoreProp.trim());
            double score = ReportSink.computeMutationScore();
            if (minScore > 0.0 && score < minScore) {
                throw new IllegalStateException(
                        "Mutation score " + score + "% is below minimum " + minScore + "%.");
            }
        }
    }

    private static int intProperty(String key, int fallback) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static double doubleProperty(String key, double fallback) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Bridge-mode applied-mutation honesty guard (external {@code mutant}
     * phase, after all tests ran).
     *
     * <p>The fork must prove that the active mutant's rewrite actually
     * executed before the coordinator is allowed to classify the fork's exit
     * code as KILLED or SURVIVED. The Catalyst rule records the applied fact
     * in {@link AppliedMutantTracker} immediately after a rewrite; if the
     * recorded id does not equal the fork's active mutant (system property,
     * because {@code afterEach} already cleared the registry), the mutation
     * never executed and this fork fails loudly instead of masquerading as a
     * survivor. The marker is written only after the check passes.
     */
    private void writeAppliedMarkerOrThrow() {
        String activeMutant = System.getProperty(MutantBootstrap.PROP_ACTIVE_MUTANT);
        String applied = AppliedMutantTracker.lastOrNull();
        if (activeMutant == null || activeMutant.isBlank() || !activeMutant.trim().equals(applied)) {
            throw new IllegalStateException(
                    "Mutation was not applied (coordinate matched no plan node): active mutant '"
                            + activeMutant + "', last applied "
                            + (applied == null ? "<none>" : "'" + applied + "'"));
        }
        String outputDir = MutantBootstrap.outputDirectoryOrNull();
        if (outputDir == null || outputDir.isBlank()) {
            throw new IllegalStateException(
                    "Mutation '" + activeMutant + "' was applied but '"
                            + MutantBootstrap.PROP_OUTPUT_DIRECTORY
                            + "' is not set; cannot persist the applied marker.");
        }
        try {
            AppliedMarkerStore.write(Path.of(outputDir), activeMutant.trim());
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could not write applied marker for mutant '" + activeMutant + "'.", e);
        }
    }

    private void runInProcessMutations(ExtensionContext context, long baselineElapsedMillis) throws IOException {
        JsonNode catalogNode = readCatalogArrayOrNull();
        if (catalogNode == null) {
            return;
        }

        Class<?> testClass = context.getRequiredTestClass();

        // Per-mutant deadline, mirroring the Mojo's semantics
        // (MutationLoopCoordinator): ceil(baseline elapsed × multiplier).
        double multiplier = doubleProperty(PROP_TIMEOUT_MULTIPLIER, 2.0);
        long deadlineMillis = baselineElapsedMillis > 0
                ? (long) Math.ceil(baselineElapsedMillis * multiplier)
                : 1000L;
        // WP-25: mark the report's config echo as deadline-enforcing before
        // the loop runs (ReportWriter resolves the property at finalize time).
        System.setProperty(PROP_TIMEOUT_ENFORCED, "true");

        for (JsonNode mutantEntry : catalogNode) {
            if (!runSingleMutant(testClass, mutantEntry, deadlineMillis)) {
                throw new IllegalStateException(
                        "WP-25 watchdog abandoned the mutation loop: a mutant's re-run thread "
                                + "ignored cancel + interrupt for " + (2 * watchdogGraceMillis)
                                + "ms; partial report flushed with a TIMED_OUT tail.");
            }
        }
    }

    private JsonNode readCatalogArrayOrNull() throws IOException {
        String catalogJson = MutationCatalogAccess.getFullCatalogJson();
        JsonNode catalogNode = MAPPER.readTree(catalogJson);
        if (catalogNode == null || !catalogNode.isArray() || catalogNode.isEmpty()) {
            return null;
        }
        return catalogNode;
    }

    /**
     * Runs one mutant's re-run under the WP-25 per-mutant deadline.
     *
     * <p>The re-run executes on a fresh daemon worker thread while this (the
     * loop) thread waits with a deadline — the two-channel split: the waiter
     * is never the blocked thread, so the control channel stays live while
     * the execution channel is wedged inside a hung Spark job.
     *
     * @return false when the watchdog abandoned the loop (worker unkillable);
     *         true otherwise (outcome recorded, loop may continue).
     */
    private boolean runSingleMutant(Class<?> testClass, JsonNode mutantEntry, long deadlineMillis) {
        String mutantId = mutantEntry.get("mutantId").asText();
        Set<String> mappedTestNames = parseMappedTestNames(mutantEntry);

        long start = System.currentTimeMillis();
        boolean timedOut = false;
        boolean abandoned = false;
        Throwable root = null;

        FutureTask<Throwable> reRun = new FutureTask<>(() -> {
            try {
                executeTestsForMutant(testClass, mappedTestNames);
                return null;
            } catch (Throwable t) {
                return unwrap(t);
            }
        });

        try {
            // Honesty-guard baseline: forget the previous mutant's applied
            // fact so a mismatch below can only come from THIS mutant.
            AppliedMutantTracker.clear();
            MutantRegistry.getInstance().setActiveMutant(mutantId);
            cleanCatalystCache();

            Thread worker = new Thread(reRun, "spark-mutator-mutant-" + mutantId);
            worker.setDaemon(true); // a wedged worker must never block JVM exit
            worker.start();
            try {
                root = reRun.get(deadlineMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                timedOut = true;
                if (!escalateToCompletion(worker)) {
                    // In-process analog of the PySpark stage-3 breaker: the
                    // worker ignored both cancellation channels. Record the
                    // deadline hit, abandon the loop, and let afterAll's
                    // finally flush the partial report. The wedged daemon is
                    // left to die with the JVM (WP-19: this path must never
                    // terminate the user's JVM, so no thread kill / exit).
                    abandoned = true;
                    recordOutcomeQuietly(mutantId, "TIMED_OUT",
                            System.currentTimeMillis() - start,
                            "timed out (> " + deadlineMillis + "ms); re-run thread unkillable "
                                    + "after cancel + interrupt, abandoning loop");
                    return false;
                }
            } catch (ExecutionException e) {
                root = unwrap(e.getCause() == null ? e : e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                root = e;
            }
        } catch (Throwable t) {
            root = unwrap(t);
        } finally {
            if (!abandoned) {
                try {
                    if (mutantId.equals(MutantRegistry.getInstance().getActiveMutantOrNull())) {
                        MutantRegistry.getInstance().clearActiveMutant(mutantId);
                    } else if (MutantRegistry.getInstance().getActiveMutantOrNull() != null) {
                        MutantRegistry.getInstance().reset();
                    }
                } finally {
                    cleanCatalystCache();
                }
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        if (timedOut) {
            // Deadline precedence mirrors the fork path's classify() order: a
            // mutant that hung may have applied the mutation without
            // finishing, so the deadline hit outranks whatever the
            // interrupted re-run threw.
            recordOutcomeQuietly(mutantId, "TIMED_OUT", elapsed,
                    "timed out (> " + deadlineMillis + "ms)");
            return true;
        }

        boolean killed = false;
        String failureDetail = null;
        boolean errored = false;
        if (root instanceof AssertionError) {
            killed = true;
            failureDetail = root.getMessage() != null ? root.getMessage() : root.toString();
        } else if (root != null) {
            errored = true;
            // Render the full cause chain: the structural-invalidation markers
            // (plan binding/resolution errors) live in nested causes, not the
            // wrapper's message.
            failureDetail = "Unhandled exception: " + StructuralInvalidation.renderChain(root);
        }
        recordMutantOutcome(mutantId, killed, errored, failureDetail, elapsed);
        return true;
    }

    /**
     * Escalation ladder (docs/ARCHITECTURE.md §6.2, ported in-process):
     * channel 1 cancels the session's running Spark jobs from THIS thread;
     * channel 2 interrupts the worker. The fork path's third channel —
     * killing the process — is unavailable here (the loop runs in the user's
     * test JVM).
     *
     * @return true if the worker terminated, false if it survived both
     *         channels (the in-process analog of stage-3
     *         DriverUnresponsiveError).
     */
    private static boolean escalateToCompletion(Thread worker) {
        cancelAllJobsQuietly();
        joinQuietly(worker, watchdogGraceMillis);
        if (!worker.isAlive()) {
            return true;
        }
        worker.interrupt();
        joinQuietly(worker, watchdogGraceMillis);
        return !worker.isAlive();
    }

    private static void joinQuietly(Thread worker, long millis) {
        try {
            worker.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Control channel: cancels all jobs on the active/default session's
     * SparkContext. The user's test code sets no job group, so the targeted
     * {@code cancelJobGroup} of the fork/PySpark paths degrades to
     * {@code cancelAllJobs} here.
     */
    private static void cancelAllJobsQuietly() {
        try {
            Object spark = activeOrDefaultSessionOrNull();
            if (spark == null) {
                return;
            }
            Object sc = Class.forName("org.apache.spark.sql.SparkSession")
                    .getMethod("sparkContext").invoke(spark);
            sc.getClass().getMethod("cancelAllJobs").invoke(sc);
        } catch (Throwable ignored) {
            // No session/context to cancel; the interrupt channel still runs.
        }
    }

    private Set<String> parseMappedTestNames(JsonNode mutantEntry) {
        Set<String> mappedTestNames = new HashSet<>();
        JsonNode mappedTestsNode = mutantEntry.get("mappedTestIds");
        if (mappedTestsNode != null && mappedTestsNode.isArray()) {
            for (JsonNode t : mappedTestsNode) {
                mappedTestNames.add(t.asText());
            }
        }
        return mappedTestNames;
    }

    private void recordMutantOutcome(
            String mutantId, boolean killed, boolean errored, String failureDetail, long elapsed) {
        // A mutant whose rewrite never executed proves nothing: report it
        // ERRORED instead of SURVIVED (a fake survivor corrupts the score).
        boolean mutationApplied = mutantId.equals(AppliedMutantTracker.lastOrNull());
        if (killed) {
            recordOutcomeQuietly(mutantId, "KILLED", elapsed, failureDetail);
        } else if (errored) {
            if (mutationApplied && failureDetail != null
                    && StructuralInvalidation.matches(failureDetail)) {
                // Applied but the mutated plan could not execute: designed
                // skip, not an engine/harness failure.
                recordOutcomeQuietly(mutantId, "SKIPPED", elapsed,
                        "mutation structurally invalidated the plan: " + failureDetail);
            } else {
                recordOutcomeQuietly(mutantId, STATUS_ERRORED, elapsed, failureDetail);
            }
        } else if (!mutationApplied) {
            // WP-24: the rewrite never executed (node hidden inside a cache,
            // pruned stub, shape that never ran). Designed and shape-dependent
            // — distinct from a harness/engine failure.
            recordOutcomeQuietly(mutantId, "NOT_APPLIED", elapsed,
                    "mutation was not applied (coordinate matched no plan node)");
        } else {
            recordOutcomeQuietly(mutantId, "SURVIVED", elapsed, null);
        }
    }

    private void executeTestsForMutant(Class<?> testClass, Set<String> mappedTestNames) throws ReflectiveOperationException {
        Constructor<?> ctor = testClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object testInstance = ctor.newInstance();

        List<Method> beforeAllMethods = findMethodsWithAnnotation(testClass, BeforeAll.class);
        List<Method> afterAllMethods = findMethodsWithAnnotation(testClass, AfterAll.class);
        List<Method> beforeMethods = findMethodsWithAnnotation(testClass, BeforeEach.class);
        List<Method> afterMethods = findMethodsWithAnnotation(testClass, AfterEach.class);
        List<Method> testMethods = findTestMethods(testClass, mappedTestNames);

        // Re-run the FULL class lifecycle, not just the per-test one. The
        // outer run's @AfterAll has typically already torn down whatever
        // @BeforeAll built (e.g. spark.stop()), so a re-run that skipped
        // @BeforeAll would execute every mutant against that dead session and
        // classify them all ERRORED. Re-invoking @BeforeAll/@AfterAll per
        // mutant mirrors the fork-per-mutant path, where each mutant gets a
        // fresh JVM: SparkSession.builder().getOrCreate() sees the stopped
        // context and builds a fresh one (SparkSession checks isStopped on the
        // cached sessions; SparkContext.stop() cleared the active context).
        for (Method beforeAll : beforeAllMethods) {
            beforeAll.setAccessible(true);
            beforeAll.invoke(testInstance);
        }
        try {
            for (Method testMethod : testMethods) {
                try {
                    for (Method before : beforeMethods) {
                        before.setAccessible(true);
                        before.invoke(testInstance);
                    }
                    testMethod.setAccessible(true);
                    testMethod.invoke(testInstance);
                } finally {
                    for (Method after : afterMethods) {
                        try {
                            after.setAccessible(true);
                            after.invoke(testInstance);
                        } catch (Throwable ignored) {
                            // AfterEach cleanup must never mask the test method's own outcome
                        }
                    }
                }
            }
        } finally {
            for (Method afterAll : afterAllMethods) {
                try {
                    afterAll.setAccessible(true);
                    afterAll.invoke(testInstance);
                } catch (Throwable ignored) {
                    // AfterAll cleanup must never mask the test methods' own outcome
                }
            }
        }
    }

    private List<Method> findMethodsWithAnnotation(Class<?> testClass, Class<? extends java.lang.annotation.Annotation> annotation) {
        List<Method> methods = new ArrayList<>();
        for (Method m : testClass.getDeclaredMethods()) {
            if (m.isAnnotationPresent(annotation)) {
                methods.add(m);
            }
        }
        return methods;
    }

    private List<Method> findTestMethods(Class<?> testClass, Set<String> mappedTestNames) {
        List<Method> result = new ArrayList<>();
        for (Method m : testClass.getDeclaredMethods()) {
            if (m.isAnnotationPresent(Test.class)
                    && (mappedTestNames.isEmpty()
                        || mappedTestNames.contains(m.getName())
                        || mappedTestNames.contains(testClass.getName() + "#" + m.getName()))) {
                result.add(m);
            }
        }
        if (result.isEmpty()) {
            for (Method m : testClass.getDeclaredMethods()) {
                if (m.isAnnotationPresent(Test.class)) {
                    result.add(m);
                }
            }
        }
        return result;
    }

    private static Throwable unwrap(Throwable t) {
        Throwable current = t;
        while (current instanceof InvocationTargetException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
