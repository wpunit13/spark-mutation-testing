package io.github.wpunit13.mutator.junit5;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.AppliedMutantTracker;
import io.github.wpunit13.mutator.MutantBootstrap;
import io.github.wpunit13.mutator.MutantRegistry;
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess;
import io.github.wpunit13.mutator.catalog.MutationCatalogIo;
import io.github.wpunit13.mutator.report.AppliedMarkerStore;
import io.github.wpunit13.mutator.report.ReportSink;
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
import org.junit.jupiter.api.AfterEach;
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
        String existing = System.getProperty("spark.sql.extensions");
        if (existing == null || existing.isBlank()) {
            System.setProperty("spark.sql.extensions", EXTENSION_CLASS);
        } else if (!existing.contains(EXTENSION_CLASS)) {
            System.setProperty("spark.sql.extensions", existing + "," + EXTENSION_CLASS);
        }
    }

    /**
     * Cleans Catalyst caches across active/default SparkSessions using SessionResetFacade.
     */
    public static void cleanCatalystCache() {
        try {
            Class<?> sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession");
            Object opt = sparkSessionClass.getMethod("getActiveSession").invoke(null);
            Method isDefined = opt.getClass().getMethod("isDefined");
            Object spark = null;
            if ((Boolean) isDefined.invoke(opt)) {
                spark = opt.getClass().getMethod("get").invoke(opt);
            } else {
                Object defaultOpt = sparkSessionClass.getMethod("getDefaultSession").invoke(null);
                if ((Boolean) isDefined.invoke(defaultOpt)) {
                    spark = defaultOpt.getClass().getMethod("get").invoke(defaultOpt);
                }
            }
            if (spark != null) {
                SessionResetFacade.resetSessionState(spark, 0L);
            }
        } catch (Throwable ignored) {
            // Spark may not be present or no active session; safely continue
        }
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
        getStore(context).put("baselineMode", isBaseline);
    }

    // 2. BeforeEach: if driving mutation run, set active mutant
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (isDisabled(context)) {
            return;
        }

        MutantBootstrap.activateFromSystemProperties();

        getStore(context).put("testStartTime", System.currentTimeMillis());
        getStore(context).remove("handledFailure");
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
            Long start = getStore(context).get("testStartTime", Long.class);
            long elapsed = start != null ? (System.currentTimeMillis() - start) : 0L;
            String failureDetail = root.getMessage() != null ? root.getMessage() : root.toString();
            getStore(context).put("handledFailure", true);

            if (root instanceof AssertionError) {
                try {
                    ReportSink.recordOutcome(activeMutant, "KILLED", elapsed, failureDetail);
                } catch (IllegalStateException ignored) {
                    // Already recorded
                }
                // Suppress AssertionError so test runner indicates mutant was successfully killed
                return;
            } else {
                try {
                    ReportSink.recordOutcome(activeMutant, "ERRORED", elapsed, failureDetail);
                } catch (IllegalStateException ignored) {
                    // Already recorded
                }
                throw throwable;
            }
        }

        // In baseline mode, record baseline failure and let exception surface normally
        getStore(context).put("baselineFailed", true);
        throw throwable;
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
                Boolean handled = getStore(context).get("handledFailure", Boolean.class);
                if (handled == null || !handled) {
                    if (MutantBootstrap.phaseOrNull() == null) {
                        Long start = getStore(context).get("testStartTime", Long.class);
                        long elapsed = start != null ? (System.currentTimeMillis() - start) : 0L;
                        try {
                            ReportSink.recordOutcome(activeMutant, "SURVIVED", elapsed, null);
                        } catch (IllegalStateException ignored) {
                            // Already recorded
                        }
                    }
                }
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

        if (Boolean.TRUE.equals(baselineMode) && !Boolean.TRUE.equals(baselineFailed)) {
            runInProcessMutations(context);
        }

        ReportSink.finalizeAndWriteReports();

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

    private void runInProcessMutations(ExtensionContext context) throws Exception {
        String catalogJson = MutationCatalogAccess.getFullCatalogJson();
        JsonNode catalogNode = MAPPER.readTree(catalogJson);

        if (catalogNode == null || !catalogNode.isArray() || catalogNode.isEmpty()) {
            return;
        }

        Class<?> testClass = context.getRequiredTestClass();

        for (JsonNode mutantEntry : catalogNode) {
            String mutantId = mutantEntry.get("mutantId").asText();
            JsonNode mappedTestsNode = mutantEntry.get("mappedTestIds");
            Set<String> mappedTestNames = new HashSet<>();
            if (mappedTestsNode != null && mappedTestsNode.isArray()) {
                for (JsonNode t : mappedTestsNode) {
                    mappedTestNames.add(t.asText());
                }
            }

            long start = System.currentTimeMillis();
            boolean killed = false;
            String failureDetail = null;
            boolean errored = false;

            try {
                // Honesty-guard baseline: forget the previous mutant's applied
                // fact so a mismatch below can only come from THIS mutant.
                AppliedMutantTracker.clear();
                MutantRegistry.getInstance().setActiveMutant(mutantId);
                cleanCatalystCache();

                executeTestsForMutant(testClass, mappedTestNames);
            } catch (Throwable t) {
                Throwable root = unwrap(t);
                if (root instanceof AssertionError) {
                    killed = true;
                    failureDetail = root.getMessage() != null ? root.getMessage() : root.toString();
                } else {
                    errored = true;
                    failureDetail = "Unhandled exception: " + root.toString();
                }
            } finally {
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

            long elapsed = System.currentTimeMillis() - start;
            // A mutant whose rewrite never executed proves nothing: report it
            // ERRORED instead of SURVIVED (a fake survivor corrupts the score).
            boolean mutationApplied = mutantId.equals(AppliedMutantTracker.lastOrNull());
            try {
                if (killed) {
                    ReportSink.recordOutcome(mutantId, "KILLED", elapsed, failureDetail);
                } else if (errored) {
                    ReportSink.recordOutcome(mutantId, "ERRORED", elapsed, failureDetail);
                } else if (!mutationApplied) {
                    ReportSink.recordOutcome(mutantId, "ERRORED", elapsed,
                            "mutation was not applied (coordinate matched no plan node)");
                } else {
                    ReportSink.recordOutcome(mutantId, "SURVIVED", elapsed, null);
                }
            } catch (IllegalStateException ignored) {
                // Write-once
            }
        }
    }

    private void executeTestsForMutant(Class<?> testClass, Set<String> mappedTestNames) throws Throwable {
        Constructor<?> ctor = testClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object testInstance = ctor.newInstance();

        List<Method> beforeMethods = findMethodsWithAnnotation(testClass, BeforeEach.class);
        List<Method> afterMethods = findMethodsWithAnnotation(testClass, AfterEach.class);
        List<Method> testMethods = findTestMethods(testClass, mappedTestNames);

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
                    }
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
            if (m.isAnnotationPresent(Test.class)) {
                if (mappedTestNames.isEmpty()
                        || mappedTestNames.contains(m.getName())
                        || mappedTestNames.contains(testClass.getName() + "#" + m.getName())) {
                    result.add(m);
                }
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
