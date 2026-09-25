package io.github.wpunit13.mutator.maven;

import io.github.wpunit13.mutator.MutantBootstrap;
import io.github.wpunit13.mutator.catalog.MutationCatalogIo;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import io.github.wpunit13.mutator.model.StructuralInvalidation;
import io.github.wpunit13.mutator.report.AppliedMarkerStore;
import io.github.wpunit13.mutator.report.DiffSnippetStore;
import io.github.wpunit13.mutator.report.FailingTests;
import io.github.wpunit13.mutator.report.OutcomeFileStore;
import io.github.wpunit13.mutator.report.ReportWriter;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Coordinates the full mutation testing lifecycle across the Maven fork boundary:
 * <ol>
 *   <li>Runs an unmutated baseline; the baseline fork discovers candidates and
 *       writes {@code catalog.json} into the shared output directory.</li>
 *   <li>Reads {@code catalog.json} back into orchestrator memory.</li>
 *   <li>Forks Surefire once per mutant (sequential within a shard; CI
 *       sharding via {@code spark.mutator.shards} splits the catalog across
 *       jobs), classifies each terminal outcome, and writes
 *       {@code outcomes/<id>.json}.</li>
 *   <li>Merges all outcome files and emits JSON/SARIF/HTML via the file-based
 *       {@link ReportWriter} (never the in-memory {@code ReportSink}, which is a
 *       per-JVM singleton that cannot see the fork's work).</li>
 *   <li>Applies the {@code minMutationScore} gate to the caller.</li>
 * </ol>
 *
 * <p>State crosses the process boundary exclusively through files under the
 * output directory: {@code catalog.json} (discovery: fork &rarr; orchestrator),
 * {@code applied/<id>.json} (applied-mutation markers: fork &rarr; orchestrator),
 * {@code diffs/<id>.json} (plan-diff snippets: fork &rarr; orchestrator, WP-19)
 * and {@code outcomes/<id>.json} (results: orchestrator writes, then merges).
 * The active-mutant directive crosses in the other direction via system
 * properties set on each fork launch.
 */
public class MutationLoopCoordinator {

    /** Fork-directive keys for the WP-19 config filters (developer-guide §3.1). */
    public static final String PROP_TARGET_MODULES = "spark.mutator.targetModules";
    public static final String PROP_EXCLUDED_MUTATORS = "spark.mutator.excludedMutators";

    private final SurefireExecutor surefireExecutor;
    private final double timeoutMultiplier;
    private final Path outputDirectory;
    private final double minMutationScore;
    private final List<String> targetModules;
    private final List<String> excludedMutators;
    /** When true, mutant forks run without fail-fast so the surefire XML
      * records ALL failing tests (complete kill matrix for the test-value
      * report); when false (default), the fork aborts after the first
      * failure and only the first killer is attributed. */
    private final boolean perTestAttribution;
    /** Per-mutant timing sink (Milestone-0 instrumentation); no-op by default. */
    private final Consumer<String> timingLog;
    /** Mechanism-(c) CI sharding: this worker runs catalog indices i where
      * i % shards == shard. Default 1/0 = whole catalog, byte-identical loop. */
    private final int shards;
    private final int shard;
    /** Single-command orchestrator: children skip the baseline (the parent
      * already ran it) and use the parent's deadline — recomputing it under
      * contention could flip borderline TIMED_OUT verdicts vs N=1. */
    private final boolean skipBaseline;
    private final long deadlineOverrideMillis;

    public MutationLoopCoordinator(
            SurefireExecutor surefireExecutor,
            double timeoutMultiplier,
            File outputDirectory,
            double minMutationScore) {
        this(surefireExecutor, timeoutMultiplier, outputDirectory, minMutationScore,
                List.of(), List.of());
    }

    public MutationLoopCoordinator(
            SurefireExecutor surefireExecutor,
            double timeoutMultiplier,
            File outputDirectory,
            double minMutationScore,
            List<String> targetModules,
            List<String> excludedMutators) {
        this(surefireExecutor, timeoutMultiplier, outputDirectory, minMutationScore,
                targetModules, excludedMutators, false);
    }

    public MutationLoopCoordinator(
            SurefireExecutor surefireExecutor,
            double timeoutMultiplier,
            File outputDirectory,
            double minMutationScore,
            List<String> targetModules,
            List<String> excludedMutators,
            boolean perTestAttribution) {
        this(surefireExecutor, timeoutMultiplier, outputDirectory, minMutationScore,
                targetModules, excludedMutators, perTestAttribution, l -> {}, 1, 0,
                false, 0L);
    }

    public MutationLoopCoordinator(
            SurefireExecutor surefireExecutor,
            double timeoutMultiplier,
            File outputDirectory,
            double minMutationScore,
            List<String> targetModules,
            List<String> excludedMutators,
            boolean perTestAttribution,
            Consumer<String> timingLog,
            int shards,
            int shard) {
        this(surefireExecutor, timeoutMultiplier, outputDirectory, minMutationScore,
                targetModules, excludedMutators, perTestAttribution, timingLog, shards, shard,
                false, 0L);
    }

    public MutationLoopCoordinator(
            SurefireExecutor surefireExecutor,
            double timeoutMultiplier,
            File outputDirectory,
            double minMutationScore,
            List<String> targetModules,
            List<String> excludedMutators,
            boolean perTestAttribution,
            Consumer<String> timingLog,
            int shards,
            int shard,
            boolean skipBaseline,
            long deadlineOverrideMillis) {
        this.surefireExecutor = Objects.requireNonNull(surefireExecutor, "surefireExecutor must not be null");
        this.timeoutMultiplier = timeoutMultiplier > 0.0 ? timeoutMultiplier : 2.0;
        this.outputDirectory = outputDirectory != null
                ? outputDirectory.toPath()
                : Path.of("target", "spark-mutator-reports");
        this.minMutationScore = minMutationScore;
        this.targetModules = targetModules == null ? List.of() : List.copyOf(targetModules);
        this.excludedMutators = excludedMutators == null ? List.of() : List.copyOf(excludedMutators);
        this.perTestAttribution = perTestAttribution;
        this.timingLog = timingLog == null ? l -> {} : timingLog;
        if (shards < 1) {
            throw new IllegalArgumentException("shards must be >= 1 (got " + shards + ")");
        }
        if (shard < 0 || shard >= shards) {
            throw new IllegalArgumentException("shard must be in [0," + shards + ") (got " + shard + ")");
        }
        this.shards = shards;
        this.shard = shard;
        if (skipBaseline && deadlineOverrideMillis <= 0) {
            throw new IllegalArgumentException(
                    "skipBaseline requires deadlineOverrideMillis > 0 (got " + deadlineOverrideMillis + ")");
        }
        this.skipBaseline = skipBaseline;
        this.deadlineOverrideMillis = deadlineOverrideMillis;
    }

    public MutationLoopCoordinator(SurefireExecutor surefireExecutor) {
        this(surefireExecutor, 2.0, new File("target", "spark-mutator-reports"), 0.0);
    }

    public MutationLoopResult execute() throws MojoFailureException, MojoExecutionException {
        long timeoutMillis = skipBaseline ? deadlineOverrideMillis : runBaseline();
        return runLoop(timeoutMillis);
    }

    /**
     * Runs the unmutated baseline fork (discovery &rarr; {@code catalog.json})
     * and returns the per-mutant deadline. Public so the single-command
     * orchestrator can run the baseline once and pass the deadline to its
     * shard children (determinism: children must not recompute it under
     * contention).
     */
    public long runBaseline() throws MojoFailureException, MojoExecutionException {
        ensureOutputDirectory();
        clearStaleAppliedMarkers();

        // 1. Baseline: run unmutated. The baseline fork performs discovery and
        //    writes catalog.json into the shared output directory.
        SurefireExecutor.SurefireResult baselineResult = surefireExecutor.execute(new SurefireExecutor.SurefireRequest(
                baselineProperties(),
                null,
                false,
                0L));

        if (!baselineResult.isSuccess()) {
            String detail = baselineResult.getFailureDetail() != null
                    ? " Reason: " + baselineResult.getFailureDetail()
                    : "";
            throw new MojoFailureException(
                    "Baseline test suite failed. Mutation testing aborted." + detail);
        }
        timingLog.accept("baseline forkStartupMs=" + forkStartupMillis(baselineResult)
                + " execMs=" + baselineResult.getElapsedMillis());

        return baselineResult.getElapsedMillis() > 0
                ? (long) Math.ceil(baselineResult.getElapsedMillis() * timeoutMultiplier)
                : 1000L;
    }

    /**
     * Post-join merge for CI sharding: reads the full catalog + every shard's
     * outcome files, re-attaches diff snippets, and writes the combined
     * reports. No baseline, no forks. Gates run afterwards in the Mojo on the
     * merged counts (evaluated once, post-join).
     */
    public MutationLoopResult mergeFromDisk() throws MojoExecutionException {
        ensureOutputDirectory();

        List<MutantMetadata> catalog;
        try {
            catalog = new ArrayList<>(readCatalog());
        } catch (MojoExecutionException e) {
            throw new MojoExecutionException(
                    "Nothing to merge: no catalog in " + outputDirectory
                            + " (run the mutation goal first). " + e.getMessage(), e);
        }
        Map<String, MutantResult> merged = readOutcomes();

        // Fail-loudly: a shard that died or was truncated leaves catalogued
        // mutants without outcomes — never merge into a silent partial report.
        List<String> missing = catalog.stream()
                .map(MutantMetadata::getMutantId)
                .filter(id -> !merged.containsKey(id))
                .toList();
        if (!missing.isEmpty()) {
            throw new MojoExecutionException(
                    missing.size() + " of " + catalog.size()
                            + " catalogued mutants have no outcome file — a shard run failed"
                            + " or was truncated. Rerun the failed shard(s), then merge."
                            + " First missing: " + missing.get(0));
        }

        for (String mutantId : catalog.stream().map(MutantMetadata::getMutantId).toList()) {
            mergeDiffSnippet(catalog, mutantId);
        }

        int killed = 0;
        int survived = 0;
        int timedOut = 0;
        int errored = 0;
        int notApplied = 0;
        for (MutantMetadata meta : catalog) {
            switch (merged.get(meta.getMutantId()).getStatus()) {
                case KILLED -> killed++;
                case SURVIVED -> survived++;
                case TIMED_OUT -> timedOut++;
                case ERRORED -> errored++;
                case NOT_APPLIED -> notApplied++;
                case SKIPPED -> { /* not produced by the fork path */ }
            }
        }

        double score = mutationScore(killed, timedOut, survived);
        String reportPath = ReportWriter.writeReports(
                outputDirectory, catalog, merged, reportConfig());
        return new MutationLoopResult(
                catalog.size(), killed, survived, timedOut, errored, notApplied, score, reportPath,
                ReportWriter.familyStats(catalog, merged));
    }

    private MutationLoopResult runLoop(long timeoutMillis) throws MojoFailureException, MojoExecutionException {
        // 2. Read the catalog back across the process boundary.
        List<MutantMetadata> catalog = new ArrayList<>(readCatalog());

        // Mechanism-(c) CI sharding: keep this worker's disjoint index slice.
        // Discovery (the baseline fork) is never sharded — every shard runs the
        // full baseline and filters the catalog by index afterwards.
        if (shards > 1) {
            List<MutantMetadata> slice = new ArrayList<>();
            for (int i = 0; i < catalog.size(); i++) {
                if (i % shards == shard) {
                    slice.add(catalog.get(i));
                }
            }
            catalog = slice;
        }

        if (catalog.isEmpty()) {
            String reportPath = ReportWriter.writeReports(
                    outputDirectory, catalog, Map.of(), reportConfig());
            return new MutationLoopResult(0, 0, 0, 0, 0, 0, 0.0, reportPath);
        }

        // 3. Mutation loop.
        int killed = 0;
        int survived = 0;
        int timedOut = 0;
        int errored = 0;
        int notApplied = 0;
        for (MutantMetadata mutant : catalog) {
            String mutantId = mutant.getMutantId();
            String testFilter = mutant.getMappedTestIds().isEmpty()
                    ? null
                    : String.join(",", mutant.getMappedTestIds());

            SurefireExecutor.SurefireResult mutantResult = surefireExecutor.execute(
                    new SurefireExecutor.SurefireRequest(
                            mutantProperties(mutantId),
                            testFilter,
                            // fail-fast off under perTestAttribution: the fork
                            // runs the whole suite so the surefire XML records
                            // every failing test (sole-killer attribution).
                            !perTestAttribution,
                            timeoutMillis,
                            // Concurrent workers isolate surefire's booter-jar
                            // temp dir, XML report dir, and Spark/Derby state.
                            shards > 1 ? "-shard-" + shard : null));

            MutantResult outcome = withFailingTests(
                    classify(mutantId, mutantResult, timeoutMillis),
                    mutantResult.getFailedTestIds());
            writeOutcome(outcome);
            mergeDiffSnippet(catalog, mutantId);
            attributeExecutedTests(catalog, mutantId, mutantResult.getExecutedTestIds());
            timingLog.accept("mutant " + mutantId
                    + " verdict=" + outcome.getStatus()
                    + " forkStartupMs=" + forkStartupMillis(mutantResult)
                    + " execMs=" + mutantResult.getElapsedMillis()
                    + " deadlineMs=" + timeoutMillis);

            switch (outcome.getStatus()) {
                case KILLED -> killed++;
                case SURVIVED -> survived++;
                case TIMED_OUT -> timedOut++;
                case ERRORED -> errored++;
                case NOT_APPLIED -> notApplied++;
                case SKIPPED -> { /* not produced here */ }
            }
        }

        // 4. Merge all outcome files (fan-in) and write the final reports.
        Map<String, MutantResult> merged = readOutcomes();
        double score = mutationScore(killed, timedOut, survived);
        String reportPath = ReportWriter.writeReports(
                outputDirectory, catalog, merged, reportConfig());

        return new MutationLoopResult(
                catalog.size(),
                killed,
                survived,
                timedOut,
                errored,
                notApplied,
                score,
                reportPath,
                ReportWriter.familyStats(catalog, merged));
    }

    private void ensureOutputDirectory() throws MojoExecutionException {
        try {
            Files.createDirectories(outputDirectory);
        } catch (IOException e) {
            throw new MojoExecutionException("Could not create output directory " + outputDirectory, e);
        }
    }

    /**
     * Deletes {@code applied/} markers left by any previous run sharing this
     * output directory (an interrupted run, a differently-scoped run, a prior
     * in-process {@code mvn test}). The marker is only meaningful within one
     * run: a stale marker makes {@link #classify} skip the NOT_APPLIED branch
     * and record a fork that never executed the mutation as KILLED — a
     * fabricated kill that inflates the mutation score. Runs here, in the one
     * place that precedes every fork in every mode (single loop, orchestrated
     * workers, CI matrix), so concurrent shard children never delete each
     * other's markers. {@code skipBaseline=true} reruns deliberately do not
     * clean: they share the directory with sibling workers.
     */
    private void clearStaleAppliedMarkers() throws MojoExecutionException {
        Path appliedDir = AppliedMarkerStore.appliedDir(outputDirectory);
        if (!Files.exists(appliedDir)) {
            return;
        }
        long removed;
        try (Stream<Path> walk = Files.walk(appliedDir)) {
            removed = walk.filter(Files::isRegularFile).count();
        } catch (IOException e) {
            throw new MojoExecutionException("Could not scan " + appliedDir, e);
        }
        try (Stream<Path> walk = Files.walk(appliedDir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException | UncheckedIOException e) {
            throw new MojoExecutionException(
                    "Could not clear stale applied markers in " + appliedDir
                            + " (stale markers would misclassify not-applied forks as KILLED)", e);
        }
        if (removed > 0) {
            timingLog.accept("cleared " + removed + " stale applied marker(s) from " + appliedDir);
        }
    }

    private List<MutantMetadata> readCatalog() throws MojoExecutionException {
        try {
            return MutationCatalogIo.readCatalogJson(outputDirectory);
        } catch (IOException e) {
            throw new MojoExecutionException("Failed to read catalog from " + outputDirectory + ": " + e.getMessage(), e);
        }
    }

    private void writeOutcome(MutantResult outcome) throws MojoExecutionException {
        try {
            OutcomeFileStore.writeOutcome(outputDirectory, outcome);
        } catch (IOException e) {
            throw new MojoExecutionException("Failed to write outcome for " + outcome.getMutantId(), e);
        }
    }

    private Map<String, MutantResult> readOutcomes() throws MojoExecutionException {
        try {
            return OutcomeFileStore.readOutcomes(outputDirectory);
        } catch (IOException e) {
            throw new MojoExecutionException("Failed to read outcomes from " + outputDirectory + ": " + e.getMessage(), e);
        }
    }

    private Map<String, String> baselineProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(MutantBootstrap.PROP_PHASE, MutantBootstrap.PHASE_BASELINE);
        props.put(MutantBootstrap.PROP_OUTPUT_DIRECTORY, outputDirectory.toAbsolutePath().toString());
        putFilterDirectives(props);
        return props;
    }

    private Map<String, String> mutantProperties(String mutantId) {
        Map<String, String> props = new HashMap<>();
        props.put(MutantBootstrap.PROP_PHASE, MutantBootstrap.PHASE_MUTANT);
        props.put(MutantBootstrap.PROP_ACTIVE_MUTANT, mutantId);
        props.put(MutantBootstrap.PROP_OUTPUT_DIRECTORY, outputDirectory.toAbsolutePath().toString());
        putFilterDirectives(props);
        return props;
    }

    /**
     * WP-19 fork directives (developer-guide §3.1): both filter properties
     * ride the same channel as {@code spark.mutator.phase} and reach every
     * fork. Blank/unset means "no filtering".
     */
    private void putFilterDirectives(Map<String, String> props) {
        if (!targetModules.isEmpty()) {
            props.put(PROP_TARGET_MODULES, String.join(",", targetModules));
        }
        if (!excludedMutators.isEmpty()) {
            props.put(PROP_EXCLUDED_MUTATORS, String.join(",", excludedMutators));
        }
    }

    /** The config echo this run was orchestrated with (report §5.3 config block). */
    private ReportWriter.Config reportConfig() {
        // The fork path always enforces the per-mutant deadline (fork kill on
        // expiry), so its config echo records timeoutEnforced = true.
        return new ReportWriter.Config(
                targetModules, excludedMutators, timeoutMultiplier, minMutationScore, true,
                shards, shard);
    }

    /**
     * Merges the mutant fork's plan-diff sidecar (WP-19) into the in-memory
     * catalog copy before the reports are written. A missing sidecar legally
     * leaves the snippet empty; a sidecar naming an unknown mutant is a
     * contract violation and fails loudly.
     */
    private void mergeDiffSnippet(List<MutantMetadata> catalog, String mutantId)
            throws MojoExecutionException {
        String snippet;
        try {
            snippet = DiffSnippetStore.readSnippetOrNull(outputDirectory, mutantId);
        } catch (IOException e) {
            throw new MojoExecutionException(
                    "Failed to read diff snippet sidecar for " + mutantId + ": " + e.getMessage(), e);
        }
        if (snippet == null || snippet.isEmpty()) {
            return;
        }
        for (int i = 0; i < catalog.size(); i++) {
            MutantMetadata meta = catalog.get(i);
            if (meta.getMutantId().equals(mutantId)) {
                catalog.set(i, new MutantMetadata(
                        meta.getMutantId(),
                        meta.getFilePath(),
                        meta.getLineNumber(),
                        meta.getOperatorType(),
                        meta.getMutationIndex(),
                        meta.getDescription(),
                        meta.getCoordinateHex(),
                        snippet,
                        meta.getMappedTestIds()));
                return;
            }
        }
        throw new MojoExecutionException(
                "Contract violation: diff snippet sidecar references unknown mutantId '"
                        + mutantId + "'.");
    }

    /**
     * Names the tests whose failure killed the mutant, appended to the KILLED
     * outcome's detail so the report answers "which test caught it" without
     * digging through per-fork surefire output (which each subsequent fork
     * overwrites). Other statuses keep their detail untouched.
     */
    private MutantResult withFailingTests(MutantResult outcome, List<String> failedTestIds) {
        if (outcome.getStatus() != MutantStatus.KILLED || failedTestIds == null || failedTestIds.isEmpty()) {
            return outcome;
        }
        // Format owned by report.FailingTests so every path emits (and any
        // consumer parses) one identical attribution block.
        String detail = FailingTests.append(outcome.getFailureDetailOrNull(), failedTestIds);
        return new MutantResult(
                outcome.getMutantId(),
                outcome.getStatus(),
                outcome.getElapsedMillis(),
                detail,
                outcome.getRecordedAtEpochMillis());
    }

    /**
     * Attributes the fork's executed tests to the mutant's catalog entry.
     * The fork path runs the whole suite per mutant (no test impact analysis
     * yet), so the conservative mapping is "every test that ran" — which is
     * exactly what the report's mappedTests cell should list. No-op when the
     * fork produced no parseable test results.
     */
    private void attributeExecutedTests(List<MutantMetadata> catalog, String mutantId, List<String> executedTestIds) {
        if (executedTestIds == null || executedTestIds.isEmpty()) {
            return;
        }
        for (int i = 0; i < catalog.size(); i++) {
            MutantMetadata meta = catalog.get(i);
            if (!meta.getMutantId().equals(mutantId) || meta.getMappedTestIds().equals(executedTestIds)) {
                continue;
            }
            catalog.set(i, new MutantMetadata(
                    meta.getMutantId(),
                    meta.getFilePath(),
                    meta.getLineNumber(),
                    meta.getOperatorType(),
                    meta.getMutationIndex(),
                    meta.getDescription(),
                    meta.getCoordinateHex(),
                    meta.getAstDiffSnippet(),
                    executedTestIds));
            return;
        }
    }

    /**
     * Classifies one mutant fork's terminal outcome, in this exact order:
     * <ol>
     *   <li>timeout &rarr; TIMED_OUT (the marker is irrelevant: a fork that
     *       hung may have applied the mutation without finishing);</li>
     * <li>applied marker missing &rarr; NOT_APPLIED with the not-applied detail,
     *       regardless of exit code — a mutation that never executed must
     *       never be classified KILLED or SURVIVED (a fake survivor corrupts
     *       the mutation score). WP-24 splits this out of ERRORED so the gate
     *       can zero-tolerance real failures without punishing shape-dependent
     *       not-applied mutants;</li>
     *   <li>otherwise classify from the exit code as before (KILLED /
     *       SURVIVED / ERRORED).</li>
     * </ol>
     */
    private MutantResult classify(String mutantId, SurefireExecutor.SurefireResult r, long timeoutMillis) {
        long elapsed = r.getElapsedMillis();
        long now = System.currentTimeMillis();
        if (r.isTimeout() || elapsed > timeoutMillis) {
            String detail = r.getFailureDetail() != null ? r.getFailureDetail() : "timed out (> " + timeoutMillis + "ms)";
            return new MutantResult(mutantId, MutantStatus.TIMED_OUT, elapsed, detail, now);
        }
        if (!AppliedMarkerStore.exists(outputDirectory, mutantId)) {
            // WP-24: designed not-applied — the mutation never executed.
            // Distinct from ERRORED (harness/engine failure).
            return new MutantResult(mutantId, MutantStatus.NOT_APPLIED, elapsed,
                    "mutation was not applied (coordinate matched no plan node)", now);
        }
        if (r.getExitCode() == 2) {
            // SurefireExecutor maps a non-MojoFailure exception to exit code 2:
            // the plugin could not run the suite, which is an ERRORED, not a KILLED.
            // Exception: the mutation WAS applied and the failure is a plan
            // binding/resolution error — the mutated node stopped producing a
            // column the rest of the plan references (e.g. INNER→ANTI with
            // downstream right-side references). The optimizer had its chance
            // to repair and could not: structurally invalid for this query,
            // a designed skip, not an engine/harness failure.
            String detail = r.getFailureDetail();
            if (detail != null && StructuralInvalidation.matches(detail)) {
                return new MutantResult(mutantId, MutantStatus.SKIPPED, elapsed,
                        "mutation structurally invalidated the plan: " + detail, now);
            }
            return new MutantResult(mutantId, MutantStatus.ERRORED, elapsed, detail, now);
        }
        if (r.isFailure()) {
            String detail = r.getFailureDetail() != null ? r.getFailureDetail() : "Assertion failed in test execution";
            return new MutantResult(mutantId, MutantStatus.KILLED, elapsed, detail, now);
        }
        return new MutantResult(mutantId, MutantStatus.SURVIVED, elapsed, null, now);
    }

    /** Fixed per-fork overhead (§1.3 S_fork): the plugin path has no
      * launch/wait seam — executeMojo spans JVM spawn through teardown — so
      * fork startup is derived as fork wall time minus the surefire suite
      * time (0 when the XML is unparseable, e.g. a timed-out fork). */
    private static long forkStartupMillis(SurefireExecutor.SurefireResult r) {
        return Math.max(0L, r.getElapsedMillis() - r.getTestTimeMillis());
    }

    private static double mutationScore(int killed, int timedOut, int survived) {
        int denominator = killed + timedOut + survived;
        if (denominator == 0) {
            return 0.0;
        }
        double raw = ((killed + timedOut) / (double) denominator) * 100.0;
        return Math.round(raw * 100.0) / 100.0;
    }

    public SurefireExecutor getSurefireExecutor() {
        return surefireExecutor;
    }

    public double getTimeoutMultiplier() {
        return timeoutMultiplier;
    }

    public File getOutputDirectory() {
        return outputDirectory.toFile();
    }

    public double getMinMutationScore() {
        return minMutationScore;
    }

    public List<String> getTargetModules() {
        return targetModules;
    }

    public List<String> getExcludedMutators() {
        return excludedMutators;
    }

    public int getShards() {
        return shards;
    }

    public int getShard() {
        return shard;
    }

    /**
     * Value object capturing the summary of the mutation execution loop.
     */
    public static class MutationLoopResult {
        private final int totalMutants;
        private final int killed;
        private final int survived;
        private final int timedOut;
        private final int errored;
        private final int notApplied;
        private final double mutationScore;
        private final String reportPath;
        private final Map<OperatorTypeDto, ReportWriter.FamilyStats> notAppliedByFamily;

        public MutationLoopResult(
                int totalMutants,
                int killed,
                int survived,
                int timedOut,
                int errored,
                int notApplied,
                double mutationScore,
                String reportPath) {
            this(totalMutants, killed, survived, timedOut, errored, notApplied, mutationScore,
                    reportPath, Map.of());
        }

        public MutationLoopResult(
                int totalMutants,
                int killed,
                int survived,
                int timedOut,
                int errored,
                int notApplied,
                double mutationScore,
                String reportPath,
                Map<OperatorTypeDto, ReportWriter.FamilyStats> notAppliedByFamily) {
            this.totalMutants = totalMutants;
            this.killed = killed;
            this.survived = survived;
            this.timedOut = timedOut;
            this.errored = errored;
            this.notApplied = notApplied;
            this.mutationScore = mutationScore;
            this.reportPath = reportPath;
            this.notAppliedByFamily = notAppliedByFamily == null
                    ? Map.of()
                    : Map.copyOf(notAppliedByFamily);
        }

        public int getTotalMutants() {
            return totalMutants;
        }

        public int getKilled() {
            return killed;
        }

        public int getSurvived() {
            return survived;
        }

        public int getTimedOut() {
            return timedOut;
        }

        public int getErrored() {
            return errored;
        }

        public int getNotApplied() {
            return notApplied;
        }

        public double getMutationScore() {
            return mutationScore;
        }

        public String getReportPath() {
            return reportPath;
        }

        /**
         * Per-family population counts for the WP-24 not-applied gate. Empty
         * when the run produced no catalogued mutants. Consumed by
         * {@code MutateMojo#enforceQualityGate} so the ratio can be scoped to
         * an accountable set of families
         * ({@code spark.mutator.notAppliedExemptMutators}).
         */
        public Map<OperatorTypeDto, ReportWriter.FamilyStats> getNotAppliedByFamily() {
            return notAppliedByFamily;
        }

        @Override
        public String toString() {
            return "MutationLoopResult{" +
                    "totalMutants=" + totalMutants +
                    ", killed=" + killed +
                    ", survived=" + survived +
                    ", timedOut=" + timedOut +
                    ", errored=" + errored +
                    ", notApplied=" + notApplied +
                    ", mutationScore=" + mutationScore +
                    ", reportPath='" + reportPath + '\'' +
                    '}';
        }
    }
}