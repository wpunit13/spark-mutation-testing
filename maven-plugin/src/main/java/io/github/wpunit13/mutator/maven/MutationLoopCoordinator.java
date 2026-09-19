package io.github.wpunit13.mutator.maven;

import io.github.wpunit13.mutator.MutantBootstrap;
import io.github.wpunit13.mutator.catalog.MutationCatalogIo;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.report.AppliedMarkerStore;
import io.github.wpunit13.mutator.report.DiffSnippetStore;
import io.github.wpunit13.mutator.report.OutcomeFileStore;
import io.github.wpunit13.mutator.report.ReportWriter;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Coordinates the full mutation testing lifecycle across the Maven fork boundary:
 * <ol>
 *   <li>Runs an unmutated baseline; the baseline fork discovers candidates and
 *       writes {@code catalog.json} into the shared output directory.</li>
 *   <li>Reads {@code catalog.json} back into orchestrator memory.</li>
 *   <li>Forks Surefire once per mutant (sequential today, parallelizable later),
 *       classifies each terminal outcome, and writes {@code outcomes/<id>.json}.</li>
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
        this.surefireExecutor = Objects.requireNonNull(surefireExecutor, "surefireExecutor must not be null");
        this.timeoutMultiplier = timeoutMultiplier > 0.0 ? timeoutMultiplier : 2.0;
        this.outputDirectory = outputDirectory != null
                ? outputDirectory.toPath()
                : Path.of("target", "spark-mutator-reports");
        this.minMutationScore = minMutationScore;
        this.targetModules = targetModules == null ? List.of() : List.copyOf(targetModules);
        this.excludedMutators = excludedMutators == null ? List.of() : List.copyOf(excludedMutators);
    }

    public MutationLoopCoordinator(SurefireExecutor surefireExecutor) {
        this(surefireExecutor, 2.0, new File("target", "spark-mutator-reports"), 0.0);
    }

    public MutationLoopResult execute() throws MojoFailureException, MojoExecutionException {
        ensureOutputDirectory();

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

        long timeoutMillis = baselineResult.getElapsedMillis() > 0
                ? (long) Math.ceil(baselineResult.getElapsedMillis() * timeoutMultiplier)
                : 1000L;

        // 2. Read the catalog back across the process boundary.
        List<MutantMetadata> catalog = new ArrayList<>(readCatalog());

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
                            true,
                            timeoutMillis));

            MutantResult outcome = classify(mutantId, mutantResult, timeoutMillis);
            writeOutcome(outcome);
            mergeDiffSnippet(catalog, mutantId);

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
                reportPath);
    }

    private void ensureOutputDirectory() throws MojoExecutionException {
        try {
            Files.createDirectories(outputDirectory);
        } catch (IOException e) {
            throw new MojoExecutionException("Could not create output directory " + outputDirectory, e);
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
                targetModules, excludedMutators, timeoutMultiplier, minMutationScore, true);
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
            return new MutantResult(mutantId, MutantStatus.ERRORED, elapsed, r.getFailureDetail(), now);
        }
        if (r.isFailure()) {
            String detail = r.getFailureDetail() != null ? r.getFailureDetail() : "Assertion failed in test execution";
            return new MutantResult(mutantId, MutantStatus.KILLED, elapsed, detail, now);
        }
        return new MutantResult(mutantId, MutantStatus.SURVIVED, elapsed, null, now);
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

        public MutationLoopResult(
                int totalMutants,
                int killed,
                int survived,
                int timedOut,
                int errored,
                int notApplied,
                double mutationScore,
                String reportPath) {
            this.totalMutants = totalMutants;
            this.killed = killed;
            this.survived = survived;
            this.timedOut = timedOut;
            this.errored = errored;
            this.notApplied = notApplied;
            this.mutationScore = mutationScore;
            this.reportPath = reportPath;
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