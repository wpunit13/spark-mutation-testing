package io.github.wpunit13.mutator.report;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Public, instance-free facade for writing the full set of report artifacts
 * (JSON, SARIF, HTML) from an explicit catalog + outcome map.
 *
 * <p>This is the cross-process entry point for the report pipeline: the Maven
 * aggregator has already read {@code catalog.json} and {@code outcomes/*.json}
 * back into memory (via {@code MutationCatalogIo} / {@code OutcomeFileStore})
 * and calls {@link #writeReports(Path, Collection, Map)} directly, without
 * touching the in-memory {@code ReportSink} singleton. The in-process path
 * ({@link ReportSink#finalizeAndWriteReports()}) delegates here with the
 * singleton's own catalog/results, so both paths share one writer pipeline.
 */
public final class ReportWriter {

    private static final String NO_OUTCOME_DETAIL = "no outcome recorded; run terminated early";

    /** Fork-directive / user-config keys shared with §3.1 (one key, both surfaces). */
    private static final String PROP_TARGET_MODULES = "spark.mutator.targetModules";
    private static final String PROP_EXCLUDED_MUTATORS = "spark.mutator.excludedMutators";
    private static final String PROP_TIMEOUT_MULTIPLIER = "spark.mutator.timeoutMultiplier";
    private static final String PROP_MIN_MUTATION_SCORE = "spark.mutator.minMutationScore";

    /**
     * WP-25: set to {@code true} by every orchestration surface that enforces
     * a per-mutant deadline (Maven fork kill, PySpark watchdog, JUnit 5
     * in-process watchdog) so the report's config echo distinguishes enforced
     * deadlines from the pre-WP-25 echo-only era.
     */
    private static final String PROP_TIMEOUT_ENFORCED = "spark.mutator.timeoutEnforced";

    /** WP-24 governance-gate knobs (one key, both surfaces). */
    public static final String PROP_MAX_ERRORED_COUNT = "spark.mutator.maxErroredCount";
    public static final String PROP_MAX_NOT_APPLIED_RATIO = "spark.mutator.maxNotAppliedRatio";

    /** Mechanism-(c) CI sharding knobs (one key, both surfaces). */
    public static final String PROP_SHARDS = "spark.mutator.shards";
    public static final String PROP_SHARD = "spark.mutator.shard";

    /** Default for {@code spark.mutator.maxNotAppliedRatio}: 20% of the run. */
    public static final double DEFAULT_MAX_NOT_APPLIED_RATIO = 0.20;

    /** Default for {@code spark.mutator.maxErroredCount}: zero tolerance. */
    public static final int DEFAULT_MAX_ERRORED_COUNT = 0;

    private ReportWriter() {
    }

    /**
     * The run's configuration echo for the report's {@code config} block
     * (schema §5.3). The block's fields already exist in the frozen schema;
     * this value object populates them. Carried as a nested type so the
     * writer pipeline gains config plumbing without a new top-level contract
     * surface.
     */
    public static final class Config {

        private final List<String> targetModules;
        private final List<String> excludedMutators;
        private final double timeoutMultiplier;
        private final double minMutationScore;
        private final boolean timeoutEnforced;
        /** Mechanism-(c) CI sharding echo; 1/0 = unsharded. */
        private final int shards;
        private final int shard;

        public Config(
                List<String> targetModules,
                List<String> excludedMutators,
                double timeoutMultiplier,
                double minMutationScore,
                boolean timeoutEnforced) {
            this(targetModules, excludedMutators, timeoutMultiplier, minMutationScore,
                    timeoutEnforced, 1, 0);
        }

        public Config(
                List<String> targetModules,
                List<String> excludedMutators,
                double timeoutMultiplier,
                double minMutationScore,
                boolean timeoutEnforced,
                int shards,
                int shard) {
            this.targetModules = targetModules == null ? List.of() : List.copyOf(targetModules);
            this.excludedMutators = excludedMutators == null ? List.of() : List.copyOf(excludedMutators);
            this.timeoutMultiplier = timeoutMultiplier;
            this.minMutationScore = minMutationScore;
            this.timeoutEnforced = timeoutEnforced;
            this.shards = shards;
            this.shard = shard;
        }

        /**
         * Resolves the config echo from the {@code spark.mutator.*} system
         * properties — the one key across both surfaces (developer-guide
         * §3.2). Used by the in-process report paths (the PySpark driver sets
         * the properties from its TOML config; the JUnit 5 in-process gate
         * documents {@code spark.mutator.minMutationScore}); unset keys fall
         * back to the documented defaults (no filtering, multiplier 2.0,
         * gate off).
         */
        public static Config fromSystemProperties() {
            return new Config(
                    splitCsv(System.getProperty(PROP_TARGET_MODULES)),
                    splitCsv(System.getProperty(PROP_EXCLUDED_MUTATORS)),
                    parseDouble(System.getProperty(PROP_TIMEOUT_MULTIPLIER), 2.0),
                    parseDouble(System.getProperty(PROP_MIN_MUTATION_SCORE), 0.0),
                    Boolean.parseBoolean(System.getProperty(PROP_TIMEOUT_ENFORCED, "false")),
                    parseInt(System.getProperty(PROP_SHARDS), 1),
                    parseInt(System.getProperty(PROP_SHARD), 0));
        }

        private static List<String> splitCsv(String value) {
            if (value == null || value.isBlank()) {
                return List.of();
            }
            return java.util.Arrays.stream(value.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }

        private static double parseDouble(String value, double fallback) {
            if (value == null || value.isBlank()) {
                return fallback;
            }
            try {
                return Double.parseDouble(value.trim());
            } catch (NumberFormatException e) {
                return fallback;
            }
        }

        private static int parseInt(String value, int fallback) {
            if (value == null || value.isBlank()) {
                return fallback;
            }
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                return fallback;
            }
        }

        public List<String> getTargetModules() {
            return targetModules;
        }

        public List<String> getExcludedMutators() {
            return excludedMutators;
        }

        public double getTimeoutMultiplier() {
            return timeoutMultiplier;
        }

        public double getMinMutationScore() {
            return minMutationScore;
        }

        public boolean isTimeoutEnforced() {
            return timeoutEnforced;
        }

        public int getShards() {
            return shards;
        }

        public int getShard() {
            return shard;
        }
    }

    /**
     * Computes the mutation score from explicit catalog + outcome data.
     * Mirrors {@link JsonReportWriter#mutationScore(int, int, int)}: errored,
     * not-applied and skipped mutants (and any catalogued mutant with no
     * recorded outcome) are excluded from both numerator and denominator. A
     * zero denominator yields 0.0 — never NaN, never Infinity.
     */
    public static double computeScore(
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results) {
        int killed = 0;
        int timedOut = 0;
        int survived = 0;
        for (MutantMetadata meta : catalog) {
            MutantResult result = results.get(meta.getMutantId());
            if (result == null) {
                continue;
            }
            switch (result.getStatus()) {
                case KILLED -> killed++;
                case TIMED_OUT -> timedOut++;
                case SURVIVED -> survived++;
                default -> {
                    // ERRORED / NOT_APPLIED / SKIPPED are excluded from the score.
                }
            }
        }
        return JsonReportWriter.mutationScore(killed, timedOut, survived);
    }

    /**
     * WP-24 governance gate: evaluates the run's outcome counts against the
     * two population knobs and returns one human-readable message per
     * violation (empty list = pass). Both orchestration paths call this with
     * their own counts; the knob resolution (system properties vs Mojo
     * parameters) stays per-surface per the one-key-both-surfaces convention.
     *
     * <ul>
     *   <li><b>Real-failure ERRORED</b> (dead sessions, shim violations,
     *       mutation crashes) — zero tolerance by default:
     *       {@code errored > maxErroredCount} fails. A negative
       *       {@code maxErroredCount} disables the check.</li>
     *   <li><b>Designed not-applied</b> (nodes hidden inside caches, pruned
     *       stubs, shapes that never execute) — ratio-gated:
     *       {@code notApplied / (total - skipped) > maxNotAppliedRatio}
       *       fails. A negative {@code maxNotAppliedRatio} disables the
       *       check.</li>
     * </ul>
     *
     * <p>The score formula is deliberately untouched: NOT_APPLIED and ERRORED
     * are excluded from both terms, exactly as before the split.
     */
    public static List<String> evaluateGateViolations(
            int total,
            int errored,
            int notApplied,
            int skipped,
            int maxErroredCount,
            double maxNotAppliedRatio) {
        List<String> violations = new ArrayList<>();
        if (maxErroredCount >= 0 && errored > maxErroredCount) {
            violations.add("real-failure ERRORED count " + errored
                    + " exceeds maxErroredCount " + maxErroredCount
                    + " (dead sessions, shim violations, or mutation crashes)");
        }
        if (maxNotAppliedRatio >= 0) {
            int denominator = total - skipped;
            double ratio = denominator <= 0 ? 0.0 : (notApplied / (double) denominator);
            if (ratio > maxNotAppliedRatio) {
                violations.add("not-applied ratio " + ratio + " (" + notApplied + "/"
                        + denominator + ") exceeds maxNotAppliedRatio " + maxNotAppliedRatio);
            }
        }
        return violations;
    }

    /**
     * Writes {@code mutation-report.json}, {@code mutation-report.sarif}, and
     * {@code mutation-report.html} into {@code outputDir}. Any catalogued
     * mutant missing from {@code results} is synthesized as {@code ERRORED} so
     * the emitted schema never carries a null result object.
     *
     * @return the absolute path of the primary mutation-report.json
     * @throws IllegalStateException if writing any artifact fails
     */
    public static String writeReports(
            Path outputDir,
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results) {
        return writeReports(outputDir, catalog, results, Config.fromSystemProperties());
    }

    /**
     * Same as {@link #writeReports(Path, Collection, Map)} with an explicit
     * config echo. The Maven coordinator passes the values it orchestrated
     * the run with; the in-process paths resolve them from system properties.
     */
    public static String writeReports(
            Path outputDir,
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results,
            Config config) {
        Map<String, MutantResult> working = new LinkedHashMap<>(results);
        for (MutantMetadata meta : catalog) {
            working.putIfAbsent(
                    meta.getMutantId(),
                    new MutantResult(
                            meta.getMutantId(),
                            MutantStatus.ERRORED,
                            0L,
                            NO_OUTCOME_DETAIL,
                            System.currentTimeMillis()));
        }
        Map<String, MutantResult> snapshot = Map.copyOf(working);
        try {
            Path json = JsonReportWriter.write(outputDir, catalog, snapshot, config);
            SarifReportWriter.write(outputDir, catalog, snapshot);
            HtmlReportWriter.write(outputDir, catalog, snapshot);
            TestValueReportWriter.write(outputDir, catalog, snapshot);
            return json.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write reports to " + outputDir, e);
        }
    }
}