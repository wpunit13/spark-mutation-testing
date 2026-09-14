package io.github.wpunit13.mutator.report;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import java.io.IOException;
import java.nio.file.Path;
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

        public Config(
                List<String> targetModules,
                List<String> excludedMutators,
                double timeoutMultiplier,
                double minMutationScore) {
            this.targetModules = targetModules == null ? List.of() : List.copyOf(targetModules);
            this.excludedMutators = excludedMutators == null ? List.of() : List.copyOf(excludedMutators);
            this.timeoutMultiplier = timeoutMultiplier;
            this.minMutationScore = minMutationScore;
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
                    parseDouble(System.getProperty(PROP_MIN_MUTATION_SCORE), 0.0));
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
    }

    /**
     * Computes the mutation score from explicit catalog + outcome data.
     * Mirrors {@link JsonReportWriter#mutationScore(int, int, int)}: errored
     * and skipped mutants (and any catalogued mutant with no recorded outcome)
     * are excluded from both numerator and denominator. A zero denominator
     * yields 0.0 — never NaN, never Infinity.
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
                    // ERRORED / SKIPPED are excluded from the score.
                }
            }
        }
        return JsonReportWriter.mutationScore(killed, timedOut, survived);
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
            return json.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write reports to " + outputDir, e);
        }
    }
}