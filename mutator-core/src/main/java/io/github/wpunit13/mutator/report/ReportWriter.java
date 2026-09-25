package io.github.wpunit13.mutator.report;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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

    /**
     * WP-24 not-applied gate scope (one key, both surfaces): comma-separated
     * {@link OperatorTypeDto} names whose not-applied mutants are exempt from the
     * ratio check. Empty (the default) keeps the gate global — every family
     * counts. See
     * {@link #evaluateGateViolations(int, int, int, int, int, double, Map, Set)}.
     */
    public static final String PROP_NOT_APPLIED_EXEMPT_MUTATORS =
            "spark.mutator.notAppliedExemptMutators";

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
     * <p>This overload is the global form: every operator family counts. Use
     * {@link #evaluateGateViolations(int, int, int, int, int, double, Map, Set)}
     * to scope the not-applied ratio to an accountable set of families.
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
        return evaluateGateViolations(total, errored, notApplied, skipped,
                maxErroredCount, maxNotAppliedRatio, Map.of(), Set.of());
    }

    /**
     * Per-family variant of
     * {@link #evaluateGateViolations(int, int, int, int, int, double)}. The
     * ERRORED check is identical; only the not-applied ratio becomes
     * scope-aware.
     *
     * <p>When {@code notAppliedExemptFamilies} is non-empty, the not-applied
     * ratio is evaluated over the <b>accountable population</b> only: the
     * families present in {@code familyStats} that are NOT exempt. Both the
     * numerator (not-applied) and the denominator (total − skipped) are
     * restricted to those families, so an exempt family cannot dilute the
     * ratio. An empty exempt set (or empty {@code familyStats}) reproduces the
     * global check exactly.
     *
     * <p>Why this exists: some operator families drift under Catalyst
     * rewriting far more than others. {@code PROJECT} nodes are the extreme —
     * the optimizer collapses aliases and prunes columns, so the identity
     * fallback refuses ambiguous matches and the run reports a load-sensitive
     * designed not-applied tail (measured 0.14 locally vs 0.28 on CI for one
     * plan). A global ratio gate then flakes on {@code PROJECT} churn, while
     * what the gate should catch is a real regression (e.g. the shim stops
     * applying to {@code FILTER}/{@code JOIN}). Exempting the noisy family
     * keeps the gate sharp on the rest.
     *
     * <p>Trade-off, stated plainly: an exempt family's not-applied mutants are
     * not gated at all — a breakage confined to an exempt family passes. Keep
     * the exempt list narrow, and read the report's {@code notApplied} count to
     * watch the exempt families by hand.
     */
    public static List<String> evaluateGateViolations(
            int total,
            int errored,
            int notApplied,
            int skipped,
            int maxErroredCount,
            double maxNotAppliedRatio,
            Map<OperatorTypeDto, FamilyStats> familyStats,
            Set<OperatorTypeDto> notAppliedExemptFamilies) {
        List<String> violations = new ArrayList<>();
        if (maxErroredCount >= 0 && errored > maxErroredCount) {
            violations.add("real-failure ERRORED count " + errored
                    + " exceeds maxErroredCount " + maxErroredCount
                    + " (dead sessions, shim violations, or mutation crashes)");
        }
        if (maxNotAppliedRatio >= 0) {
            boolean scoped = notAppliedExemptFamilies != null
                    && !notAppliedExemptFamilies.isEmpty()
                    && familyStats != null
                    && !familyStats.isEmpty();
            int scopedTotal = total;
            int scopedSkipped = skipped;
            int scopedNotApplied = notApplied;
            if (scoped) {
                scopedTotal = 0;
                scopedSkipped = 0;
                scopedNotApplied = 0;
                for (Map.Entry<OperatorTypeDto, FamilyStats> entry : familyStats.entrySet()) {
                    if (notAppliedExemptFamilies.contains(entry.getKey())) {
                        continue;
                    }
                    FamilyStats stats = entry.getValue();
                    scopedTotal += stats.getTotal();
                    scopedSkipped += stats.getSkipped();
                    scopedNotApplied += stats.getNotApplied();
                }
            }
            int denominator = scopedTotal - scopedSkipped;
            double ratio = denominator <= 0 ? 0.0 : (scopedNotApplied / (double) denominator);
            if (ratio > maxNotAppliedRatio) {
                String scope = scoped ? " (excluding " + notAppliedExemptFamilies + ")" : "";
                violations.add("not-applied ratio " + ratio + " (" + scopedNotApplied + "/"
                        + denominator + ")" + scope
                        + " exceeds maxNotAppliedRatio " + maxNotAppliedRatio);
            }
        }
        return violations;
    }

    /**
     * Aggregates per-family population counts from a catalog + outcome map.
     * Families with no catalogued mutants are absent from the result. A
     * catalogued mutant with no recorded outcome still counts toward its
     * family's {@code total} — mirroring {@code summary.totalMutants}.
     */
    public static Map<OperatorTypeDto, FamilyStats> familyStats(
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results) {
        Map<OperatorTypeDto, int[]> acc = new EnumMap<>(OperatorTypeDto.class);
        for (MutantMetadata meta : catalog) {
            int[] counts = acc.computeIfAbsent(meta.getOperatorType(), k -> new int[3]);
            counts[0]++;
            MutantResult result = results.get(meta.getMutantId());
            if (result == null) {
                continue;
            }
            switch (result.getStatus()) {
                case NOT_APPLIED -> counts[1]++;
                case SKIPPED -> counts[2]++;
                default -> { }
            }
        }
        Map<OperatorTypeDto, FamilyStats> stats = new EnumMap<>(OperatorTypeDto.class);
        acc.forEach((family, counts) ->
                stats.put(family, new FamilyStats(counts[0], counts[1], counts[2])));
        return stats;
    }

    /**
     * Parses a comma-separated list of {@link OperatorTypeDto} names
     * (case-insensitive, blank-tolerant) into a family set. Unknown tokens are
     * ignored, so a typo narrows the check's scope rather than failing the run.
     */
    public static Set<OperatorTypeDto> parseOperatorTypesCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return Set.of();
        }
        return parseOperatorTypes(Arrays.asList(csv.split(",")));
    }

    /** @see #parseOperatorTypesCsv(String) */
    public static Set<OperatorTypeDto> parseOperatorTypes(Collection<String> names) {
        Set<OperatorTypeDto> families = EnumSet.noneOf(OperatorTypeDto.class);
        if (names == null) {
            return families;
        }
        for (String name : names) {
            if (name == null || name.isBlank()) {
                continue;
            }
            try {
                families.add(OperatorTypeDto.valueOf(name.trim().toUpperCase(Locale.ROOT)));
            } catch (IllegalArgumentException ignored) {
                // Unknown family name: ignore (a typo must not fail the run).
            }
        }
        return families;
    }

    /**
     * Per-operator-family outcome populations feeding the WP-24 not-applied
     * gate. {@code total} counts catalogued mutants of the family (whether or
     * not they produced an outcome); {@code notApplied} and {@code skipped}
     * count recorded outcomes.
     */
    public static final class FamilyStats {

        private final int total;
        private final int notApplied;
        private final int skipped;

        public FamilyStats(int total, int notApplied, int skipped) {
            this.total = total;
            this.notApplied = notApplied;
            this.skipped = skipped;
        }

        public int getTotal() {
            return total;
        }

        public int getNotApplied() {
            return notApplied;
        }

        public int getSkipped() {
            return skipped;
        }

        @Override
        public String toString() {
            return "FamilyStats{total=" + total + ", notApplied=" + notApplied
                    + ", skipped=" + skipped + '}';
        }
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