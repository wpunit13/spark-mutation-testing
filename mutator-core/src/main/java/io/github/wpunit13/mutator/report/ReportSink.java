package io.github.wpunit13.mutator.report;

import io.github.wpunit13.mutator.MutantBootstrap;
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JVM-wide singleton receiving each mutant's terminal outcome from the
 * Python harness, and finalizing the report artifacts.
 */
public final class ReportSink {


    private final Map<String, MutantResult> results = new ConcurrentHashMap<>();

    private ReportSink() {
    }

    private static ReportSink instance() {
        return Holder.INSTANCE;
    }

    private static final class Holder {
        static final ReportSink INSTANCE = new ReportSink();
    }

    /**
     * Records one mutant's terminal outcome. Called by the Python harness
     * immediately after classifying a mutant, once per mutant, exactly once.
     *
     * @param mutantId must already exist in the catalog.
     * @param status one of "KILLED", "SURVIVED", "TIMED_OUT", "ERRORED",
     *        "NOT_APPLIED" (exact string match, case-sensitive; any other
     *        value throws IllegalArgumentException).
     * @param elapsedMillis wall-clock duration of this mutant's test execution.
     * @param failureDetailOrNull for KILLED/ERRORED: the failing assertion
     *        message or exception string; null for SURVIVED; null or a
     *        diagnostic reason string for TIMED_OUT.
     * @throws IllegalArgumentException if mutantId is unknown or status is not
     *         a valid enum value.
     * @throws IllegalStateException if this mutantId has already been recorded
     *         (results are write-once; re-recording indicates a harness-level
     *         double-execution bug).
     */
    public static void recordOutcome(
            String mutantId,
            String status,
            long elapsedMillis,
            String failureDetailOrNull) {
        MutantStatus parsed;
        try {
            parsed = MutantStatus.valueOf(status);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalArgumentException(
                    "Unknown status '" + status + "': must be one of "
                            + java.util.Arrays.toString(MutantStatus.values()) + ".");
        }
        if (MutationCatalogAccess.findByIdOrNull(mutantId) == null) {
            throw new IllegalArgumentException(
                    "Unknown mutantId '" + mutantId + "': not present in the catalog.");
        }
        MutantResult result = new MutantResult(
                mutantId,
                parsed,
                elapsedMillis,
                failureDetailOrNull,
                System.currentTimeMillis());
        if (instance().results.putIfAbsent(mutantId, result) != null) {
            throw new IllegalStateException(
                    "Mutant '" + mutantId + "' has already been recorded; results are write-once.");
        }
    }

    /**
     * Finalizes and writes all configured report artifacts (terminal table,
     * HTML, mutation-report.json, SARIF) to the configured output directory.
     * Called once, after the mutation loop exhausts all mutants.
     *
     * @return the absolute path of the primary mutation-report.json as a String.
     */
    public static String finalizeAndWriteReports() {
        Path outputDir = resolveOutputDir();
        return ReportWriter.writeReports(
                outputDir,
                MutationCatalogAccess.allEntries(),
                Map.copyOf(instance().results));
    }

    /**
     * Count of recorded outcomes carrying the given status. Used by the
     * WP-24 governance gate (real-failure ERRORED and not-applied ratio).
     */
    public static int countByStatus(MutantStatus status) {
        int count = 0;
        for (MutantResult result : instance().results.values()) {
            if (result.getStatus() == status) {
                count++;
            }
        }
        return count;
    }

    /**
     * Per-family outcome populations for the WP-24 not-applied gate, derived
     * from the singleton catalog (operator types) and the recorded outcomes.
     * Used by the in-process paths (JUnit 5 extension, PySpark driver) to scope
     * the gate to an accountable set of operator families.
     */
    public static Map<OperatorTypeDto, ReportWriter.FamilyStats> familyStats() {
        return ReportWriter.familyStats(MutationCatalogAccess.allEntries(), snapshotResults());
    }

    /** Read accessor used by the report writers. */
    static Map<String, MutantResult> snapshotResults() {
        return Map.copyOf(instance().results);
    }

    /**
     * Computes the mutation score from the in-memory catalog + recorded
     * outcomes. Convenience for in-process callers (the JUnit5 extension) that
     * need to apply a {@code minMutationScore} gate without reading the report.
     */
    public static double computeMutationScore() {
        return ReportWriter.computeScore(MutationCatalogAccess.allEntries(), snapshotResults());
    }

    /** Test-only reset; clears every recorded outcome. */
    public static void clearForTesting() {
        instance().results.clear();
    }

    private static Path resolveOutputDir() {
        String configured = System.getProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY);
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("SPARK_MUTATOR_OUTPUT_DIR");
        }
        Path dir = configured == null || configured.isBlank()
                ? Paths.get("target", "spark-mutator-reports")
                : Paths.get(configured);
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new IllegalStateException("Could not create output directory " + dir, e);
        }
        return dir;
    }
}
