package io.github.wpunit13.mutator.report;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
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

    private ReportWriter() {
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
            Path json = JsonReportWriter.write(outputDir, catalog, snapshot);
            SarifReportWriter.write(outputDir, catalog, snapshot);
            HtmlReportWriter.write(outputDir, catalog, snapshot);
            return json.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write reports to " + outputDir, e);
        }
    }
}