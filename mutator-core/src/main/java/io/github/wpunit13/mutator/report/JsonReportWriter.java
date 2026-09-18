package io.github.wpunit13.mutator.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Writes the primary mutation-report.json artifact, conforming to schema
 * version 2 of docs/CONTRACTS.md §5.3 (WP-24: NOT_APPLIED split out of
 * ERRORED).
 */
public final class JsonReportWriter {

    static final String FILE_NAME = "mutation-report.json";
    private static final String SCHEMA_URL =
            "https://spark-mutator.wpunit13.io/schema/mutation-report/v2.json";
    private static final Pattern HEX16 = Pattern.compile("^[0-9a-f]{16}$");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonReportWriter() {
    }

    static Path write(
            Path outputDir,
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results) throws IOException {
        return write(outputDir, catalog, results, ReportWriter.Config.fromSystemProperties());
    }

    static Path write(
            Path outputDir,
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results,
            ReportWriter.Config runConfig) throws IOException {
        // Contract-violation check: mutantId and coordinateHex must be exactly
        // 16 lowercase hex characters; never silently accepted.
        for (MutantMetadata meta : catalog) {
            if (!HEX16.matcher(meta.getMutantId()).matches()) {
                throw new IllegalStateException(
                        "Contract violation: mutantId '" + meta.getMutantId()
                                + "' is not 16 lowercase hex characters.");
            }
            if (!HEX16.matcher(meta.getCoordinateHex()).matches()) {
                throw new IllegalStateException(
                        "Contract violation: coordinateHex '" + meta.getCoordinateHex()
                                + "' for mutantId '" + meta.getMutantId()
                                + "' is not 16 lowercase hex characters.");
            }
        }

        List<MutantMetadata> sorted = catalog.stream()
                .sorted(Comparator.comparing(MutantMetadata::getMutantId))
                .toList();

        int killed = 0;
        int survived = 0;
        int timedOut = 0;
        int errored = 0;
        int notApplied = 0;
        int skipped = 0;
        for (MutantMetadata meta : sorted) {
            MutantResult result = resolveResult(meta, results);
            switch (result.getStatus()) {
                case KILLED -> killed++;
                case SURVIVED -> survived++;
                case TIMED_OUT -> timedOut++;
                case ERRORED -> errored++;
                case NOT_APPLIED -> notApplied++;
                case SKIPPED -> skipped++;
            }
        }

        ObjectNode root = MAPPER.createObjectNode();
        root.put("$schema", SCHEMA_URL);
        root.put("schemaVersion", 2);
        root.put("generatedAtEpochMillis", System.currentTimeMillis());

        ObjectNode config = root.putObject("config");
        // WP-19 config wiring: the config block's fields are part of the
        // frozen §5.3 schema and are now populated from the run's actual
        // configuration instead of placeholder literals.
        ArrayNode targetModules = config.putArray("targetModules");
        runConfig.getTargetModules().forEach(targetModules::add);
        ArrayNode excludedMutators = config.putArray("excludedMutators");
        runConfig.getExcludedMutators().forEach(excludedMutators::add);
        config.put("timeoutMultiplier", runConfig.getTimeoutMultiplier());
        config.put("minMutationScore", runConfig.getMinMutationScore());

        ObjectNode summary = root.putObject("summary");
        summary.put("totalMutants", catalog.size());
        summary.put("killed", killed);
        summary.put("survived", survived);
        summary.put("timedOut", timedOut);
        summary.put("errored", errored);
        summary.put("notApplied", notApplied);
        summary.put("skipped", skipped);
        summary.set("mutationScore", DoubleNode.valueOf(mutationScore(killed, timedOut, survived)));

        ArrayNode mutants = root.putArray("mutants");
        for (MutantMetadata meta : sorted) {
            MutantResult result = resolveResult(meta, results);
            ObjectNode element = mutants.addObject();
            element.put("mutantId", meta.getMutantId());
            element.put("filePath", meta.getFilePath());
            element.put("lineNumber", meta.getLineNumber());
            element.put("operatorType", meta.getOperatorType().name());
            element.put("mutationIndex", meta.getMutationIndex());
            element.put("description", meta.getDescription());
            element.put("coordinateHex", meta.getCoordinateHex());
            element.put("astDiffSnippet", meta.getAstDiffSnippet());
            ArrayNode testIds = element.putArray("mappedTestIds");
            meta.getMappedTestIds().forEach(testIds::add);
            ObjectNode resultNode = element.putObject("result");
            resultNode.put("status", result.getStatus().name());
            resultNode.put("elapsedMillis", result.getElapsedMillis());
            if (result.getFailureDetailOrNull() == null) {
                resultNode.putNull("failureDetailOrNull");
            } else {
                resultNode.put("failureDetailOrNull", result.getFailureDetailOrNull());
            }
            resultNode.put("recordedAtEpochMillis", result.getRecordedAtEpochMillis());
        }

        Path target = outputDir.resolve(FILE_NAME);
        Files.createDirectories(outputDir);
        Files.writeString(
                target,
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n",
                StandardCharsets.UTF_8);
        return target;
    }

    private static MutantResult resolveResult(
            MutantMetadata meta, Map<String, MutantResult> results) {
        MutantResult result = results.get(meta.getMutantId());
        if (result != null) {
            return result;
        }
        // Transiently-missing outcome (run terminated early): classify as
        // ERRORED so the schema's result field is never actually null.
        return new MutantResult(
                meta.getMutantId(),
                MutantStatus.ERRORED,
                0L,
                "no outcome recorded; run terminated early",
                System.currentTimeMillis());
    }

    /**
     * ((killed + timedOut) / (killed + timedOut + survived)) * 100, rounded
     * to 2 decimals. Errored and skipped mutants are excluded from both
     * numerator and denominator. Zero denominator yields 0.0 — never NaN,
     * never Infinity.
     */
    static double mutationScore(int killed, int timedOut, int survived) {
        int denominator = killed + timedOut + survived;
        if (denominator == 0) {
            return 0.0;
        }
        double raw = ((killed + timedOut) / (double) denominator) * 100;
        return Math.round(raw * 100.0) / 100.0;
    }
}
