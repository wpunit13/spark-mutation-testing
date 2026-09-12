package io.github.wpunit13.mutator.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Cross-process transport for per-mutant terminal outcomes.
 *
 * <p>Each mutant fork writes one {@code <mutantId>.json} under
 * {@code <outputDir>/outcomes/}; the aggregating Mojo reads every file back
 * and merges them into the final report. One file per mutant (rather than a
 * shared append log) is what makes parallel mutant execution safe: each fork
 * owns a disjoint write path, so there is no inter-process contention, and a
 * later merge pass produces the single authoritative report.
 */
public final class OutcomeFileStore {

    public static final String OUTCOMES_DIR_NAME = "outcomes";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OutcomeFileStore() {
    }

    public static Path outcomesDir(Path outputDir) {
        return outputDir.resolve(OUTCOMES_DIR_NAME);
    }

    /**
     * Writes {@code result} to {@code <outputDir>/outcomes/<mutantId>.json}.
     */
    public static void writeOutcome(Path outputDir, MutantResult result) throws IOException {
        Path dir = outcomesDir(outputDir);
        Files.createDirectories(dir);

        ObjectNode node = MAPPER.createObjectNode();
        node.put("mutantId", result.getMutantId());
        node.put("status", result.getStatus().name());
        node.put("elapsedMillis", result.getElapsedMillis());
        if (result.getFailureDetailOrNull() == null) {
            node.putNull("failureDetailOrNull");
        } else {
            node.put("failureDetailOrNull", result.getFailureDetailOrNull());
        }
        node.put("recordedAtEpochMillis", result.getRecordedAtEpochMillis());

        Path target = dir.resolve(result.getMutantId() + ".json");
        Files.writeString(
                target,
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node) + "\n",
                StandardCharsets.UTF_8);
    }

    /**
     * Reads every {@code *.json} under {@code <outputDir>/outcomes/} into a
     * {@code mutantId -> MutantResult} map. Returns an empty map (never null)
     * when the directory is absent. A malformed file aborts with
     * {@link IOException} rather than being silently skipped, so a corrupt
     * outcome surfaces loudly instead of corrupting the merged report.
     */
    public static Map<String, MutantResult> readOutcomes(Path outputDir) throws IOException {
        Path dir = outcomesDir(outputDir);
        Map<String, MutantResult> results = new LinkedHashMap<>();
        if (!Files.isDirectory(dir)) {
            return results;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.json")) {
            for (Path file : stream) {
                MutantResult result = parse(Files.readString(file), file.getFileName().toString());
                results.put(result.getMutantId(), result);
            }
        }
        return results;
    }

    private static MutantResult parse(String json, String fileName) throws IOException {
        JsonNode node = MAPPER.readTree(json);
        if (node == null || !node.isObject()) {
            throw new IOException("Outcome file " + fileName + " must be a JSON object.");
        }
        String mutantId = node.path("mutantId").asText();
        if (mutantId == null || mutantId.isBlank()) {
            throw new IOException("Outcome file " + fileName + " is missing a non-blank mutantId.");
        }
        MutantStatus status;
        try {
            status = MutantStatus.valueOf(node.path("status").asText());
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IOException("Outcome file " + fileName + " has invalid status: " + node.path("status").asText());
        }
        long elapsedMillis = node.path("elapsedMillis").asLong(0L);
        JsonNode detailNode = node.get("failureDetailOrNull");
        String failureDetail = (detailNode == null || detailNode.isNull()) ? null : detailNode.asText();
        long recordedAt = node.path("recordedAtEpochMillis").asLong(0L);
        return new MutantResult(mutantId, status, elapsedMillis, failureDetail, recordedAt);
    }
}