package io.github.wpunit13.mutator.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Cross-process transport for the per-mutant plan-diff snippet (WP-19).
 *
 * <p>The mutant fork captures the matched plan node's before/after shapes at
 * rewrite time and persists one {@code <mutantId>.json} under
 * {@code <outputDir>/diffs/}; the aggregating Mojo (a different JVM) reads the
 * snippet back and merges it into its in-memory catalog copy before the final
 * reports are written. One file per mutant mirrors {@link AppliedMarkerStore}:
 * each fork owns a disjoint write path, so there is no inter-process
 * contention.
 *
 * <p>This is a sidecar under the report directory, deliberately NOT a field
 * in {@code mutation-report.json} (the report schema is frozen;
 * {@code astDiffSnippet} already exists on each mutant entry and is populated
 * from this transport). Unlike the applied marker, a MISSING sidecar is legal
 * — the snippet is optional report-facing data, and absence simply leaves the
 * field empty. A malformed or mismatched sidecar, however, is a contract
 * violation and fails loudly.
 */
public final class DiffSnippetStore {

    public static final String DIFFS_DIR_NAME = "diffs";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DiffSnippetStore() {
    }

    public static Path diffsDir(Path outputDir) {
        return outputDir.resolve(DIFFS_DIR_NAME);
    }

    /**
     * Writes {@code <outputDir>/diffs/<mutantId>.json} containing
     * {@code {"mutantId": ..., "astDiffSnippet": ...}}, creating the
     * {@code diffs/} directory on demand.
     */
    public static void write(Path outputDir, String mutantId, String astDiffSnippet) throws IOException {
        Path dir = diffsDir(outputDir);
        Files.createDirectories(dir);

        ObjectNode node = MAPPER.createObjectNode();
        node.put("mutantId", mutantId);
        node.put("astDiffSnippet", astDiffSnippet);

        Path target = dir.resolve(mutantId + ".json");
        Files.writeString(
                target,
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node) + "\n",
                StandardCharsets.UTF_8);
    }

    /**
     * Reads {@code <outputDir>/diffs/<mutantId>.json} and returns its
     * {@code astDiffSnippet}, or {@code null} when the sidecar is absent
     * (absence is legal — the snippet is optional). Never creates anything.
     *
     * @throws IOException if the sidecar exists but cannot be read or parsed.
     * @throws IllegalStateException if the sidecar's {@code mutantId} does not
     *         match the requested id (a mis-filed sidecar is a contract
     *         violation, never silently accepted).
     */
    public static String readSnippetOrNull(Path outputDir, String mutantId) throws IOException {
        Path file = diffsDir(outputDir).resolve(mutantId + ".json");
        if (!Files.isRegularFile(file)) {
            return null;
        }
        JsonNode node = MAPPER.readTree(Files.readString(file, StandardCharsets.UTF_8));
        String recordedId = node.path("mutantId").asText(null);
        if (!mutantId.equals(recordedId)) {
            throw new IllegalStateException(
                    "Contract violation: diff sidecar " + file + " records mutantId '"
                            + recordedId + "' but was read for '" + mutantId + "'.");
        }
        return node.path("astDiffSnippet").asText("");
    }
}
