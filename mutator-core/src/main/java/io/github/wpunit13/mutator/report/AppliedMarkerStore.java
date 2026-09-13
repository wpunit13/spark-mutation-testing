package io.github.wpunit13.mutator.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Cross-process transport for the "mutation was actually applied" fact.
 *
 * <p>The mutant fork persists one {@code <mutantId>.json} under
 * {@code <outputDir>/applied/} after its harness verified that the active
 * mutant's rewrite really executed; the aggregating Mojo (a different JVM)
 * reads the marker's existence back to distinguish "a test killed the mutant"
 * from "the mutation never executed" — the latter must be ERRORED, never a
 * fake SURVIVED. One file per mutant mirrors {@link OutcomeFileStore}: each
 * fork owns a disjoint write path, so there is no inter-process contention.
 *
 * <p>This is a sidecar under the report directory, deliberately NOT a field
 * in {@code mutation-report.json} (the report schema is frozen).
 */
public final class AppliedMarkerStore {

    public static final String APPLIED_DIR_NAME = "applied";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AppliedMarkerStore() {
    }

    public static Path appliedDir(Path outputDir) {
        return outputDir.resolve(APPLIED_DIR_NAME);
    }

    /**
     * Writes {@code <outputDir>/applied/<mutantId>.json} containing
     * {@code {"mutantId": ..., "appliedAtEpochMillis": ...}}, creating the
     * {@code applied/} directory on demand.
     */
    public static void write(Path outputDir, String mutantId) throws IOException {
        Path dir = appliedDir(outputDir);
        Files.createDirectories(dir);

        ObjectNode node = MAPPER.createObjectNode();
        node.put("mutantId", mutantId);
        node.put("appliedAtEpochMillis", System.currentTimeMillis());

        Path target = dir.resolve(mutantId + ".json");
        Files.writeString(
                target,
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node) + "\n",
                StandardCharsets.UTF_8);
    }

    /**
     * Pure existence check for {@code <outputDir>/applied/<mutantId>.json};
     * never creates anything. A missing output directory reads as
     * {@code false}.
     */
    public static boolean exists(Path outputDir, String mutantId) {
        return Files.isRegularFile(appliedDir(outputDir).resolve(mutantId + ".json"));
    }
}