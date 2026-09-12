package io.github.wpunit13.mutator.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Cross-process transport for the Discovery-phase mutant catalog.
 *
 * <p>The baseline fork writes {@code catalog.json} into the shared output
 * directory; the aggregating Maven Mojo reads it back. This is the bridge
 * that lets the {@code InMemoryMutationCatalog} (a per-JVM singleton, populated
 * only in the forked test JVM) reach the orchestrator running in the Maven JVM.
 *
 * <p>The schema is intentionally a superset of
 * {@link MutationCatalogAccess#getFullCatalogJson()} — it additionally carries
 * {@code astDiffSnippet} and preserves {@code mappedTestIds} — because the
 * report layer needs the complete {@link MutantMetadata} to render its
 * artifacts, not just the ID/coordinate surface.
 */
public final class MutationCatalogIo {

    public static final String CATALOG_FILE_NAME = "catalog.json";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MutationCatalogIo() {
    }

    /**
     * Writes every entry as a JSON array to {@code <outputDir>/catalog.json}.
     * Entries are ordered by mutantId so output is deterministic across runs
     * and independent of the caller's collection ordering.
     */
    public static void writeCatalogJson(Path outputDir, Collection<MutantMetadata> entries) throws IOException {
        Files.createDirectories(outputDir);
        List<MutantMetadata> sorted = entries.stream()
                .sorted(Comparator.comparing(MutantMetadata::getMutantId))
                .toList();
        ArrayNode array = MAPPER.createArrayNode();
        for (MutantMetadata meta : sorted) {
            ObjectNode element = array.addObject();
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
        }
        Files.writeString(
                outputDir.resolve(CATALOG_FILE_NAME),
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(array) + "\n",
                StandardCharsets.UTF_8);
    }

    /**
     * Reads {@code <outputDir>/catalog.json} back into a list of
     * {@link MutantMetadata}. Returns an empty list (never null) when the file
     * is absent — a legitimately empty catalog is indistinguishable from a
     * missing one at this layer, so callers must not treat "file absent" as a
     * fatal error.
     */
    public static List<MutantMetadata> readCatalogJson(Path outputDir) throws IOException {
        Path file = outputDir.resolve(CATALOG_FILE_NAME);
        if (!Files.isRegularFile(file)) {
            return List.of();
        }
        JsonNode root = MAPPER.readTree(Files.readString(file));
        if (root == null || !root.isArray()) {
            throw new IOException("catalog.json root must be a JSON array, got: " + (root == null ? "null" : root.getNodeType()));
        }
        List<MutantMetadata> result = new ArrayList<>();
        for (JsonNode element : root) {
            result.add(parseEntry(element));
        }
        return result;
    }

    private static MutantMetadata parseEntry(JsonNode e) {
        String mutantId = e.path("mutantId").asText();
        String filePath = e.path("filePath").asText();
        int lineNumber = e.path("lineNumber").asInt(-1);
        OperatorTypeDto operatorType = OperatorTypeDto.valueOf(e.path("operatorType").asText());
        int mutationIndex = e.path("mutationIndex").asInt(0);
        String description = e.path("description").asText();
        String coordinateHex = e.path("coordinateHex").asText();
        String astDiffSnippet = e.path("astDiffSnippet").asText("");
        List<String> mappedTestIds = new ArrayList<>();
        JsonNode idsNode = e.get("mappedTestIds");
        if (idsNode != null && idsNode.isArray()) {
            for (JsonNode id : idsNode) {
                mappedTestIds.add(id.asText());
            }
        }
        return new MutantMetadata(
                mutantId,
                filePath,
                lineNumber,
                operatorType,
                mutationIndex,
                description,
                coordinateHex,
                astDiffSnippet,
                mappedTestIds);
    }
}