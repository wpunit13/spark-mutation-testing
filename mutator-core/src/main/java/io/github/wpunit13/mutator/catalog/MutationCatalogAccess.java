package io.github.wpunit13.mutator.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.wpunit13.mutator.model.MutantMetadata;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Public read-side facade over the in-memory mutant catalog, plus the
 * accessor through which the registration side obtains the
 * {@link MutationCatalogSink}.
 */
public final class MutationCatalogAccess {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MutationCatalogAccess() {
    }

    /**
     * The registration-side counterpart to the read-side methods; the
     * Catalyst rule obtains the sink through this accessor.
     */
    public static MutationCatalogSink sink() {
        return InMemoryMutationCatalog.getInstance();
    }

    /**
     * Returns an unmodifiable snapshot of every catalogued mutant, including
     * {@code astDiffSnippet} (which {@link #getFullCatalogJson()} deliberately
     * omits). The report layer lives in a different package and needs the full
     * entry set to synthesize missing outcomes and hand the writers a complete
     * catalog; this public accessor is the narrowest way to expose it without
     * coupling the two packages.
     */
    public static Collection<MutantMetadata> allEntries() {
        return InMemoryMutationCatalog.getInstance().allEntries();
    }

    public static MutantMetadata findByIdOrNull(String mutantId) {
        return InMemoryMutationCatalog.getInstance().findByIdOrNull(mutantId);
    }

    /** Test-only reset; delegates to the backing singleton. */
    public static void clearForTesting() {
        InMemoryMutationCatalog.getInstance().clearForTesting();
    }

    /**
     * Seeds the in-memory catalog from externally-supplied metadata — the
     * fork-side half of the cross-process catalog handoff (see
     * {@link MutationCatalogIo}). Idempotent: existing entries are kept.
     */
    public static void loadCatalog(Collection<MutantMetadata> entries) {
        InMemoryMutationCatalog.getInstance().loadAll(entries);
    }

    /**
     * Returns the full Discovery-phase mutant catalog as a JSON array string.
     * Called once, after the baseline phase completes, before the mutation
     * loop begins.
     *
     * <p>Each element shape: {"mutantId": &lt;string&gt;, "filePath":
     * &lt;string&gt;, "lineNumber": &lt;int&gt;, "operatorType": &lt;string&gt;,
     * "mutationIndex": &lt;int&gt;, "description": &lt;string&gt;,
     * "coordinateHex": &lt;string&gt;, "mappedTestIds": [&lt;string&gt;, ...]}.
     * The array is ordered by mutantId ascending so output is deterministic
     * across runs.
     */
    public static String getFullCatalogJson() {
        List<MutantMetadata> sorted = allEntries().stream()
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
            ArrayNode testIds = element.putArray("mappedTestIds");
            meta.getMappedTestIds().forEach(testIds::add);
        }
        return array.toString();
    }

    /**
     * Returns the test-impact-mapped subset relevant to a single mutant, as a
     * JSON array of test id strings, matching {@link MutantMetadata#getMappedTestIds()}
     * for that mutant. Provided as a separate narrow accessor so the Python
     * side is not forced to parse the full catalog just to schedule one
     * mutant's test run.
     *
     * @throws IllegalArgumentException if mutantId is not present in the catalog.
     */
    public static String getMappedTestIdsJson(String mutantId) {
        MutantMetadata meta = findByIdOrNull(mutantId);
        if (meta == null) {
            throw new IllegalArgumentException(
                    "Unknown mutantId '" + mutantId + "': not present in the catalog.");
        }
        ArrayNode array = MAPPER.createArrayNode();
        meta.getMappedTestIds().forEach(array::add);
        return array.toString();
    }
}
