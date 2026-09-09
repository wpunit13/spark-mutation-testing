package io.github.wpunit13.mutator.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.TestContextTracker;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MutationCatalogTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void resetSingletons() {
        MutationCatalogAccess.clearForTesting();
        TestContextTracker.clearCurrentTestId();
    }

    @AfterEach
    void clearThreadLocal() {
        TestContextTracker.clearCurrentTestId();
    }

    private void register(String mutantId, String coordinateHex) {
        MutationCatalogAccess.sink().registerCandidate(
                "my_pipeline/transforms/orders.py",
                42,
                OperatorTypeDto.JOIN,
                1,
                "INNER -> CROSS",
                coordinateHex,
                mutantId);
    }

    @Test
    void idempotentAdditiveMergeAppendsDistinctTestIdsInOrder() {
        TestContextTracker.setCurrentTestId("t1");
        register("aaaaaaaaaaaaaaaa", "9f8e7d6c5b4a3210");
        TestContextTracker.setCurrentTestId("t2");
        register("aaaaaaaaaaaaaaaa", "9f8e7d6c5b4a3210");

        List<MutantMetadata> entries = List.copyOf(MutationCatalogAccess.allEntries());
        assertEquals(1, entries.size(), "re-registering the same mutant must not duplicate");
        assertEquals(List.of("t1", "t2"), entries.get(0).getMappedTestIds());
    }

    @Test
    void sameTestIdRegisteredTwiceYieldsExactlyOneMappedTestId() {
        TestContextTracker.setCurrentTestId("t1");
        register("bbbbbbbbbbbbbbbb", "1111111111111111");
        register("bbbbbbbbbbbbbbbb", "1111111111111111");

        List<MutantMetadata> entries = List.copyOf(MutationCatalogAccess.allEntries());
        assertEquals(1, entries.size());
        assertEquals(List.of("t1"), entries.get(0).getMappedTestIds());
    }

    @Test
    void registrationWithUnsetTrackerYieldsEmptyMappedTestIds() {
        TestContextTracker.clearCurrentTestId();
        register("cccccccccccccccc", "2222222222222222");

        List<MutantMetadata> entries = List.copyOf(MutationCatalogAccess.allEntries());
        assertEquals(1, entries.size());
        assertTrue(entries.get(0).getMappedTestIds().isEmpty());
        assertEquals("", entries.get(0).getAstDiffSnippet());
    }

    @Test
    void getFullCatalogJsonHasExactKeySetAndAscendingMutantIdOrder() throws Exception {
        register("bbbbbbbbbbbbbbbb", "1111111111111111");
        register("aaaaaaaaaaaaaaaa", "9f8e7d6c5b4a3210");

        JsonNode array = mapper.readTree(MutationCatalogAccess.getFullCatalogJson());
        assertTrue(array.isArray());
        assertEquals(2, array.size());

        Iterator<JsonNode> elements = array.elements();
        JsonNode first = elements.next();
        JsonNode second = elements.next();
        assertEquals("aaaaaaaaaaaaaaaa", first.get("mutantId").asText());
        assertEquals("bbbbbbbbbbbbbbbb", second.get("mutantId").asText());

        List<String> expectedKeys = List.of(
                "mutantId",
                "filePath",
                "lineNumber",
                "operatorType",
                "mutationIndex",
                "description",
                "coordinateHex",
                "mappedTestIds");
        assertEquals(expectedKeys, fieldNamesOf(first));
        assertEquals(expectedKeys, fieldNamesOf(second));
    }

    @Test
    void getMappedTestIdsJsonOnUnknownIdThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> MutationCatalogAccess.getMappedTestIdsJson("ffffffffffffffff"));
    }

    private static List<String> fieldNamesOf(JsonNode node) {
        List<String> names = new java.util.ArrayList<>();
        node.fieldNames().forEachRemaining(names::add);
        return names;
    }
}
