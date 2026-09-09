package io.github.wpunit13.mutator.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JsonReportWriterTest {

    private static final String[] HEX_IDS = {
            "0000000000000001", "0000000000000002", "0000000000000003",
            "0000000000000004", "0000000000000005", "0000000000000006",
            "0000000000000007", "0000000000000008", "0000000000000009",
            "000000000000000a", "000000000000000b"
    };

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private MutantMetadata metadata(int index) {
        return new MutantMetadata(
                HEX_IDS[index],
                "my_pipeline/transforms/orders.py",
                42,
                OperatorTypeDto.JOIN,
                1,
                "INNER -> CROSS",
                "9f8e7d6c5b4a3210",
                "- Join Inner\n+ Join Cross",
                List.of("test_orders.py::test_join"));
    }

    private Map<String, MutantResult> results(Object... pairs) {
        Map<String, MutantResult> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            int index = (Integer) pairs[i];
            MutantStatus status = (MutantStatus) pairs[i + 1];
            map.put(HEX_IDS[index], new MutantResult(
                    HEX_IDS[index], status, 100L, null, 1732000000123L));
        }
        return map;
    }

    private JsonNode writeAndParse(int metadataCount, Map<String, MutantResult> results)
            throws Exception {
        List<MutantMetadata> catalog = new ArrayList<>();
        for (int i = 0; i < metadataCount; i++) {
            catalog.add(metadata(i));
        }
        Path written = JsonReportWriter.write(tempDir, catalog, results);
        assertTrue(Files.exists(written));
        return mapper.readTree(written.toFile());
    }

    @Test
    void scoreThreeKilledOneSurvivedIs75() throws Exception {
        JsonNode root = writeAndParse(4, results(
                0, MutantStatus.KILLED,
                1, MutantStatus.KILLED,
                2, MutantStatus.KILLED,
                3, MutantStatus.SURVIVED));
        assertEquals(75.0, root.get("summary").get("mutationScore").asDouble(), 0.0);
    }

    @Test
    void scoreTwoKilledOneTimedOutOneSurvivedIs75() throws Exception {
        JsonNode root = writeAndParse(4, results(
                0, MutantStatus.KILLED,
                1, MutantStatus.KILLED,
                2, MutantStatus.TIMED_OUT,
                3, MutantStatus.SURVIVED));
        assertEquals(75.0, root.get("summary").get("mutationScore").asDouble(), 0.0);
    }

    @Test
    void scoreExcludesErroredAndSkippedFromBothNumeratorAndDenominator() throws Exception {
        JsonNode root = writeAndParse(9, results(
                0, MutantStatus.KILLED,
                1, MutantStatus.ERRORED,
                2, MutantStatus.ERRORED,
                3, MutantStatus.ERRORED,
                4, MutantStatus.ERRORED,
                5, MutantStatus.ERRORED,
                6, MutantStatus.SKIPPED,
                7, MutantStatus.SKIPPED,
                8, MutantStatus.SKIPPED));
        assertEquals(100.0, root.get("summary").get("mutationScore").asDouble(), 0.0);
        assertEquals(9, root.get("summary").get("totalMutants").asInt());
        assertEquals(5, root.get("summary").get("errored").asInt());
        assertEquals(3, root.get("summary").get("skipped").asInt());
    }

    @Test
    void scoreZeroDenominatorIsZeroNotNaN() throws Exception {
        JsonNode root = writeAndParse(2, results(
                0, MutantStatus.ERRORED,
                1, MutantStatus.ERRORED));
        JsonNode scoreNode = root.get("summary").get("mutationScore");
        assertTrue(scoreNode.isNumber(), "mutationScore must be a JSON number, not a string");
        assertEquals(0.0, scoreNode.asDouble(), 0.0);
    }

    @Test
    void scoreOneKilledTwoSurvivedRoundsTo33Point33() throws Exception {
        JsonNode root = writeAndParse(3, results(
                0, MutantStatus.KILLED,
                1, MutantStatus.SURVIVED,
                2, MutantStatus.SURVIVED));
        assertEquals(33.33, root.get("summary").get("mutationScore").asDouble(), 0.0);
    }

    @Test
    void malformedCoordinateHexThrowsIllegalState() {
        List<MutantMetadata> catalog = List.of(new MutantMetadata(
                HEX_IDS[0],
                "my_pipeline/transforms/orders.py",
                42,
                OperatorTypeDto.JOIN,
                1,
                "INNER -> CROSS",
                "NOTVALIDHEX",
                "",
                List.of()));
        assertThrows(IllegalStateException.class,
                () -> JsonReportWriter.write(tempDir, catalog, Map.of()));
    }
}
