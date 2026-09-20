package io.github.wpunit13.mutator.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestValueReportWriterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private static MutantMetadata meta(String mutantId, List<String> mappedTests) {
        return new MutantMetadata(
                mutantId,
                "unknown",
                -1,
                OperatorTypeDto.JOIN,
                1,
                "INNER -> CROSS",
                "9f8e7d6c5b4a3210",
                "",
                mappedTests);
    }

    private static MutantResult killedWith(String... failingTests) {
        return new MutantResult(
                "0000000000000000",
                MutantStatus.KILLED,
                100L,
                FailingTests.append("There are test failures.", List.of(failingTests)),
                1732000000123L);
    }

    @Test
    void soleKillerIsLoadBearingSharedKillerIsRedundantCandidate() throws Exception {
        // mutant 1: both tests fail (shared kill for both)
        // mutant 2: only the strong test fails (sole kill for the strong test)
        MutantMetadata m1 = meta("0000000000000001", List.of("pipeline.WeakTest.count", "pipeline.StrongTest.rows"));
        MutantMetadata m2 = meta("0000000000000002", List.of("pipeline.WeakTest.count", "pipeline.StrongTest.rows"));

        Map<String, MutantResult> results = new HashMap<>();
        results.put(m1.getMutantId(), new MutantResult(
                m1.getMutantId(), MutantStatus.KILLED, 100L,
                FailingTests.append("fail", List.of("pipeline.WeakTest.count", "pipeline.StrongTest.rows")),
                1L));
        results.put(m2.getMutantId(), new MutantResult(
                m2.getMutantId(), MutantStatus.KILLED, 100L,
                FailingTests.append("fail", List.of("pipeline.StrongTest.rows")),
                1L));

        TestValueReportWriter.TestValueReport report =
                TestValueReportWriter.compute(List.of(m1, m2), results);

        assertEquals(2, report.getKilledTotal());
        assertEquals(2, report.getAttributedKilled());
        assertEquals(1.0, report.attributionCoverage());

        Map<String, TestValueReportWriter.TestValue> byId = new LinkedHashMap<>();
        for (TestValueReportWriter.TestValue t : report.getTests()) {
            byId.put(t.getTestId(), t);
        }
        TestValueReportWriter.TestValue weak = byId.get("pipeline.WeakTest.count");
        TestValueReportWriter.TestValue strong = byId.get("pipeline.StrongTest.rows");

        assertEquals(2, weak.getMutantsRun());
        assertEquals(0, weak.getSoleKills());
        assertEquals(1, weak.getSharedKills());
        assertEquals(TestValueReportWriter.VERDICT_REDUNDANT_CANDIDATE, weak.getVerdict());

        assertEquals(2, strong.getMutantsRun());
        assertEquals(1, strong.getSoleKills());
        assertEquals(1, strong.getSharedKills());
        assertEquals(TestValueReportWriter.VERDICT_LOAD_BEARING, strong.getVerdict());
    }

    @Test
    void killedMutantsWithoutAttributionYieldInsufficientData() throws Exception {
        MutantMetadata m1 = meta("0000000000000001", List.of("pipeline.WeakTest.count"));
        Map<String, MutantResult> results = new HashMap<>();
        // Pre-attribution-era detail: killed, but no failing-test block.
        results.put(m1.getMutantId(), new MutantResult(
                m1.getMutantId(), MutantStatus.KILLED, 100L,
                "There are test failures.\nPlease refer to surefire-reports", 1L));

        TestValueReportWriter.TestValueReport report =
                TestValueReportWriter.compute(List.of(m1), results);

        assertEquals(1, report.getKilledTotal());
        assertEquals(0, report.getAttributedKilled());
        assertEquals(0.0, report.attributionCoverage());
        assertEquals(
                TestValueReportWriter.VERDICT_INSUFFICIENT_DATA,
                report.getTests().get(0).getVerdict());
    }

    @Test
    void writesJsonAndHtmlArtifacts() throws Exception {
        MutantMetadata m1 = meta("0000000000000001", List.of("pipeline.StrongTest.rows"));
        Map<String, MutantResult> results = new HashMap<>();
        results.put(m1.getMutantId(), killedWith("pipeline.StrongTest.rows"));

        Path json = TestValueReportWriter.write(tempDir, List.of(m1), results);

        assertTrue(Files.exists(json), "test-value-report.json written");
        assertTrue(Files.exists(tempDir.resolve("test-value-report.html")), "test-value-report.html written");

        JsonNode root = mapper.readTree(json.toFile());
        assertEquals(1, root.get("killedTotal").asInt());
        assertEquals(1, root.get("attributedKilled").asInt());
        assertEquals(1, root.get("attributionCoverage").asDouble());
        assertEquals("pipeline.StrongTest.rows", root.get("tests").get(0).get("testId").asText());
        assertEquals(TestValueReportWriter.VERDICT_LOAD_BEARING, root.get("tests").get(0).get("verdict").asText());

        String html = Files.readString(tempDir.resolve("test-value-report.html"));
        assertTrue(html.contains("pipeline.StrongTest.rows"), () -> html);
        assertTrue(html.contains(TestValueReportWriter.VERDICT_LOAD_BEARING), () -> html);
        assertTrue(html.contains("REDUNDANT_CANDIDATE means"), () -> html);
    }
}
