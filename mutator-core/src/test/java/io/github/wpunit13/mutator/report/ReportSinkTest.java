package io.github.wpunit13.mutator.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReportSinkTest {

    private static final String OUTPUT_DIR_PROPERTY = "spark.mutator.outputDirectory";
    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    @BeforeEach
    void resetSingletons() {
        MutationCatalogAccess.clearForTesting();
        ReportSink.clearForTesting();
        System.clearProperty(OUTPUT_DIR_PROPERTY);
    }

    @AfterEach
    void restoreSystemState() {
        System.clearProperty(OUTPUT_DIR_PROPERTY);
    }

    private void register(String mutantId) {
        MutationCatalogAccess.sink().registerCandidate(
                "my_pipeline/transforms/orders.py",
                42,
                OperatorTypeDto.JOIN,
                1,
                "INNER -> CROSS",
                "9f8e7d6c5b4a3210",
                mutantId);
    }

    @Test
    void unknownMutantIdThrowsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> ReportSink.recordOutcome("ffffffffffffffff", "KILLED", 10L, null));
    }

    @Test
    void lowercaseStatusThrowsAndUppercaseSucceeds() {
        register("aaaaaaaaaaaaaaaa");
        assertThrows(IllegalArgumentException.class,
                () -> ReportSink.recordOutcome("aaaaaaaaaaaaaaaa", "killed", 10L, null));
        ReportSink.recordOutcome("aaaaaaaaaaaaaaaa", "KILLED", 10L, "assertion failed");
        assertEquals(1, ReportSink.snapshotResults().size());
    }

    @Test
    void recordingSameMutantTwiceThrowsIllegalState() {
        register("aaaaaaaaaaaaaaaa");
        ReportSink.recordOutcome("aaaaaaaaaaaaaaaa", "SURVIVED", 10L, null);
        assertThrows(IllegalStateException.class,
                () -> ReportSink.recordOutcome("aaaaaaaaaaaaaaaa", "SURVIVED", 10L, null));
    }

    @Test
    void unrecordedMutantIsSynthesizedAsErroredInJson() throws Exception {
        register("aaaaaaaaaaaaaaaa");
        System.setProperty(OUTPUT_DIR_PROPERTY, tempDir.resolve("out").toString());

        ReportSink.finalizeAndWriteReports();

        JsonNode root = mapper.readTree(tempDir.resolve("out").resolve("mutation-report.json").toFile());
        JsonNode mutant = root.get("mutants").get(0);
        assertEquals("aaaaaaaaaaaaaaaa", mutant.get("mutantId").asText());
        assertEquals("ERRORED", mutant.get("result").get("status").asText());
        assertEquals(
                "no outcome recorded; run terminated early",
                mutant.get("result").get("failureDetailOrNull").asText());
    }

    @Test
    void finalizeReturnsExistingAbsolutePathAndWritesAllThreeArtifacts() throws Exception {
        register("aaaaaaaaaaaaaaaa");
        System.setProperty(OUTPUT_DIR_PROPERTY, tempDir.toString());

        String reportPath = ReportSink.finalizeAndWriteReports();

        Path jsonPath = Path.of(reportPath);
        assertTrue(jsonPath.isAbsolute(), "expected an absolute path, got: " + reportPath);
        assertTrue(Files.exists(jsonPath), "mutation-report.json must exist on disk");
        assertTrue(Files.exists(tempDir.resolve("mutation-report.sarif")),
                "mutation-report.sarif must exist in the output directory");
        assertTrue(Files.exists(tempDir.resolve("mutation-report.html")),
                "mutation-report.html must exist in the output directory");
    }
}
