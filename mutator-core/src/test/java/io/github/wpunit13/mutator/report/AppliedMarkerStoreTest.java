package io.github.wpunit13.mutator.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppliedMarkerStoreTest {

    private static final String MUTANT_ID = "0123456789abcdef";

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void writeCreatesAppliedDirAndMarkerFileWithContractFields() throws Exception {
        AppliedMarkerStore.write(tempDir, MUTANT_ID);

        Path marker = AppliedMarkerStore.appliedDir(tempDir).resolve(MUTANT_ID + ".json");
        assertTrue(Files.isRegularFile(marker), "marker file must exist under applied/");

        JsonNode node = mapper.readTree(marker.toFile());
        assertEquals(MUTANT_ID, node.get("mutantId").asText());
        assertTrue(node.get("appliedAtEpochMillis").asLong() > 0L, "appliedAtEpochMillis must be set");
    }

    @Test
    void existsIsAPureCheckThatNeverCreatesAnything() {
        assertFalse(AppliedMarkerStore.exists(tempDir, MUTANT_ID));
        assertFalse(Files.exists(AppliedMarkerStore.appliedDir(tempDir)),
                "exists must not create the applied/ directory");
    }

    @Test
    void existsIsTrueOnlyForTheWrittenMutant() throws Exception {
        assertFalse(AppliedMarkerStore.exists(tempDir, MUTANT_ID));

        AppliedMarkerStore.write(tempDir, MUTANT_ID);

        assertTrue(AppliedMarkerStore.exists(tempDir, MUTANT_ID));
        assertFalse(AppliedMarkerStore.exists(tempDir, "ffffffffffffffff"),
                "a different mutant's marker must not read as present");
    }

    @Test
    void existsOnAMissingOutputDirectoryIsFalse() {
        assertFalse(AppliedMarkerStore.exists(tempDir.resolve("does-not-exist"), MUTANT_ID));
    }

    @Test
    void writeOverwritesAnExistingMarker() throws Exception {
        AppliedMarkerStore.write(tempDir, MUTANT_ID);
        AppliedMarkerStore.write(tempDir, MUTANT_ID);

        assertTrue(AppliedMarkerStore.exists(tempDir, MUTANT_ID));
        JsonNode node = mapper.readTree(
                AppliedMarkerStore.appliedDir(tempDir).resolve(MUTANT_ID + ".json").toFile());
        assertEquals(MUTANT_ID, node.get("mutantId").asText());
    }

    @Test
    void writePropagatesIoFailures() throws Exception {
        // A directory at the marker's path forces the file write to fail.
        Path markerPath = AppliedMarkerStore.appliedDir(tempDir).resolve(MUTANT_ID + ".json");
        Files.createDirectories(markerPath);

        org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
                () -> AppliedMarkerStore.write(tempDir, MUTANT_ID));
    }
}