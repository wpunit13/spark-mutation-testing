package io.github.wpunit13.mutator.report;

import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutcomeFileStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void roundTripsSingleOutcome() throws Exception {
        MutantResult killed = new MutantResult(
                "aaaaaaaaaaaaaaaa", MutantStatus.KILLED, 123L, "Expected [42] but found [0]", 1700000000000L);
        OutcomeFileStore.writeOutcome(tempDir, killed);

        Map<String, MutantResult> read = OutcomeFileStore.readOutcomes(tempDir);
        assertEquals(1, read.size());
        MutantResult result = read.get("aaaaaaaaaaaaaaaa");
        assertEquals(MutantStatus.KILLED, result.getStatus());
        assertEquals(123L, result.getElapsedMillis());
        assertEquals("Expected [42] but found [0]", result.getFailureDetailOrNull());
        assertEquals(1700000000000L, result.getRecordedAtEpochMillis());
    }

    @Test
    void mergesMultipleOutcomesIntoOneMap() throws Exception {
        OutcomeFileStore.writeOutcome(tempDir, new MutantResult(
                "aaaaaaaaaaaaaaaa", MutantStatus.SURVIVED, 10L, null, 1L));
        OutcomeFileStore.writeOutcome(tempDir, new MutantResult(
                "bbbbbbbbbbbbbbbb", MutantStatus.ERRORED, 20L, "boom", 2L));

        Map<String, MutantResult> read = OutcomeFileStore.readOutcomes(tempDir);
        assertEquals(2, read.size());
        assertEquals(MutantStatus.SURVIVED, read.get("aaaaaaaaaaaaaaaa").getStatus());
        assertEquals(MutantStatus.ERRORED, read.get("bbbbbbbbbbbbbbbb").getStatus());
    }

    @Test
    void missingOutcomesDirReadsAsEmptyMap() throws Exception {
        assertTrue(OutcomeFileStore.readOutcomes(tempDir).isEmpty());
    }

    @Test
    void malformedOutcomeFileThrows() throws Exception {
        Path dir = OutcomeFileStore.outcomesDir(tempDir);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("cccccccccccccccc.json"), "{ not json ");

        org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
                () -> OutcomeFileStore.readOutcomes(tempDir));
    }
}