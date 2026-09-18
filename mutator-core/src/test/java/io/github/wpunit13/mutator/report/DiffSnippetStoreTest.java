package io.github.wpunit13.mutator.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * WP-19: the {@code diffs/<mutantId>.json} sidecar contract — a structural
 * mirror of {@link AppliedMarkerStore} for the plan-diff snippet. Missing
 * sidecars are legal (the snippet is optional); malformed or mis-filed ones
 * fail loudly.
 */
class DiffSnippetStoreTest {

    private static final String MUTANT_ID = "abcdef0123456789";

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void writeCreatesSidecarWithMutantIdAndSnippet() throws Exception {
        DiffSnippetStore.write(tempDir, MUTANT_ID, "'Join Inner' => 'Join Cross'");

        Path file = tempDir.resolve("diffs").resolve(MUTANT_ID + ".json");
        assertTrue(Files.isRegularFile(file), "sidecar must exist under diffs/");

        JsonNode node = mapper.readTree(Files.readString(file));
        assertEquals(MUTANT_ID, node.get("mutantId").asText());
        assertEquals("'Join Inner' => 'Join Cross'", node.get("astDiffSnippet").asText());
    }

    @Test
    void readSnippetOrNullRoundTripsTheSnippet() throws Exception {
        assertNull(DiffSnippetStore.readSnippetOrNull(tempDir, MUTANT_ID),
                "a missing sidecar reads as null (absence is legal)");

        DiffSnippetStore.write(tempDir, MUTANT_ID, "before => after");
        assertEquals("before => after", DiffSnippetStore.readSnippetOrNull(tempDir, MUTANT_ID));
    }

    @Test
    void readSnippetOrNullIsPerMutant() throws Exception {
        DiffSnippetStore.write(tempDir, MUTANT_ID, "a => b");
        DiffSnippetStore.write(tempDir, "1111111111111111", "c => d");

        assertEquals("a => b", DiffSnippetStore.readSnippetOrNull(tempDir, MUTANT_ID));
        assertEquals("c => d", DiffSnippetStore.readSnippetOrNull(tempDir, "1111111111111111"));
        assertNull(DiffSnippetStore.readSnippetOrNull(tempDir, "2222222222222222"));
    }

    @Test
    void readSnippetOrNullFailsLoudlyOnMismatchedMutantId() throws Exception {
        DiffSnippetStore.write(tempDir, MUTANT_ID, "a => b");

        // Mis-file the sidecar: the FILE is named after one mutant but its
        // content records another. That is a contract violation, never
        // silently accepted.
        Path misfiled = DiffSnippetStore.diffsDir(tempDir).resolve("ffffffffffffffff.json");
        Files.move(DiffSnippetStore.diffsDir(tempDir).resolve(MUTANT_ID + ".json"), misfiled);

        assertThrows(IllegalStateException.class,
                () -> DiffSnippetStore.readSnippetOrNull(tempDir, "ffffffffffffffff"));
    }

    @Test
    void readSnippetOrNullFailsLoudlyOnMalformedJson() throws Exception {
        Path dir = DiffSnippetStore.diffsDir(tempDir);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(MUTANT_ID + ".json"), "{not json");

        assertThrows(IOException.class,
                () -> DiffSnippetStore.readSnippetOrNull(tempDir, MUTANT_ID));
    }

    @Test
    void writeOverwritesAnExistingSidecar() throws Exception {
        DiffSnippetStore.write(tempDir, MUTANT_ID, "old => snippet");
        DiffSnippetStore.write(tempDir, MUTANT_ID, "new => snippet");

        assertEquals("new => snippet", DiffSnippetStore.readSnippetOrNull(tempDir, MUTANT_ID));
    }

    @Test
    void missingOutputDirectoryReadsAsNull() throws IOException {
        assertNull(DiffSnippetStore.readSnippetOrNull(tempDir.resolve("does-not-exist"), MUTANT_ID));
    }
}
