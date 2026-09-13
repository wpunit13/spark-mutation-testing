package io.github.wpunit13.mutator.catalog;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MutationCatalogIoTest {

    @TempDir
    Path tempDir;

    private static MutantMetadata meta(String mutantId, String description, List<String> mappedTestIds) {
        return new MutantMetadata(
                mutantId,
                "src/main/scala/Pipeline.scala",
                42,
                OperatorTypeDto.AGGREGATE,
                0,
                description,
                "0123456789abcdef", // coordinateHex: valid 16-hex
                "sum -> max",
                mappedTestIds);
    }

    @Test
    void roundTripsFullMetadataIncludingAstDiffSnippetAndMappedTestIds() throws Exception {
        MutantMetadata a = meta("aaaaaaaaaaaaaaaa", "AGGREGATE -> swap function", List.of("TestA", "TestB"));
        MutantMetadata b = meta("bbbbbbbbbbbbbbbb", "AGGREGATE -> drop grouping key", List.of());

        MutationCatalogIo.writeCatalogJson(tempDir, List.of(b, a));

        List<MutantMetadata> read = MutationCatalogIo.readCatalogJson(tempDir);
        assertEquals(2, read.size());
        // Entries are ordered by mutantId, so 'a' (a...) precedes 'b' (b...).
        assertEquals("aaaaaaaaaaaaaaaa", read.get(0).getMutantId());
        assertEquals("bbbbbbbbbbbbbbbb", read.get(1).getMutantId());

        MutantMetadata first = read.get(0);
        assertEquals("src/main/scala/Pipeline.scala", first.getFilePath());
        assertEquals(42, first.getLineNumber());
        assertEquals(OperatorTypeDto.AGGREGATE, first.getOperatorType());
        assertEquals("AGGREGATE -> swap function", first.getDescription());
        assertEquals("0123456789abcdef", first.getCoordinateHex());
        assertEquals("sum -> max", first.getAstDiffSnippet());
        assertEquals(List.of("TestA", "TestB"), first.getMappedTestIds());
    }

    @Test
    void missingCatalogFileReadsAsEmptyList() throws Exception {
        assertTrue(MutationCatalogIo.readCatalogJson(tempDir).isEmpty());
    }
}