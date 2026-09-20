package io.github.wpunit13.mutator.report;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HtmlReportWriterTest {

    @TempDir
    Path tempDir;

    private MutantMetadata metadata(List<String> mappedTestIds) {
        return new MutantMetadata(
                "0000000000000001",
                "unknown",
                -1,
                OperatorTypeDto.FILTER,
                3,
                "FILTER -> NOT(predicate)",
                "72661923281222c8",
                "- Filter (a AND b)\n+ Filter NOT (a AND b)",
                mappedTestIds);
    }

    private String writeHtml(MutantMetadata meta) throws Exception {
        Map<String, MutantResult> results = Map.of(
                meta.getMutantId(),
                new MutantResult(meta.getMutantId(), MutantStatus.SURVIVED, 100L, null, 1732000000123L));
        Path out = HtmlReportWriter.write(tempDir, List.of(meta), results);
        return Files.readString(out);
    }

    @Test
    void mappedTestIdsAreListedNotJustCounted() throws Exception {
        String html = writeHtml(metadata(List.of("tests/test_orders_weak.py::test_build_report_count_only")));

        // The count summary...
        assertTrue(html.contains("<summary>1</summary>"), () -> html);
        // ...and the actual test id a developer can open in an editor.
        assertTrue(html.contains("tests/test_orders_weak.py::test_build_report_count_only"), () -> html);
    }

    @Test
    void emptyMappedTestListRendersCountOnly() throws Exception {
        String html = writeHtml(metadata(List.of()));

        assertTrue(html.contains("<td>0</td>"), () -> html);
        assertFalse(html.contains("<summary>0</summary>"), () -> html);
    }

    @Test
    void unknownSourceLocationRendersAnHonestPlaceholder() throws Exception {
        String html = writeHtml(metadata(List.of("tests/test_orders_weak.py::test_build_report_count_only")));

        // "unknown:-1" reads like a real location; an em dash reads as "none".
        assertFalse(html.contains("unknown:-1"), () -> html);
        assertTrue(html.contains("<code>\u2014</code>"), () -> html);
    }
}
