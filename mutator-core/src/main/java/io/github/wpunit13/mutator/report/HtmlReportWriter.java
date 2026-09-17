package io.github.wpunit13.mutator.report;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Writes a single self-contained HTML report: inline {@code <style>} only,
 * no JavaScript, no external assets, no CDN links.
 */
public final class HtmlReportWriter {

    static final String FILE_NAME = "mutation-report.html";

    private static final String SUMMARY_ITEM_CLOSE = "</span></div>";
    private static final String TABLE_CELL_CLOSE = "</td>";

    private HtmlReportWriter() {
    }

    static Path write(
            Path outputDir,
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results) throws IOException {
        List<MutantMetadata> sorted = catalog.stream()
                .sorted(Comparator.comparing(MutantMetadata::getMutantId))
                .toList();

        int killed = 0;
        int survived = 0;
        int timedOut = 0;
        int errored = 0;
        int notApplied = 0;
        int skipped = 0;
        for (MutantMetadata meta : sorted) {
            MutantResult result = results.get(meta.getMutantId());
            if (result == null) {
                errored++;
                continue;
            }
            switch (result.getStatus()) {
                case KILLED -> killed++;
                case SURVIVED -> survived++;
                case TIMED_OUT -> timedOut++;
                case ERRORED -> errored++;
                case NOT_APPLIED -> notApplied++;
                case SKIPPED -> skipped++;
            }
        }
        double score = JsonReportWriter.mutationScore(killed, timedOut, survived);

        StringBuilder html = new StringBuilder(16 * 1024);
        html.append("<!DOCTYPE html>\n")
                .append("<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n")
                .append("<title>spark-mutator report</title>\n")
                .append("<style>\n")
                .append("body { font-family: -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;"
                        + " margin: 2rem; color: #1f2328; }\n")
                .append("h1 { font-size: 1.4rem; }\n")
                .append(".summary { display: flex; gap: 1.5rem; margin: 1rem 0 1.5rem 0;"
                        + " padding: 1rem; background: #f6f8fa; border: 1px solid #d0d7de;"
                        + " border-radius: 6px; }\n")
                .append(".summary div { font-size: 0.95rem; }\n")
                .append(".summary span { display: block; font-size: 1.5rem; font-weight: 600; }\n")
                .append("table { border-collapse: collapse; width: 100%; font-size: 0.85rem; }\n")
                .append("th, td { border: 1px solid #d0d7de; padding: 0.4rem 0.6rem;"
                        + " text-align: left; vertical-align: top; }\n")
                .append("th { background: #f6f8fa; }\n")
                .append("tr.survived { background: #fff8c5; }\n")
                .append("tr.survived td.status { color: #9a6700; font-weight: 700; }\n")
                .append("code { font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;"
                        + " font-size: 0.8rem; }\n")
                .append("details.diff-cell { display: inline-block; }\n")
                .append("details.diff-cell summary { cursor: pointer; color: #0969da; }\n")
                .append("details.diff-cell pre { max-width: 60ch; white-space: pre-wrap; word-break: break-all;"
                        + " background: #f6f8fa; border: 1px solid #d0d7de; border-radius: 4px;"
                        + " padding: 0.4rem; margin: 0.3rem 0 0 0; font-size: 0.75rem; }\n")
                .append("</style>\n</head>\n<body>\n")
                .append("<h1>spark-mutator mutation report</h1>\n");

        html.append("<div class=\"summary\">")
                .append("<div>Total mutants<span>").append(sorted.size()).append(SUMMARY_ITEM_CLOSE)
                .append("<div>Mutation score<span>").append(score).append(SUMMARY_ITEM_CLOSE)
                .append("<div>Killed<span>").append(killed).append(SUMMARY_ITEM_CLOSE)
                .append("<div>Survived<span>").append(survived).append(SUMMARY_ITEM_CLOSE)
                .append("<div>Timed out<span>").append(timedOut).append(SUMMARY_ITEM_CLOSE)
                .append("<div>Errored<span>").append(errored).append(SUMMARY_ITEM_CLOSE)
                .append("<div>Not applied<span>").append(notApplied).append(SUMMARY_ITEM_CLOSE)
                .append("<div>Skipped<span>").append(skipped).append(SUMMARY_ITEM_CLOSE)
                .append("</div>\n");

        html.append("<table>\n<tr><th>Mutant ID</th><th>Operator</th><th>Description</th>")
                .append("<th>Status</th><th>Location</th><th>Mapped tests</th><th>Plan diff</th></tr>\n");
        for (MutantMetadata meta : sorted) {
            MutantResult result = results.get(meta.getMutantId());
            MutantStatus status = result == null ? MutantStatus.ERRORED : result.getStatus();
            String rowClass = status == MutantStatus.SURVIVED ? " class=\"survived\"" : "";
            String location = meta.getFilePath() + ":" + meta.getLineNumber();
            html.append("<tr").append(rowClass).append(">")
                    .append("<td><code>").append(escape(meta.getMutantId())).append("</code></td>")
                    .append("<td>").append(escape(meta.getOperatorType().name())).append(TABLE_CELL_CLOSE)
                    .append("<td>").append(escape(meta.getDescription())).append(TABLE_CELL_CLOSE)
                    .append("<td class=\"status\">").append(escape(status.name())).append(TABLE_CELL_CLOSE)
                    .append("<td><code>").append(escape(location)).append("</code></td>")
                    .append("<td>").append(meta.getMappedTestIds().size()).append(TABLE_CELL_CLOSE)
                    .append(diffCell(meta.getAstDiffSnippet()))
                    .append("</tr>\n");
        }
        html.append("</table>\n</body>\n</html>\n");

        Path target = outputDir.resolve(FILE_NAME);
        Files.createDirectories(outputDir);
        Files.writeString(target, html.toString(), StandardCharsets.UTF_8);
        return target;
    }

    /**
     * Expandable plan-diff cell (WP-19): the captured before/after plan
     * fragment inside a {@code <details>} element, all plan text escaped.
     * An empty snippet renders nothing for that row — the field is optional.
     */
    private static String diffCell(String astDiffSnippet) {
        if (astDiffSnippet == null || astDiffSnippet.isEmpty()) {
            return "<td>" + TABLE_CELL_CLOSE;
        }
        return "<td><details class=\"diff-cell\"><summary>diff</summary><pre>"
                + escape(astDiffSnippet)
                + "</pre></details>" + TABLE_CELL_CLOSE;
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}
