package io.github.wpunit13.mutator.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Writes the per-test value report ({@code test-value-report.json} /
 * {@code test-value-report.html}): for every test that ran against at least
 * one mutant, how many mutants it kills — and critically whether it is ever
 * the <em>sole</em> killer.
 *
 * <p>A test is {@code LOAD_BEARING} when at least one mutant exists whose
 * only failing test is this one: remove the test and that mutant escapes.
 * A test with zero sole kills is a {@code REDUNDANT_CANDIDATE}: everything
 * it catches, some other test in THIS run also catches. Redundancy is always
 * relative to the current suite — the report says so explicitly.
 *
 * <p>The computation needs per-mutant failing-test attribution, which paths
 * provide at different fidelity: the Maven fork loop captures ALL failing
 * tests per mutant (surefire XML), the PySpark loop captures all of them only
 * with {@code per_test_attribution = true} (otherwise fail-fast records the
 * first killer), and the JUnit 5 in-process loop records none. Where no
 * attribution exists the verdict is {@code INSUFFICIENT_DATA} — never a
 * misleading accusation.
 */
public final class TestValueReportWriter {

    static final String JSON_FILE_NAME = "test-value-report.json";
    static final String HTML_FILE_NAME = "test-value-report.html";

    static final String VERDICT_LOAD_BEARING = "LOAD_BEARING";
    static final String VERDICT_REDUNDANT_CANDIDATE = "REDUNDANT_CANDIDATE";
    static final String VERDICT_INSUFFICIENT_DATA = "INSUFFICIENT_DATA";

    private TestValueReportWriter() {
    }

    /** One test's kill attribution within this run. */
    public static final class TestValue {
        private final String testId;
        private final int mutantsRun;
        private final int soleKills;
        private final int sharedKills;
        private final String verdict;

        TestValue(String testId, int mutantsRun, int soleKills, int sharedKills, String verdict) {
            this.testId = testId;
            this.mutantsRun = mutantsRun;
            this.soleKills = soleKills;
            this.sharedKills = sharedKills;
            this.verdict = verdict;
        }

        public String getTestId() {
            return testId;
        }

        public int getMutantsRun() {
            return mutantsRun;
        }

        public int getSoleKills() {
            return soleKills;
        }

        public int getSharedKills() {
            return sharedKills;
        }

        public String getVerdict() {
            return verdict;
        }
    }

    /** Pure computation result, separated from I/O for testability. */
    public static final class TestValueReport {
        private final List<TestValue> tests;
        private final int killedTotal;
        private final int attributedKilled;

        TestValueReport(List<TestValue> tests, int killedTotal, int attributedKilled) {
            this.tests = tests;
            this.killedTotal = killedTotal;
            this.attributedKilled = attributedKilled;
        }

        public List<TestValue> getTests() {
            return tests;
        }

        public int getKilledTotal() {
            return killedTotal;
        }

        public int getAttributedKilled() {
            return attributedKilled;
        }

        /** Fraction of KILLED mutants whose failing tests were named (0..1). */
        public double attributionCoverage() {
            return killedTotal == 0 ? 0.0 : Math.round(attributedKilled / (double) killedTotal * 100.0) / 100.0;
        }
    }

    /** Pure attribution computation over catalog + outcomes. */
    static TestValueReport compute(
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results) {
        // failing tests per killed mutant, only where attribution was recorded
        Map<String, List<String>> failing = new LinkedHashMap<>();
        int killedTotal = 0;
        int attributedKilled = 0;
        for (MutantMetadata meta : catalog) {
            MutantResult result = results.get(meta.getMutantId());
            if (result == null || result.getStatus() != MutantStatus.KILLED) {
                continue;
            }
            killedTotal++;
            List<String> ids = FailingTests.parse(result.getFailureDetailOrNull());
            if (!ids.isEmpty()) {
                failing.put(meta.getMutantId(), ids);
                attributedKilled++;
            }
        }

        // testId → kill bookkeeping
        Map<String, int[]> kills = new TreeMap<>(); // [0]=total kills, [1]=sole kills
        Map<String, Integer> ran = new TreeMap<>();
        for (MutantMetadata meta : catalog) {
            for (String testId : meta.getMappedTestIds()) {
                ran.merge(testId, 1, Integer::sum);
            }
        }
        for (List<String> ids : failing.values()) {
            for (String testId : ids) {
                int[] counters = kills.computeIfAbsent(testId, k -> new int[2]);
                counters[0]++;
            }
            if (ids.size() == 1) {
                kills.get(ids.get(0))[1]++;
            }
        }

        boolean anyAttribution = attributedKilled > 0;
        List<TestValue> tests = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : ran.entrySet()) {
            String testId = entry.getKey();
            int[] counters = kills.getOrDefault(testId, new int[2]);
            String verdict;
            if (!anyAttribution) {
                verdict = VERDICT_INSUFFICIENT_DATA;
            } else if (counters[1] > 0) {
                verdict = VERDICT_LOAD_BEARING;
            } else {
                verdict = VERDICT_REDUNDANT_CANDIDATE;
            }
            tests.add(new TestValue(testId, entry.getValue(), counters[1], counters[0] - counters[1], verdict));
        }
        return new TestValueReport(tests, killedTotal, attributedKilled);
    }

    /**
     * Writes {@code test-value-report.json} and {@code test-value-report.html}
     * into {@code outputDir}.
     *
     * @return the absolute path of test-value-report.json
     */
    public static Path write(
            Path outputDir,
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results) throws IOException {
        TestValueReport report = compute(catalog, results);
        Files.createDirectories(outputDir);
        Path json = writeJson(outputDir, report);
        writeHtml(outputDir, report);
        return json;
    }

    private static Path writeJson(Path outputDir, TestValueReport report) throws IOException {
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("killedTotal", report.getKilledTotal());
        root.put("attributedKilled", report.getAttributedKilled());
        root.put("attributionCoverage", report.attributionCoverage());
        List<Map<String, Object>> tests = new ArrayList<>();
        for (TestValue t : report.getTests()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("testId", t.getTestId());
            row.put("mutantsRun", t.getMutantsRun());
            row.put("soleKills", t.getSoleKills());
            row.put("sharedKills", t.getSharedKills());
            row.put("verdict", t.getVerdict());
            tests.add(row);
        }
        root.put("tests", tests);
        Path target = outputDir.resolve(JSON_FILE_NAME);
        Files.writeString(target, mapper.writeValueAsString(root), StandardCharsets.UTF_8);
        return target;
    }

    private static Path writeHtml(Path outputDir, TestValueReport report) throws IOException {
        StringBuilder html = new StringBuilder(8 * 1024);
        html.append("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n")
                .append("<title>spark-mutator test value report</title>\n")
                .append("<style>\n")
                .append("body { font-family: -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;"
                        + " margin: 2rem; color: #1f2328; }\n")
                .append("h1 { font-size: 1.4rem; }\n")
                .append("p.note { color: #57606a; max-width: 80ch; }\n")
                .append("table { border-collapse: collapse; width: 100%; font-size: 0.85rem; }\n")
                .append("th, td { border: 1px solid #d0d7de; padding: 0.4rem 0.6rem; text-align: left; }\n")
                .append("th { background: #f6f8fa; }\n")
                .append("td.verdict { font-weight: 700; }\n")
                .append("tr.redundant td.verdict { color: #9a6700; }\n")
                .append("code { font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;"
                        + " font-size: 0.8rem; }\n")
                .append("</style>\n</head>\n<body>\n")
                .append("<h1>spark-mutator test value report</h1>\n")
                .append("<p class=\"note\">Verdicts are relative to THIS run's suite: ")
                .append("REDUNDANT_CANDIDATE means every mutant the test catches is also caught by ")
                .append("another test in the run — safe to drop only while the rest of the suite stays. ")
                .append("Verdicts require per-mutant failing-test attribution: enabled on the Maven fork ")
                .append("path, and on PySpark with per_test_attribution = true. ")
                .append(String.format("attributionCoverage = %.2f (%d of %d killed mutants).",
                        report.attributionCoverage(), report.getAttributedKilled(), report.getKilledTotal()))
                .append("</p>\n");
        if (report.attributionCoverage() == 0.0) {
            html.append("<p class=\"note\">No failing-test attribution was recorded for this run; ")
                    .append("per-test verdicts are unknown.</p>\n");
        }
        html.append("<table>\n<tr><th>Test</th><th>Mutants run</th><th>Sole kills</th>")
                .append("<th>Shared kills</th><th>Verdict</th></tr>\n");
        for (TestValue t : report.getTests()) {
            String rowClass = VERDICT_REDUNDANT_CANDIDATE.equals(t.getVerdict()) ? " class=\"redundant\"" : "";
            html.append("<tr").append(rowClass).append(">")
                    .append("<td><code>").append(escape(t.getTestId())).append("</code></td>")
                    .append("<td>").append(t.getMutantsRun()).append("</td>")
                    .append("<td>").append(t.getSoleKills()).append("</td>")
                    .append("<td>").append(t.getSharedKills()).append("</td>")
                    .append("<td class=\"verdict\">").append(escape(t.getVerdict())).append("</td>")
                    .append("</tr>\n");
        }
        html.append("</table>\n</body>\n</html>\n");
        Path target = outputDir.resolve(HTML_FILE_NAME);
        Files.writeString(target, html.toString(), StandardCharsets.UTF_8);
        return target;
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
