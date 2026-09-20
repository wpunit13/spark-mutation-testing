package io.github.wpunit13.mutator.report;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Canonical format for the per-mutant failing-test attribution appended to a
 * KILLED outcome's {@code failureDetailOrNull}:
 *
 * <pre>
 * &lt;original detail&gt;
 *
 * Failing tests:
 *   - com.example.FooTest.methodOne
 *   - com.example.FooTest.methodTwo
 * </pre>
 *
 * Both orchestration paths append in this exact shape (the Maven fork loop
 * from its surefire XML parse; the PySpark loop from its per-mutant re-run),
 * and this class is the single place that formats and parses it, so the
 * test-value report can attribute kills to tests regardless of which path
 * produced the outcomes.
 */
public final class FailingTests {

    /** Marker line introducing the failing-test list inside a detail string. */
    public static final String MARKER = "Failing tests:";

    private static final String ITEM_PREFIX = "  - ";

    private FailingTests() {
    }

    /**
     * Appends the failing-test block to {@code detail} (or creates it when
     * {@code detail} is null/blank). Duplicate ids are dropped, order kept.
     * Returns {@code detail} unchanged when {@code failedTestIds} is null or
     * empty.
     */
    public static String append(String detail, List<String> failedTestIds) {
        if (failedTestIds == null || failedTestIds.isEmpty()) {
            return detail;
        }
        String base = detail == null || detail.isBlank() ? "" : detail + "\n\n";
        StringBuilder sb = new StringBuilder(base).append(MARKER).append('\n');
        for (String id : failedTestIds) {
            sb.append(ITEM_PREFIX).append(id).append('\n');
        }
        // Drop the trailing newline; details are single logical strings.
        return sb.substring(0, sb.length() - 1);
    }

    /**
     * Extracts the failing-test ids from a detail string. Returns an empty
     * list when the detail is null, has no marker, or lists nothing — callers
     * treat that as "no per-test attribution recorded".
     */
    public static List<String> parse(String failureDetail) {
        if (failureDetail == null) {
            return List.of();
        }
        int marker = failureDetail.indexOf(MARKER);
        if (marker < 0) {
            return List.of();
        }
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        for (String line : failureDetail.substring(marker + MARKER.length()).split("\n", -1)) {
            String trimmed = line.trim();
            if (trimmed.startsWith(ITEM_PREFIX.trim() + " ")) {
                String id = trimmed.substring(ITEM_PREFIX.trim().length()).trim();
                if (!id.isEmpty()) {
                    ids.add(id);
                }
            } else if (!ids.isEmpty() && !trimmed.isEmpty()) {
                // A non-item line after the list started ends the block.
                break;
            }
        }
        return List.copyOf(new ArrayList<>(ids));
    }
}
