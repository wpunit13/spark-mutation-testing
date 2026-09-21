package io.github.wpunit13.mutator.model;

/**
 * Detects the failure signature of a mutation that was APPLIED but produced a
 * plan the query engine cannot execute: the mutated node no longer produces a
 * column that the rest of the plan references (e.g. INNER/LEFT &rarr; ANTI
 * drops the right side's columns while a downstream aggregate still selects
 * them; AGGREGATE &rarr; drop grouping key removes a column the projection
 * above still names). The optimizer had its chance to repair the plan and
 * could not, so the mutation is structurally invalid for this query — not a
 * harness/engine failure (ERRORED) and not a test kill.
 *
 * <p>Both harnesses (JUnit 5 in-process bridge, Maven fork coordinator)
 * reclassify such failures as SKIPPED: excluded from the mutation score and
 * from the WP-24 gate's not-applied denominator, but visible in the report
 * with the reason. A real engine/harness failure never matches these markers
 * (dead sessions, OOMs, NPEs produce different signatures).
 *
 * <p>Matching is textual because the fork path only sees the surefire XML's
 * rendered exception chain, never live Throwable objects. The markers are
 * Catalyst's own stable binding/resolution error shapes:
 * <ul>
 *   <li>{@code Couldn't find <attr>#<id> in [...]} — physical-plan binding
 *       (AttributeSeq) after a mutated node stopped producing a column;</li>
 *   <li>{@code key not found: <attr>#<id>} — Scala map lookup on an exprId
 *       the mutated plan no longer contains;</li>
 *   <li>AnalysisException / resolution wording — the mutated plan no longer
 *       resolves.</li>
 * </ul>
 */
public final class StructuralInvalidation {

    private static final java.util.regex.Pattern ROW_SHAPE_MISMATCH =
            java.util.regex.Pattern.compile("Index (\\d+) out of bounds for length \\1\\b");

    private StructuralInvalidation() {
    }

    /** True when the rendered failure chain matches a plan-invalidation marker. */
    public static boolean matches(String renderedFailureChain) {
        if (renderedFailureChain == null || renderedFailureChain.isEmpty()) {
            return false;
        }
        return renderedFailureChain.contains("AnalysisException")
                || renderedFailureChain.contains("UnresolvedException")
                || renderedFailureChain.contains("cannot resolve")
                || renderedFailureChain.contains("Cannot resolve column name")
                || renderedFailureChain.contains("Unresolved attribute")
                || (renderedFailureChain.contains("Couldn't find ")
                        && renderedFailureChain.contains(" in ["))
                || (renderedFailureChain.contains("key not found: ")
                        && renderedFailureChain.contains("#"))
                // Catalyst binding assertion after a mutated node stopped
                // producing a column: "index (2) should < 2" (AttributeSeq).
                || (renderedFailureChain.contains("index (")
                        && renderedFailureChain.contains(") should < "))
                // A row writer indexing exactly past the end of a shrunk row
                // (e.g. a dropped grouping key shrinking the aggregate's row
                // under a parquet write).
                || ROW_SHAPE_MISMATCH.matcher(renderedFailureChain).find();
    }

    /** Renders a Throwable's cause chain into the single string {@link
     * #matches(String)} consumes: one {@code ClassName: message} line per
     * cause, shallowest first. */
    public static String renderChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        Throwable current = t;
        int depth = 0;
        while (current != null && depth < 16) {
            sb.append(current.getClass().getName());
            if (current.getMessage() != null) {
                sb.append(": ").append(current.getMessage());
            }
            sb.append('\n');
            current = current.getCause() == current ? null : current.getCause();
            depth++;
        }
        return sb.toString();
    }
}
