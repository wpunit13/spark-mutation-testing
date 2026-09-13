package io.github.wpunit13.mutator;

import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Records whether the active mutant's rewrite was actually applied to a plan
 * node, as opposed to the rule matching no node (a residual coordinate
 * no-match). This is the honesty guard that keeps a mutation that never
 * executed from being reported as SURVIVED: the harness reads
 * {@link #lastOrNull()} at fork teardown and refuses to classify a mutant as
 * KILLED or SURVIVED unless the recorded id matches the active mutant.
 *
 * <p>Lives in mutator-core (not in the Scala interceptor) so both the Scala
 * {@code CatalystMutationRule} — the only writer — and the Java harnesses
 * (JUnit 5 bridge, standalone loop) can read it without new module
 * dependencies.
 *
 * <p>Thread safety mirrors {@link MutantRegistry}: a single
 * {@link AtomicReference}. The write happens on the thread running the
 * Catalyst rule; the read happens at harness teardown.
 */
public final class AppliedMutantTracker {

    private static final Pattern MUTANT_ID_PATTERN = Pattern.compile("^[0-9a-f]{16}$");

    private static final AtomicReference<String> LAST = new AtomicReference<>(null);

    private AppliedMutantTracker() {
    }

    /**
     * Records {@code mutantId} as the most recently applied mutant. Called by
     * the Catalyst rule immediately after a rewrite is tagged as applied —
     * never on a no-match.
     *
     * @throws IllegalArgumentException if {@code mutantId} is null or not a
     *         16-char lowercase hex string (the MutantID format).
     */
    public static void record(String mutantId) {
        if (mutantId == null || !MUTANT_ID_PATTERN.matcher(mutantId).matches()) {
            throw new IllegalArgumentException(
                    "mutantId must be a non-null 16-char lowercase hex string, got: " + mutantId);
        }
        LAST.set(mutantId);
    }

    /**
     * @return the most recently applied mutant id, or {@code null} until a
     *         rewrite was applied.
     */
    public static String lastOrNull() {
        return LAST.get();
    }

    /** Test/loop reset: forgets the recorded mutant. */
    public static void clear() {
        LAST.set(null);
    }
}