package io.github.wpunit13.mutator;

import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Thread-safe singleton state machine tracking the currently active mutant.
 * Exactly one instance exists per JVM process (per driver).
 *
 * <p>State: IDLE (reference is null) or ACTIVE(id). Transitions are
 * compareAndSet-based and linearizable; no method ever blocks on anything
 * other than the in-memory reference, so the control-channel watchdog can
 * call {@link #getActiveMutantOrNull()} concurrently with an in-flight
 * execution-channel Spark job.
 */
public final class MutantRegistry {

    private static final Pattern MUTANT_ID_PATTERN = Pattern.compile("^[0-9a-f]{16}$");

    private static final class Holder {
        static final MutantRegistry INSTANCE = new MutantRegistry();
    }

    private final AtomicReference<String> state = new AtomicReference<>(null);

    private MutantRegistry() {
    }

    /** Returns the single JVM-wide instance. Thread-safe, idempotent. */
    public static MutantRegistry getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Transitions IDLE -&gt; ACTIVE(mutantId).
     *
     * @param mutantId non-null, non-blank, matching the MutantID format
     *        (16 lowercase hex chars).
     * @throws IllegalArgumentException if mutantId is null, blank, or fails
     *         the format check.
     * @throws IllegalStateException if the registry is already ACTIVE for a
     *         different mutant id (i.e. the previous mutant's reset sequence
     *         did not run to completion).
     */
    public void setActiveMutant(String mutantId) {
        if (mutantId == null || mutantId.isBlank() || !MUTANT_ID_PATTERN.matcher(mutantId).matches()) {
            throw new IllegalArgumentException(
                    "mutantId must be a non-null, non-blank 16-char lowercase hex string, got: "
                            + mutantId);
        }
        if (!state.compareAndSet(null, mutantId)) {
            throw new IllegalStateException(
                    "Registry is already ACTIVE for mutant id '" + state.get()
                            + "'; cannot activate '" + mutantId + "'. The previous mutant's "
                            + "reset sequence did not run to completion.");
        }
    }

    /**
     * Transitions ACTIVE(mutantId) -&gt; IDLE. Must be the last step of the
     * reset sequence.
     *
     * <p>Validation contract: a null, blank, or malformed id is rejected
     * with {@link IllegalArgumentException} before any state transition is
     * attempted (mirroring {@link #setActiveMutant(String)}); a well-formed
     * id that does not match the current state is rejected with
     * {@link IllegalStateException}.
     *
     * @param mutantId the id the caller expects to be currently active.
     * @throws IllegalArgumentException if mutantId is null, blank, or fails
     *         the format check.
     * @throws IllegalStateException if the registry is IDLE, or if the
     *         currently active id does not equal {@code mutantId} (guards
     *         against clearing the wrong mutant due to a race or ordering
     *         bug — this is deliberately strict, not idempotent-on-mismatch).
     */
    public void clearActiveMutant(String mutantId) {
        if (mutantId == null || mutantId.isBlank() || !MUTANT_ID_PATTERN.matcher(mutantId).matches()) {
            throw new IllegalArgumentException(
                    "mutantId must be a non-null, non-blank 16-char lowercase hex string, got: "
                            + mutantId);
        }
        while (true) {
            String current = state.get();
            if (current == null) {
                throw new IllegalStateException(
                        "Registry is IDLE; cannot clear mutant id '" + mutantId + "'.");
            }
            if (!current.equals(mutantId)) {
                throw new IllegalStateException(
                        "Expected to clear mutant id '" + mutantId + "' but registry is ACTIVE for '"
                                + current + "'.");
            }
            if (state.compareAndSet(current, null)) {
                break;
            }
        }
    }

    /**
     * Non-blocking read of current state.
     *
     * @return the active mutant id, or {@code null} if IDLE.
     */
    public String getActiveMutantOrNull() {
        return state.get();
    }

    /**
     * Unconditional forced reset to IDLE regardless of current state. Used
     * exclusively by the driver-resilience circuit breaker, never by the
     * normal mutation loop. Idempotent.
     */
    public void reset() {
        state.set(null);
    }
}
