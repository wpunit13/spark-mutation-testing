package io.github.wpunit13.mutator.model;

/**
 * Outcome classification for an evaluated mutant.
 *
 * <p>SKIPPED is used by the pre-flight cardinality gate and is explicitly
 * excluded from the Mutation Score denominator.
 */
public enum MutantStatus {
    KILLED,
    SURVIVED,
    TIMED_OUT,
    ERRORED,

    /**
     * The mutation never executed: the rewrite did not land on any plan node
     * that reached execution (WP-24). Distinct from {@link #ERRORED} — an
     * ERRORED means the harness or engine misbehaved (dead session, shim
     * violation, mutation crash); a NOT_APPLIED is a designed, shape-dependent
     * outcome (e.g. nodes hidden inside an {@code InMemoryRelation} once the
     * cache substitutes them out). Excluded from the mutation score and from
     * the {@code maxErroredCount} gate; governed by
     * {@code spark.mutator.maxNotAppliedRatio} instead.
     */
    NOT_APPLIED,

    SKIPPED
}
