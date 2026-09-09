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
    SKIPPED
}
