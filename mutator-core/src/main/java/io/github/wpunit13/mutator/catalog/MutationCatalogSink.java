package io.github.wpunit13.mutator.catalog;

import io.github.wpunit13.mutator.model.OperatorTypeDto;

/**
 * Registration-side counterpart to the read-side methods of
 * {@link MutationCatalogAccess}. The Catalyst rule calls this once per
 * candidate mutant discovered during the Discovery &amp; Impact Mapping
 * phase.
 *
 * <p>Implementations must be safe to call concurrently (multiple queries
 * and multiple driver threads may register candidates simultaneously) and
 * must be idempotent-additive: registering the same {@code mutantId} again
 * must never create a duplicate entry, and must only append the current
 * test id (see {@code TestContextTracker.getCurrentTestIdOrNull()}) to the
 * existing {@code mappedTestIds} if it is not already present.
 */
public interface MutationCatalogSink {

    void registerCandidate(
            String filePath,
            int lineNumber,
            OperatorTypeDto operatorType,
            int mutationIndex,
            String description,
            String coordinateHex,
            String mutantId);

    /**
     * Records the discovery-time identity of the plan node a candidate came
     * from — the SAME three fields the Optimizer-phase identity fallback
     * matches on (node class, the node's expression-referenced column names,
     * and the expression-class simple names). The viability filter (see
     * {@code InMemoryMutationCatalog}) uses this to predict whether the
     * fallback could ever find a matching node; candidates whose site is
     * provably absent from every optimized plan (e.g. a Filter the optimizer
     * pushed wholly into a file scan, or an analyzer-inserted Project that
     * later batches collapse) are never offered to the loop — they could
     * only ever report NOT_APPLIED and poison the gate.
     */
    default void recordSiteHint(String mutantId, String nodeClass,
                                java.util.Set<String> referencedColumns,
                                java.util.Set<String> exprClasses,
                                java.util.Set<String> exprSigSet) {
    }

    /**
     * Records the discovery-time SHAPE-FREE key of the candidate's plan node:
     * the coordinate the node yields when classified as a plan root
     * (depth=0, ordinal=-1) — position-independent, so it is invariant across
     * the construction-vs-count-wrapped analyzed-plan shapes the fork-time
     * PostHoc match otherwise depends on. The PostHoc no-match path seeds the
     * pending rewrite from this key when the positional coordinate is absent
     * from the fork's plans (a match the fork could otherwise miss entirely —
     * measured: the same mutant flips KILLED/NOT_APPLIED across runs purely
     * on which shape reaches the match phase first).
     */
    default void recordShapeFreeKey(String mutantId, String shapeFreeKey) {
    }

    /**
     * Records one optimizer-walk observation. {@code walkId} identifies the
     * optimizer-batch invocation (one per Optimizer-phase rule application),
     * letting the viability filter replay the fallback's per-walk semantics:
     * a candidate is viable iff SOME walk contains a node that the fallback
     * would accept for it. The fallback accepts a node when its class equals
     * the recorded class, at least one of the recorded referenced columns is
     * present in the node's output schema, the node is not a pure
     * inserted-null-guard, and the node offers the candidate's mutation index.
     */
    default void recordOptimizerObservation(
            String nodeClass, java.util.Set<String> schemaFieldNames,
            java.util.Set<Integer> offeredMutationIndexes,
            boolean insertedNullGuardOnly, java.util.Set<String> exprSigSet) {
    }
}
