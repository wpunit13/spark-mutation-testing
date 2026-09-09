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
}
