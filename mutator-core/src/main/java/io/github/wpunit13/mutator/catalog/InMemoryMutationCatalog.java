package io.github.wpunit13.mutator.catalog;

import io.github.wpunit13.mutator.TestContextTracker;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JVM-wide singleton backing store for the mutant catalog.
 *
 * <p>Registration is idempotent-additive: the first test to exercise a given
 * NodeCoordinate creates the mutant's {@link MutantMetadata}; every
 * subsequent test that exercises the same coordinate only appends its test
 * id to the existing {@code mappedTestIds}. Because {@code MutantMetadata}
 * is immutable, the merge produces a replacement object — that is correct
 * and intended.
 */
final class InMemoryMutationCatalog implements MutationCatalogSink {

    private static final class Holder {
        static final InMemoryMutationCatalog INSTANCE = new InMemoryMutationCatalog();
    }

    private final Map<String, MutantMetadata> catalog = new ConcurrentHashMap<>();

    private InMemoryMutationCatalog() {
    }

    static InMemoryMutationCatalog getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Single atomic map operation — deliberately a
     * {@link ConcurrentHashMap#compute}, not a check-then-act sequence, so
     * concurrent registrations for the same mutantId cannot lose a test id.
     */
    @Override
    public void registerCandidate(
            String filePath,
            int lineNumber,
            OperatorTypeDto operatorType,
            int mutationIndex,
            String description,
            String coordinateHex,
            String mutantId) {
        String currentTestId = TestContextTracker.getCurrentTestIdOrNull();
        catalog.compute(mutantId, (key, existing) -> {
            if (existing == null) {
                List<String> seeded = currentTestId == null ? List.of() : List.of(currentTestId);
                // astDiffSnippet is "" — nothing computes it yet.
                return new MutantMetadata(
                        mutantId,
                        filePath,
                        lineNumber,
                        operatorType,
                        mutationIndex,
                        description,
                        coordinateHex,
                        "",
                        seeded);
            }
            if (currentTestId == null || existing.getMappedTestIds().contains(currentTestId)) {
                return existing;
            }
            List<String> appended = new ArrayList<>(existing.getMappedTestIds());
            appended.add(currentTestId);
            return new MutantMetadata(
                    existing.getMutantId(),
                    existing.getFilePath(),
                    existing.getLineNumber(),
                    existing.getOperatorType(),
                    existing.getMutationIndex(),
                    existing.getDescription(),
                    existing.getCoordinateHex(),
                    existing.getAstDiffSnippet(),
                    appended);
        });
    }

    /** Returns an unmodifiable snapshot of every catalogued mutant. */
    Collection<MutantMetadata> allEntries() {
        return List.copyOf(catalog.values());
    }

    MutantMetadata findByIdOrNull(String mutantId) {
        return catalog.get(mutantId);
    }

    /**
     * Test-only reset. Clears the entire catalog; used exclusively by unit
     * tests in {@code @BeforeEach} because this singleton is JVM-wide.
     */
    void clearForTesting() {
        catalog.clear();
    }
}
