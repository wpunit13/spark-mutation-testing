package io.github.wpunit13.mutator.catalog;

import io.github.wpunit13.mutator.TestContextTracker;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    /** Discovery-time identity of a candidate's plan node (see the sink javadoc). */
    private record SiteHint(String nodeClass, Set<String> referencedColumns,
                            Set<String> exprClasses, Set<String> exprSigSet) {
    }

    /** One optimizer-phase observation of a live node (see the sink javadoc). */
    private record OptimizerObservation(
            String nodeClass, Set<String> schemaFieldNames, Set<Integer> offeredMutationIndexes,
            boolean insertedNullGuardOnly, Set<String> exprSigSet) {
    }

    /** Re-anchor identity recorded at discovery (see the sink javadoc). */
    record ReAnchorKey(String shapeFreeKey, String deepKey) {
    }

    private static final class Holder {
        static final InMemoryMutationCatalog INSTANCE = new InMemoryMutationCatalog();
    }

    private final Map<String, MutantMetadata> catalog = new ConcurrentHashMap<>();
    private final Map<String, SiteHint> siteHints = new ConcurrentHashMap<>();
    private final Map<String, ReAnchorKey> reAnchorKeys = new ConcurrentHashMap<>();
    private final Set<OptimizerObservation> optimizerObservations = ConcurrentHashMap.newKeySet();

    /**
     * Set once the first mutant activation is observed in this JVM. The
     * harness clears the active mutant between test methods, so the Catalyst
     * rule sees transient IDLE windows during mutant re-runs; discovery and
     * optimizer observation must not fire there — they would catalogue and
     * observe nodes from MUTATED plans, polluting the viability filter's
     * observation set (a mutated plan's Filter would make phantom filter
     * hints look viable). Reset only with the catalog (resetForDiscovery).
     */
    private volatile boolean mutationLoopStarted = false;

    private volatile int lastLoggedDropCount = -1;

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

    @Override
    public void recordSiteHint(String mutantId, String nodeClass,
                               Set<String> referencedColumns, Set<String> exprClasses,
                               Set<String> exprSigSet) {
        siteHints.putIfAbsent(mutantId, new SiteHint(
                nodeClass, Set.copyOf(referencedColumns), Set.copyOf(exprClasses),
                Set.copyOf(exprSigSet)));
    }

    @Override
    public void recordReAnchorKey(String mutantId, String shapeFreeKey, String deepKey) {
        reAnchorKeys.putIfAbsent(mutantId, new ReAnchorKey(shapeFreeKey, deepKey));
    }

    /** The discovery-time re-anchor keys for {@code mutantId}, or null. */
    ReAnchorKey reAnchorKeyOrNull(String mutantId) {
        return reAnchorKeys.get(mutantId);
    }

    @Override
    public void recordOptimizerObservation(
            String nodeClass, Set<String> schemaFieldNames,
            Set<Integer> offeredMutationIndexes, boolean insertedNullGuardOnly,
            Set<String> exprSigSet) {
        optimizerObservations.add(new OptimizerObservation(
                nodeClass, Set.copyOf(schemaFieldNames), Set.copyOf(offeredMutationIndexes),
                insertedNullGuardOnly, Set.copyOf(exprSigSet)));
    }

    /** Marks the mutation loop as running; discovery/observation freeze. */
    void markMutationLoopStarted() {
        mutationLoopStarted = true;
    }

    boolean isMutationLoopStarted() {
        return mutationLoopStarted;
    }

    /** Returns an unmodifiable snapshot of every catalogued mutant. */
    Collection<MutantMetadata> allEntries() {
        List<MutantMetadata> entries = List.copyOf(catalog.values());
        if (Boolean.getBoolean("spark.mutator.viabilityFilter.disabled")
                || siteHints.isEmpty() || optimizerObservations.isEmpty()) {
            return entries;
        }
        List<MutantMetadata> viable = new ArrayList<>(entries.size());
        int dropped = 0;
        for (MutantMetadata entry : entries) {
            if (isViable(entry)) {
                viable.add(entry);
            } else {
                dropped++;
            }
        }
        if (dropped > 0 && dropped != lastLoggedDropCount) {
            lastLoggedDropCount = dropped;
            System.out.println("[spark-mutator] viability filter dropped " + dropped
                    + " of " + entries.size() + " discovered mutant(s): their plan sites are "
                    + "absent from every optimized plan (pushed into scans / collapsed by "
                    + "the optimizer) and could only ever report NOT_APPLIED");
        }
        return List.copyOf(viable);
    }

    /**
     * Viability = the Optimizer-phase matcher could find at least one node:
     * some observed node of the hint's class offers the candidate's mutation
     * index, is not a pure inserted-null-guard (the fallback refuses those),
     * and — the site-identity check — correlates with the hint's expression
     * fingerprint set. Entries without a hint (loaded from catalog.json in
     * mutant forks, or registered outside discovery) are always kept.
     *
     * The correlation check is COMPUTED-SIG-AWARE. A sig that is exactly a
     * positional attribute placeholder ("#<ordinal>", the shim's rendering of
     * a pure pass-through) collides across every same-shape node built over
     * the same source columns, so it proves nothing about site identity:
     *
     *  - Hint with computed sigs (aliases, functions, literals — the site's
     *    actual fingerprint): an observation must contain at least one of
     *    THOSE. Without this, a site the optimizer eliminated entirely (e.g.
     *    ConvertToLocalRelation folding a withColumn Project into its
     *    LocalRelation) stays "viable" because impostor pass-through Projects
     *    share its trivial sigs — measured: 73 PROJECT mutants discovered on
     *    construction-time intermediate plans ran the full loop only to report
     *    NOT_APPLIED (the fallback then correctly refuses the impostors as
     *    ambiguous).
     *  - ALL-trivial hint (a pure pass-through site): the shape-free key is
     *    just the ordinal sequence "#0;...;#k", so the Optimizer-phase primary
     *    match can only ever hit a node whose fingerprint set EQUALS the
     *    hint's, and the fallback cannot disambiguate impostors (they render
     *    identical trivial sigs — measured: 5 candidates all passing every
     *    identity criterion, refused as ambiguous). An exact-set observation
     *    is therefore the only honest viability signal; anything else can
     *    only ever report NOT_APPLIED and is dropped.
     */
    private boolean isViable(MutantMetadata entry) {
        SiteHint hint = siteHints.get(entry.getMutantId());
        if (hint == null) {
            return true;
        }
        Set<String> computedHintSigs = new HashSet<>();
        for (String sig : hint.exprSigSet()) {
            if (!sig.matches("#\\d+")) {
                computedHintSigs.add(sig);
            }
        }
        for (OptimizerObservation obs : optimizerObservations) {
            if (obs.nodeClass().equals(hint.nodeClass())
                    && obs.offeredMutationIndexes().contains(entry.getMutationIndex())
                    && !obs.insertedNullGuardOnly()
                    && !obs.exprSigSet().isEmpty()
                    && sigSetsCorrelate(obs.exprSigSet(), hint.exprSigSet(), computedHintSigs)) {
                return true;
            }
        }
        return false;
    }

    /** True when the observation's sig set correlates with the hint's: at
      * least one computed (non-pass-through) sig shared when the hint has
      * computed sigs; otherwise (all-trivial hint) the observation's set must
      * EQUAL the hint's — see the isViable javadoc for why intersection is
      * not enough. */
    private static boolean sigSetsCorrelate(Set<String> obsSigs, Set<String> hintSigs,
                                            Set<String> computedHintSigs) {
        if (computedHintSigs.isEmpty()) {
            return obsSigs.equals(hintSigs);
        }
        for (String sig : obsSigs) {
            if (computedHintSigs.contains(sig)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Replaces the entry's {@code astDiffSnippet} — the in-process half of
     * WP-19 plan-diff capture (the fork-side half is the {@code diffs/}
     * sidecar written by {@code DiffSnippetStore}). Mirrors the
     * {@code mappedTestIds} merge pattern: {@code MutantMetadata} is
     * immutable, so the update produces a replacement object under a single
     * atomic map operation.
     *
     * @throws IllegalArgumentException if mutantId is not present in the
     *         catalog (fail loudly — same contract as
     *         {@code ReportSink.recordOutcome}).
     */
    void recordAstDiffSnippet(String mutantId, String astDiffSnippet) {
        if (catalog.get(mutantId) == null) {
            throw new IllegalArgumentException(
                    "Unknown mutantId '" + mutantId + "': not present in the catalog.");
        }
        catalog.computeIfPresent(mutantId, (key, existing) -> new MutantMetadata(
                existing.getMutantId(),
                existing.getFilePath(),
                existing.getLineNumber(),
                existing.getOperatorType(),
                existing.getMutationIndex(),
                existing.getDescription(),
                existing.getCoordinateHex(),
                astDiffSnippet,
                existing.getMappedTestIds()));
    }

    /**
     * Restores externally-supplied entries — the fork-side half of the
     * cross-process catalog handoff. A mutant fork never runs Discovery (that
     * happened in the baseline fork, a different JVM), so the bridge loads
     * {@code catalog.json} into this store before the Catalyst rule needs it.
     * Existing entries are never overwritten.
     */
    void loadAll(Collection<MutantMetadata> entries) {
        for (MutantMetadata entry : entries) {
            catalog.putIfAbsent(entry.getMutantId(), entry);
        }
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
        siteHints.clear();
        reAnchorKeys.clear();
        optimizerObservations.clear();
        mutationLoopStarted = false;
        lastLoggedDropCount = -1;
    }
}
