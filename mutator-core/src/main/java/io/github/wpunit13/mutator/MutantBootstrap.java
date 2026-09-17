package io.github.wpunit13.mutator;

import io.github.wpunit13.mutator.catalog.MutationCatalogAccess;
import io.github.wpunit13.mutator.catalog.MutationCatalogIo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Framework-agnostic bridge between the fork-launch system properties set by
 * the Maven orchestrator and the per-JVM {@link MutantRegistry}.
 *
 * <p>The Mojo tells a forked test JVM "which phase / which mutant is active"
 * exclusively through system properties (there is no shared memory across the
 * process boundary). This class is the single place that reads those
 * properties and applies them, so JUnit5, ScalaTest, TestNG — any harness —
 * get the same activation behavior by calling
 * {@link #activateFromSystemProperties()} once before running tests. Keeping
 * the logic here (instead of inside a specific test framework's hook) is what
 * makes the control channel framework-independent.
 */
public final class MutantBootstrap {

    public static final String PROP_PHASE = "spark.mutator.phase";
    public static final String PROP_ACTIVE_MUTANT = "spark.mutator.active.mutant";
    public static final String PROP_OUTPUT_DIRECTORY = "spark.mutator.outputDirectory";
    public static final String PROP_MIN_MUTATION_SCORE = "spark.mutator.minMutationScore";

    /** WP-24 governance-gate knobs (one key, both surfaces). */
    public static final String PROP_MAX_ERRORED_COUNT = "spark.mutator.maxErroredCount";
    public static final String PROP_MAX_NOT_APPLIED_RATIO = "spark.mutator.maxNotAppliedRatio";

    /** Magic value marking the externally-orchestrated baseline run. */
    public static final String PHASE_BASELINE = "baseline";
    /** Magic value marking an externally-orchestrated single-mutant run. */
    public static final String PHASE_MUTANT = "mutant";

    private MutantBootstrap() {
    }

    /**
     * Applies the active-mutant and phase system properties to the registry,
     * exactly once. Idempotent: a second call with the same active mutant is a
     * no-op (the registry only transitions from IDLE to ACTIVE).
     *
     * <p>When a mutant is activated, this JVM's in-memory catalog must know
     * it — the Catalyst rule resolves the mutant's coordinate through
     * {@code MutationCatalogAccess.findByIdOrNull} and silently no-ops on an
     * unknown id. A mutant fork never runs Discovery (that happened in the
     * baseline fork, a different JVM), so the catalog is loaded from
     * {@code catalog.json} in the configured output directory. Failing loudly
     * here (instead of letting the rule no-op) is what keeps a broken handoff
     * from masquerading as "every mutant survived".
     */
    public static void activateFromSystemProperties() {
        String active = System.getProperty(PROP_ACTIVE_MUTANT);
        if (active == null || active.isBlank()) {
            return;
        }
        if (MutantRegistry.getInstance().getActiveMutantOrNull() == null) {
            MutantRegistry.getInstance().setActiveMutant(active.trim());
        }
        ensureCatalogLoaded(active.trim());
    }

    private static void ensureCatalogLoaded(String activeMutantId) {
        if (MutationCatalogAccess.findByIdOrNull(activeMutantId) != null) {
            // Discovery already populated this JVM's catalog (baseline fork or
            // in-process mode), or the bridge loaded it on a previous call.
            return;
        }
        String dir = outputDirectoryOrNull();
        if (dir == null || dir.isBlank()) {
            throw new IllegalStateException(
                    "Active mutant '" + activeMutantId + "' is not catalogued in this JVM and no '"
                            + PROP_OUTPUT_DIRECTORY + "' system property is set to load catalog.json from.");
        }
        Path catalogFile = Path.of(dir, MutationCatalogIo.CATALOG_FILE_NAME);
        if (!Files.isRegularFile(catalogFile)) {
            throw new IllegalStateException(
                    "Active mutant '" + activeMutantId + "' is not catalogued in this JVM and "
                            + catalogFile + " does not exist. The baseline fork must run before "
                            + "any mutant fork.");
        }
        try {
            MutationCatalogAccess.loadCatalog(MutationCatalogIo.readCatalogJson(Path.of(dir)));
        } catch (IOException e) {
            throw new IllegalStateException("Could not load " + catalogFile, e);
        }
        if (MutationCatalogAccess.findByIdOrNull(activeMutantId) == null) {
            throw new IllegalStateException(
                    "Active mutant '" + activeMutantId + "' is not present in " + catalogFile + ".");
        }
    }

    /**
     * Returns the phase ({@code "baseline"}, {@code "mutant"}) if an external
     * orchestrator is driving this JVM, or {@code null} when running standalone
     * (e.g. a developer's IDE or a plain {@code mvn test} without the Mojo).
     */
    public static String phaseOrNull() {
        String phase = System.getProperty(PROP_PHASE);
        return (phase == null || phase.isBlank()) ? null : phase.trim();
    }

    /** Returns the configured output directory, or null when not set. */
    public static String outputDirectoryOrNull() {
        String dir = System.getProperty(PROP_OUTPUT_DIRECTORY);
        return (dir == null || dir.isBlank()) ? null : dir.trim();
    }
}
