package io.github.wpunit13.mutator;

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
     */
    public static void activateFromSystemProperties() {
        String active = System.getProperty(PROP_ACTIVE_MUTANT);
        if (active == null || active.isBlank()) {
            return;
        }
        if (MutantRegistry.getInstance().getActiveMutantOrNull() == null) {
            MutantRegistry.getInstance().setActiveMutant(active.trim());
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