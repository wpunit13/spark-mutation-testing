package io.github.wpunit13.mutator

import io.github.wpunit13.mutator.catalog.{MutationCatalogAccess, MutationCatalogIo}
import io.github.wpunit13.mutator.report.AppliedMarkerStore

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

/**
 * WP-26 zero-touch fork handoff: performs the developer-guide §7 harness
 * obligations at JVM exit so the Maven plugin path needs no test-code anchor
 * (no @EnableSparkMutationTesting, no shared base class).
 *
 * Registered from [[MutatorSparkExtension.apply]] — the same config-activated
 * entry point SurefireConfigurator injects via spark.sql.extensions — so a
 * fork driven purely by the Mojo's system properties gets the full handoff:
 *
 *  - baseline phase: write catalog.json (Discovery is complete once every
 *    query has been analyzed; JVM exit is a safe flush point).
 *  - mutant phase: write the applied marker only if
 *    [[AppliedMutantTracker.lastOrNull]] equals the active mutant. Absence
 *    keeps the coordinator's NOT_APPLIED semantics (classify() checks the
 *    timeout before the marker, so a kill -9 hard timeout still classifies
 *    TIMED_OUT, never a false verdict).
 *
 * Idempotent with the JUnit 5 bridge: when an annotated class also runs in
 * the fork, both writers produce identical bytes (afterAll runs before JVM
 * exit), so the overwrite is harmless. Standalone in-process mode (no
 * spark.mutator.phase) is a no-op — the annotation still drives that loop.
 *
 * Write failures are reported on stderr but never throw from the hook: a
 * missing catalog.json reads as an empty catalog (visible empty report), and
 * a missing marker reads as NOT_APPLIED — neither can masquerade as "every
 * mutant survived".
 */
object ForkHandoffShutdownHook {

  private val registered = new AtomicBoolean(false)

  def register(): Unit = {
    if (registered.compareAndSet(false, true)) {
      Runtime.getRuntime.addShutdownHook(
        new Thread(null, () => flush(), "spark-mutator-fork-handoff"))
    }
  }

  /** Pure, testable core: one handoff write for the current fork phase. */
  def flush(): Unit = {
    val phase = MutantBootstrap.phaseOrNull()
    val outputDir = MutantBootstrap.outputDirectoryOrNull()
    if (phase == null || outputDir == null || outputDir.isBlank) {
      return
    }
    val dir = Path.of(outputDir.trim)
    try {
      if (phase == MutantBootstrap.PHASE_BASELINE) {
        MutationCatalogIo.writeCatalogJson(dir, MutationCatalogAccess.allEntries())
      } else if (phase == MutantBootstrap.PHASE_MUTANT) {
        val active = System.getProperty(MutantBootstrap.PROP_ACTIVE_MUTANT)
        val applied = AppliedMutantTracker.lastOrNull()
        if (active != null && active.trim.nonEmpty && active.trim == applied) {
          AppliedMarkerStore.write(dir, active.trim)
        }
      }
    } catch {
      case t: Throwable =>
        System.err.println(
          "[spark-mutator] fork handoff flush failed (phase=" + phase + "): " + t)
        t.printStackTrace()
    }
  }
}
