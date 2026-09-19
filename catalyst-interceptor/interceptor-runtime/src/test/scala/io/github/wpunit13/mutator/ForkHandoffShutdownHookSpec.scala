package io.github.wpunit13.mutator

import io.github.wpunit13.mutator.catalog.{MutationCatalogAccess, MutationCatalogIo}
import io.github.wpunit13.mutator.model.{MutantMetadata, OperatorTypeDto}
import io.github.wpunit13.mutator.report.AppliedMarkerStore

import java.nio.file.{Files, Path}
import java.util.List
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ForkHandoffShutdownHookSpec extends AnyFunSuite with BeforeAndAfter {

  private val MUTANT_A = "0123456789abcdef"

  private def meta(id: String): MutantMetadata = new MutantMetadata(
    id, "src/main/java/pipeline/OrdersPipeline.java", 42, OperatorTypeDto.JOIN,
    2, "INNER -> ANTI", "coord-" + id, "ast-diff", List.of[String]())

  private def setProps(phase: String, outputDir: Path, activeMutant: String): Unit = {
    if (phase != null) System.setProperty(MutantBootstrap.PROP_PHASE, phase)
    System.setProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY, outputDir.toAbsolutePath.toString)
    if (activeMutant != null) System.setProperty(MutantBootstrap.PROP_ACTIVE_MUTANT, activeMutant)
  }

  private def clearProps(): Unit = {
    System.clearProperty(MutantBootstrap.PROP_PHASE)
    System.clearProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY)
    System.clearProperty(MutantBootstrap.PROP_ACTIVE_MUTANT)
  }

  before {
    MutationCatalogAccess.clearForTesting()
    AppliedMutantTracker.clear()
  }

  after {
    clearProps()
    MutationCatalogAccess.clearForTesting()
    AppliedMutantTracker.clear()
  }

  test("baseline phase flush writes catalog.json with the discovered entries") {
    val dir = Files.createTempDirectory("fork-handoff-baseline-")
    MutationCatalogAccess.loadCatalog(List.of(meta(MUTANT_A), meta("fedcba9876543210")))
    setProps(MutantBootstrap.PHASE_BASELINE, dir, null)

    ForkHandoffShutdownHook.flush()

    val catalog = MutationCatalogIo.readCatalogJson(dir)
    assert(catalog.size == 2, "both discovered entries must reach catalog.json")
    assert(catalog.get(0).getMutantId == MUTANT_A, "entries must be sorted by mutantId")
  }

  test("mutant phase flush writes the applied marker when the tracker matches") {
    val dir = Files.createTempDirectory("fork-handoff-mutant-")
    AppliedMutantTracker.record(MUTANT_A)
    setProps(MutantBootstrap.PHASE_MUTANT, dir, MUTANT_A)

    ForkHandoffShutdownHook.flush()

    assert(AppliedMarkerStore.exists(dir, MUTANT_A), "matching applied fact must persist the marker")
  }

  test("mutant phase flush with no applied fact writes no marker (NOT_APPLIED semantics)") {
    val dir = Files.createTempDirectory("fork-handoff-notapplied-")
    setProps(MutantBootstrap.PHASE_MUTANT, dir, MUTANT_A)

    ForkHandoffShutdownHook.flush()

    assert(!AppliedMarkerStore.exists(dir, MUTANT_A), "absent applied fact must stay absent")
  }

  test("mutant phase flush with a mismatched applied fact writes no marker") {
    val dir = Files.createTempDirectory("fork-handoff-mismatch-")
    AppliedMutantTracker.record("fedcba9876543210")
    setProps(MutantBootstrap.PHASE_MUTANT, dir, MUTANT_A)

    ForkHandoffShutdownHook.flush()

    assert(!AppliedMarkerStore.exists(dir, MUTANT_A))
    assert(!AppliedMarkerStore.exists(dir, "fedcba9876543210"))
  }

  test("flush without a fork phase is a no-op (standalone mode unchanged)") {
    val dir = Files.createTempDirectory("fork-handoff-standalone-")
    MutationCatalogAccess.loadCatalog(List.of(meta(MUTANT_A)))
    AppliedMutantTracker.record(MUTANT_A)
    System.setProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY, dir.toAbsolutePath.toString)

    ForkHandoffShutdownHook.flush()

    assert(!Files.exists(dir.resolve(MutationCatalogIo.CATALOG_FILE_NAME)))
    assert(!AppliedMarkerStore.exists(dir, MUTANT_A))
  }
}
