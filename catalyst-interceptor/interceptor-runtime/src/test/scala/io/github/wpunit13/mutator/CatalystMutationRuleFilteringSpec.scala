package io.github.wpunit13.mutator

import io.github.wpunit13.mutator.catalog.MutationCatalogAccess
import io.github.wpunit13.mutator.dispatch.ShimDispatcher
import io.github.wpunit13.mutator.model.MutantMetadata
import io.github.wpunit13.mutator.model.OperatorTypeDto
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

/**
 * WP-19 specs for the engine-side config filters and plan-diff capture in
 * [[CatalystMutationRule]]:
 *
 *  - `spark.mutator.excludedMutators` keeps excluded operators out of the
 *    catalog AND refuses to rewrite them even when a stale catalog entry
 *    exists (fail-safe; the skip is observable, never an error).
 *  - `spark.mutator.targetModules` registers candidates only under a matching
 *    file-path hint; with the default "unknown" hint it registers NOTHING and
 *    warns once.
 *  - A rewrite captures a non-empty `astDiffSnippet` ("<before> => <after>",
 *    single-line, bounded) into the catalog, and the fork-side sidecar branch
 *    persists it under `<outputDir>/diffs/<mutantId>.json`.
 *
 * The directives are read once per rule invocation from system properties, so
 * each test sets/clears them around its own analyses.
 */
class CatalystMutationRuleFilteringSpec extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private var sidecarDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("CatalystMutationRuleFilteringSpec")
      .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  override def beforeEach(): Unit = {
    MutantRegistry.getInstance().reset()
    MutationCatalogAccess.clearForTesting()
    AppliedMutantTracker.clear()
    sidecarDir = java.nio.file.Files.createTempDirectory("spark-mutator-diffs")
  }

  override def afterEach(): Unit = {
    MutantRegistry.getInstance().reset()
    MutationCatalogAccess.clearForTesting()
    AppliedMutantTracker.clear()
    System.clearProperty(CatalystMutationRule.MutationPolicy.ExcludedMutatorsProp)
    System.clearProperty(CatalystMutationRule.MutationPolicy.TargetModulesProp)
    System.clearProperty(MutantBootstrap.PROP_PHASE)
    System.clearProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY)
    CatalystMutationRule.setCurrentFilePathHint(CatalystMutationRule.DefaultFilePathHint)
    if (sidecarDir != null) {
      scala.util.Using.resource(java.nio.file.Files.walk(sidecarDir)) { stream =>
        stream.iterator().asScala.toSeq
          .sortBy(_.getNameCount)(Ordering[Int].reverse)
          .foreach(p => java.nio.file.Files.deleteIfExists(p))
      }
      sidecarDir = null
    }
  }

  /** Two-table INNER join over local ranges; built from scratch on every call. */
  private def buildJoinQuery(): DataFrame = {
    val left = spark.range(0, 5, 1, 1).selectExpr("id AS lkey", "id AS lval")
    val right = spark.range(0, 5, 1, 1).selectExpr("id AS rkey", "id AS rval")
    left.join(right, left("lkey") === right("rkey"), "inner")
  }

  private def catalogEntries: Seq[MutantMetadata] =
    MutationCatalogAccess.allEntries().asScala.toSeq

  private def entriesOf(operatorType: OperatorTypeDto): Seq[MutantMetadata] =
    catalogEntries.filter(_.getOperatorType == operatorType)

  private def joinsIn(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Seq[org.apache.spark.sql.catalyst.plans.logical.Join] =
    plan.collect { case j: org.apache.spark.sql.catalyst.plans.logical.Join => j }

  // ---------------------------------------------------------------------------
  // excludedMutators
  // ---------------------------------------------------------------------------

  test("excludedMutators keeps excluded operators out of the catalog (case-insensitive)") {
    System.setProperty(CatalystMutationRule.MutationPolicy.ExcludedMutatorsProp, "join")

    buildJoinQuery().where("lval > 1").queryExecution.optimizedPlan

    assert(entriesOf(OperatorTypeDto.JOIN).isEmpty,
      "no JOIN candidate may be catalogued while JOIN is excluded")
    assert(entriesOf(OperatorTypeDto.FILTER).nonEmpty,
      "non-excluded operators must still be catalogued")
  }

  test("an excluded operator is refused at rewrite time even with a stale catalog entry") {
    // Populate the catalog WITHOUT exclusion, then exclude retroactively: the
    // fail-safe must refuse the rewrite even though the entry exists.
    buildJoinQuery().queryExecution.optimizedPlan
    val mutant = catalogEntries
      .find(e => e.getOperatorType == OperatorTypeDto.JOIN && e.getMutationIndex == 1)
      .getOrElse(fail("no catalogued JOIN idx-1 mutant"))

    System.setProperty(CatalystMutationRule.MutationPolicy.ExcludedMutatorsProp, "JOIN")
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    val plan = buildJoinQuery().queryExecution.optimizedPlan
    assert(joinsIn(plan).forall(_.joinType == org.apache.spark.sql.catalyst.plans.Inner),
      "an excluded mutant must never apply, even from a stale catalog entry")
    assert(AppliedMutantTracker.lastOrNull() == null,
      "a refused rewrite must not be recorded as applied")
  }

  // ---------------------------------------------------------------------------
  // targetModules
  // ---------------------------------------------------------------------------

  test("targetModules with the default unknown hint registers NOTHING (fail-safe)") {
    System.setProperty(CatalystMutationRule.MutationPolicy.TargetModulesProp, "tests/")

    buildJoinQuery().queryExecution.optimizedPlan

    assert(catalogEntries.isEmpty,
      "with no harness-fed hint, targetModules must register nothing rather than everything")
  }

  test("targetModules registers candidates under a matching hint prefix") {
    System.setProperty(CatalystMutationRule.MutationPolicy.TargetModulesProp, "tests/")
    CatalystMutationRule.setCurrentFilePathHint("tests/TestOrdersSpec.scala")

    buildJoinQuery().queryExecution.optimizedPlan

    assert(catalogEntries.nonEmpty,
      "a matching hint must let discovery register candidates")
    assert(catalogEntries.forall(_.getFilePath == "tests/TestOrdersSpec.scala"),
      "candidates must carry the fed hint as their filePath")
  }

  test("targetModules with a non-matching hint registers nothing") {
    System.setProperty(CatalystMutationRule.MutationPolicy.TargetModulesProp, "tests/")
    CatalystMutationRule.setCurrentFilePathHint("src/main/scala/Pipeline.scala")

    buildJoinQuery().queryExecution.optimizedPlan

    assert(catalogEntries.isEmpty,
      "a hint outside every prefix must filter everything out")
  }

  // ---------------------------------------------------------------------------
  // astDiffSnippet capture
  // ---------------------------------------------------------------------------

  test("a rewrite captures a bounded single-line astDiffSnippet into the catalog") {
    buildJoinQuery().queryExecution.optimizedPlan // discovery (IDLE)

    val mutant = catalogEntries
      .find(e => e.getOperatorType == OperatorTypeDto.JOIN && e.getMutationIndex == 1)
      .find { m =>
        MutantRegistry.getInstance().reset()
        AppliedMutantTracker.clear()
        MutantRegistry.getInstance().setActiveMutant(m.getMutantId)
        joinsIn(buildJoinQuery().queryExecution.optimizedPlan)
          .exists(_.joinType == org.apache.spark.sql.catalyst.plans.Cross)
      }
      .getOrElse(fail("no flowing JOIN idx-1 mutant"))
    MutantRegistry.getInstance().reset()
    AppliedMutantTracker.clear()
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    buildJoinQuery().queryExecution.optimizedPlan

    val snippet = MutationCatalogAccess.findByIdOrNull(mutant.getMutantId).getAstDiffSnippet
    assert(snippet != null && snippet.nonEmpty, "a rewrite must capture a diff snippet")
    assert(snippet.contains(" => "), s"snippet must be '<before> => <after>': $snippet")
    assert(!snippet.contains("\n"), "fragments must be single-line: " + snippet.take(80))
    assert(snippet.length <= 2 * CatalystMutationRule.MaxPlanFragmentChars + 4,
      "snippet must respect the shared fragment bound")
    MutantRegistry.getInstance().reset()
  }

  test("recordAstDiffSnippet replaces the entry and preserves mappedTestIds") {
    buildJoinQuery().queryExecution.optimizedPlan
    val mutant = catalogEntries.head

    MutationCatalogAccess.recordAstDiffSnippet(mutant.getMutantId, "Before Plan | Fragment => After Plan")

    val updated = MutationCatalogAccess.findByIdOrNull(mutant.getMutantId)
    assert(updated.getAstDiffSnippet == "Before Plan | Fragment => After Plan")
    assert(updated.getMappedTestIds == mutant.getMappedTestIds,
      "the snippet replacement must not disturb the test-impact map")
    assert(updated.getCoordinateHex == mutant.getCoordinateHex,
      "identity fields must be preserved")
  }

  test("recordAstDiffSnippet fails loudly on an unknown mutantId") {
    val ex = intercept[IllegalArgumentException] {
      MutationCatalogAccess.recordAstDiffSnippet("ffffffffffffffff", "a => b")
    }
    assert(ex.getMessage.contains("Unknown mutantId"))
  }

  test("an externally-orchestrated mutant fork persists the snippet as a sidecar") {
    // Simulate the fork environment: phase=mutant + output directory set.
    System.setProperty(MutantBootstrap.PROP_PHASE, MutantBootstrap.PHASE_MUTANT)
    System.setProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY, sidecarDir.toString)

    buildJoinQuery().queryExecution.optimizedPlan // discovery (IDLE)
    val mutant = catalogEntries
      .find(e => e.getOperatorType == OperatorTypeDto.JOIN && e.getMutationIndex == 1)
      .find { m =>
        MutantRegistry.getInstance().reset()
        AppliedMutantTracker.clear()
        MutantRegistry.getInstance().setActiveMutant(m.getMutantId)
        joinsIn(buildJoinQuery().queryExecution.optimizedPlan)
          .exists(_.joinType == org.apache.spark.sql.catalyst.plans.Cross)
      }
      .getOrElse(fail("no flowing JOIN idx-1 mutant"))
    MutantRegistry.getInstance().reset()

    val sidecar = sidecarDir.resolve("diffs").resolve(mutant.getMutantId + ".json")
    assert(java.nio.file.Files.isRegularFile(sidecar),
      "the mutant fork must persist the diff snippet sidecar")
    val node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(sidecar.toFile)
    assert(node.get("mutantId").asText() == mutant.getMutantId)
    assert(node.get("astDiffSnippet").asText().contains(" => "))
  }
}