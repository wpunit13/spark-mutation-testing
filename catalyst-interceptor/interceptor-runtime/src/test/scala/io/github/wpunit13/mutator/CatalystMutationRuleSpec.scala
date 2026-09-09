package io.github.wpunit13.mutator

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.wpunit13.mutator.api.SparkShimVersion
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess
import io.github.wpunit13.mutator.dispatch.ShimDispatcher
import io.github.wpunit13.mutator.model.MutantMetadata
import io.github.wpunit13.mutator.model.OperatorTypeDto
import io.github.wpunit13.mutator.MutantRegistry
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

/**
 * End-to-end specs exercising the real extension path:
 * spark.sql.extensions -> MutatorSparkExtension -> ShimDispatcher ->
 * ServiceLoader shim -> CatalystMutationRule, on a local SparkSession.
 */
class CatalystMutationRuleSpec extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val mapper = new ObjectMapper()

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("CatalystMutationRuleSpec")
      .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  // MutantRegistry and the catalog are JVM-wide singletons.
  override def beforeEach(): Unit = {
    MutantRegistry.getInstance().reset()
    MutationCatalogAccess.clearForTesting()
  }

  override def afterEach(): Unit = {
    MutantRegistry.getInstance().reset()
    MutationCatalogAccess.clearForTesting()
  }

  /** Two-table INNER join over local ranges; built from scratch on every call. */
  private def buildJoinQuery(): DataFrame = {
    val left = spark.range(0, 5, 1, 1).selectExpr("id AS lkey", "id AS lval")
    val right = spark.range(0, 5, 1, 1).selectExpr("id AS rkey", "id AS rval")
    left.join(right, left("lkey") === right("rkey"), "inner")
  }

  private def joinsIn(plan: LogicalPlan): Seq[Join] =
    plan.collect { case j: Join => j }

  private def filtersIn(plan: LogicalPlan): Seq[Filter] =
    plan.collect { case f: Filter => f }

  private def catalogEntries: Seq[MutantMetadata] =
    MutationCatalogAccess.allEntries().asScala.toSeq

  private def findJoinMutant(mutationIndex: Int): MutantMetadata =
    catalogEntries
      .find(e => e.getOperatorType == OperatorTypeDto.JOIN && e.getMutationIndex == mutationIndex)
      .getOrElse(fail("no catalogued JOIN mutant with mutationIndex " + mutationIndex))

  private def findFilterMutant(mutationIndex: Int): MutantMetadata =
    catalogEntries
      .find(e => e.getOperatorType == OperatorTypeDto.FILTER && e.getMutationIndex == mutationIndex)
      .getOrElse(fail("no catalogued FILTER mutant with mutationIndex " + mutationIndex))

  test("ShimDispatcher resolves the 3.5/2.13 shim through ServiceLoader") {
    // Lives here rather than ShimDispatcherSpec because interceptor-dispatch
    // deliberately has no shim on its test classpath.
    assert(ShimDispatcher.activeShim.supportedVersion == SparkShimVersion("3.5", "2.13"))
  }

  test("Discovery mode catalogs at least one JOIN entry") {
    buildJoinQuery().queryExecution.optimizedPlan

    val json = mapper.readTree(MutationCatalogAccess.getFullCatalogJson())
    val joinEntries = json.asScala.filter(_.get("operatorType").asText() == "JOIN")
    assert(joinEntries.nonEmpty, "expected at least one JOIN entry in the catalog")
  }

  test("Discovery mode returns the plan unchanged (join stays Inner)") {
    val plan = buildJoinQuery().queryExecution.optimizedPlan
    val joinNodes = joinsIn(plan)
    assert(joinNodes.nonEmpty, "expected a Join node in the optimized plan")
    assert(joinNodes.forall(_.joinType == Inner), "nothing must be mutated while IDLE")
  }

  test("Active mode mutates the catalogued JOIN (mutationIndex 1: INNER -> CROSS)") {
    // Discovery run populates the catalog while the registry is IDLE.
    buildJoinQuery().queryExecution.optimizedPlan

    val mutant = findJoinMutant(1)
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    // Brand-new Dataset built from scratch: reusing the earlier reference
    // would serve a memoized plan and the test would pass for the wrong
    // reason.
    val plan = buildJoinQuery().queryExecution.optimizedPlan

    assert(
      joinsIn(plan).exists(_.joinType == Cross),
      "expected the Join node to be mutated to Cross; plan:\n" + plan.toString)
  }

  test("Re-applying the rule to an already-mutated plan is a no-op (idempotency)") {
    buildJoinQuery().queryExecution.optimizedPlan

    val mutant = findJoinMutant(1)
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    val mutatedPlan = buildJoinQuery().queryExecution.optimizedPlan
    assert(joinsIn(mutatedPlan).exists(_.joinType == Cross))

    // Construct the rule directly for this assertion.
    val rule = new CatalystMutationRule(ShimDispatcher.activeShim)
    val second = rule.apply(mutatedPlan)
    val third = rule.apply(second)
    assert(third == second, "second invocation must not double-mutate")
    assert(joinsIn(third).forall(_.joinType == Cross))
  }

  test("alreadyMutated tag skips an already-rewritten node (no double mutation)") {
    // Discovery via the rule directly against the analyzed plan so that the
    // catalogued coordinates are computed against exactly the tree we
    // re-apply the rule to below (no optimizer interference).
    val analyzed = buildJoinQuery().where("lval > 1").queryExecution.analyzed
    val discoveryRule = new CatalystMutationRule(ShimDispatcher.activeShim)
    discoveryRule.apply(analyzed)

    // FILTER mutant "FILTER -> FALSE": re-classifying the mutated node still
    // yields a matching candidate, so without the tag the rule would mutate
    // it a second time.
    val mutant = findFilterMutant(2)
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    val first = discoveryRule.apply(analyzed)
    val firstConditions = filtersIn(first).map(_.condition)
    assert(firstConditions.exists(_.isInstanceOf[Literal]),
      "expected the filter condition to be rewritten to Literal(false)")

    val second = discoveryRule.apply(first)
    assert(second == first, "tagged node must be skipped, not mutated again")
    assert(filtersIn(second).map(_.condition) == firstConditions,
      "condition must still be Literal(false), not Not(Literal(false))")
  }

  test("Unknown active mutant is a no-op: planning succeeds and plan is unmutated") {
    MutantRegistry.getInstance().setActiveMutant("ffffffffffffffff")
    val plan = buildJoinQuery().queryExecution.optimizedPlan
    val joinNodes = joinsIn(plan)
    assert(joinNodes.nonEmpty)
    assert(joinNodes.forall(_.joinType == Inner), "uncatalogued mutant must not mutate anything")
  }

  test("ShimMutationException from the shim propagates unchanged") {
    val analyzed = spark.range(0, 3).selectExpr("id").queryExecution.analyzed
    val rule = new CatalystMutationRule(ShimDispatcher.activeShim)

    // No mutant active: Discovery mode, plan returned unchanged, no throw.
    assert(rule.apply(analyzed) ne null)

    // Direct probe of the propagation contract: the shim's
    // ShimMutationException must surface unchanged out of a mutation call,
    // not be caught, wrapped, or swallowed by the rule layer.
    val ex = intercept[io.github.wpunit13.mutator.api.ShimMutationException] {
      ShimDispatcher.activeShim.mutateJoin(analyzed, 0) // analyzed is not a Join node
    }
    assert(ex.getMessage.contains("mutateJoin expects a Join node"))
  }
}
