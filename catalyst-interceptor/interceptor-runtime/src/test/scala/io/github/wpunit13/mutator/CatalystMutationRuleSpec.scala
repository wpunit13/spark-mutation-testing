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

  // MutantRegistry, the catalog, and the applied-mutant tracker are
  // JVM-wide singletons.
  override def beforeEach(): Unit = {
    MutantRegistry.getInstance().reset()
    MutationCatalogAccess.clearForTesting()
    AppliedMutantTracker.clear()
  }

  override def afterEach(): Unit = {
    MutantRegistry.getInstance().reset()
    MutationCatalogAccess.clearForTesting()
    AppliedMutantTracker.clear()
  }

  /** Two-table INNER join over local ranges; built from scratch on every call. */
  private def buildJoinQuery(): DataFrame = {
    val left = spark.range(0, 5, 1, 1).selectExpr("id AS lkey", "id AS lval")
    val right = spark.range(0, 5, 1, 1).selectExpr("id AS rkey", "id AS rval")
    left.join(right, left("lkey") === right("rkey"), "inner")
  }

  /** Mirrors the WP-15 example pipeline (USING join + And filter + select). */
  private def orders: DataFrame = {
    val s = spark
    import s.implicits._
    Seq(
      (1, "C1", 150, "COMPLETED"),
      (2, "C2", 150, "COMPLETED"),
      (3, "C1", 150, "PENDING"),
      (4, "C2", 50, "COMPLETED"),
      (5, "C99", 150, "COMPLETED"),
      (6, "C98", 150, "COMPLETED")
    ).toDF("order_id", "customer_id", "amount", "status")
  }

  private def customers: DataFrame = {
    val s = spark
    import s.implicits._
    Seq(
      ("C1", "Customer One"),
      ("C2", "Customer Two")
    ).toDF("customer_id", "customer_name")
  }

  /** Same construction as examples/spark-*-pipeline's OrdersPipeline. */
  private def buildReport(o: DataFrame, c: DataFrame): DataFrame =
    o
      .join(c, Seq("customer_id"))
      .filter(o("amount") > 100 && o("status") === "COMPLETED")
      .select("order_id", "customer_id", "amount", "status")

  private def joinsIn(plan: LogicalPlan): Seq[Join] =
    plan.collect { case j: Join => j }

  private def filtersIn(plan: LogicalPlan): Seq[Filter] =
    plan.collect { case f: Filter => f }

  private def catalogEntries: Seq[MutantMetadata] =
    MutationCatalogAccess.allEntries().asScala.toSeq

  private def findJoinMutant(mutationIndex: Int): MutantMetadata =
    findJoinMutants(mutationIndex).headOption.getOrElse(
      fail("no catalogued JOIN mutant with mutationIndex " + mutationIndex))

  /** All catalogued JOIN mutants with the given index (one per coordinate). */
  private def findJoinMutants(mutationIndex: Int): Seq[MutantMetadata] =
    catalogEntries.filter(e =>
      e.getOperatorType == OperatorTypeDto.JOIN && e.getMutationIndex == mutationIndex)

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

    // The catalog holds one (JOIN, 1) mutant per coordinate (the eager join
    // analysis and the UsingJoin analysis present the join at two shapes).
    // Only the coordinate whose plan flows into the executed query produces a
    // Cross in the optimized plan, so assert that at least one does.
    val mutants = findJoinMutants(1)
    assert(mutants.nonEmpty, "expected catalogued JOIN idx-1 mutants")

    val flowingMutants = mutants.filter { mutant =>
      MutantRegistry.getInstance().reset()
      MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)
      // Brand-new Dataset built from scratch: reusing the earlier reference
      // would serve a memoized plan and the test would pass for the wrong
      // reason.
      val plan = buildJoinQuery().queryExecution.optimizedPlan
      val cross = joinsIn(plan).exists(_.joinType == Cross)
      if (!cross) {
        info(s"mutant ${mutant.getMutantId} (coord ${mutant.getCoordinateHex}) targets a " +
          "discarded intermediate plan; no Cross in the executed plan")
      }
      cross
    }
    MutantRegistry.getInstance().reset()
    assert(flowingMutants.nonEmpty,
      "expected at least one JOIN idx-1 mutant to mutate the Join to Cross")
  }

  test("Re-applying the rule to an already-mutated plan is a no-op (idempotency)") {
    buildJoinQuery().queryExecution.optimizedPlan

    val mutant = findJoinMutants(1).find { m =>
      MutantRegistry.getInstance().reset()
      MutantRegistry.getInstance().setActiveMutant(m.getMutantId)
      joinsIn(buildJoinQuery().queryExecution.optimizedPlan).exists(_.joinType == Cross)
    }.getOrElse(fail("no JOIN idx-1 mutant flows into the executed plan"))
    MutantRegistry.getInstance().reset()
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    val mutatedPlan = buildJoinQuery().queryExecution.optimizedPlan
    assert(joinsIn(mutatedPlan).exists(_.joinType == Cross))

    // Construct the rules directly for this assertion: the post-hoc phase
    // re-classifies the mutated node (its coordinate still matches) but must
    // not re-tag it, and the optimizer phase must find no pending rewrite.
    val postHoc = new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.PostHoc)
    val optimizer = new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.Optimizer)
    val second = postHoc.apply(mutatedPlan)
    val third = optimizer.apply(second)
    assert(third == second, "re-application must not double-mutate")
    assert(joinsIn(third).forall(_.joinType == Cross))
  }

  test("alreadyMutated tag skips an already-rewritten node (no double mutation)") {
    // Drive both phases directly against the analyzed plan so that the
    // catalogued coordinates are computed against exactly the tree we
    // re-apply the rules to below (no optimizer interference).
    val analyzed = buildJoinQuery().where("lval > 1").queryExecution.analyzed
    val postHoc = new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.PostHoc)
    val optimizer = new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.Optimizer)
    postHoc.apply(analyzed) // registry IDLE: discovery against this exact tree

    // FILTER mutant "FILTER -> FALSE": re-classifying the mutated node still
    // yields a matching candidate, so without the tag the rule would mutate
    // it a second time. Pick the mutant that actually flows into THIS tree.
    val mutant = catalogEntries
      .filter(e => e.getOperatorType == OperatorTypeDto.FILTER && e.getMutationIndex == 2)
      .find { m =>
        MutantRegistry.getInstance().reset()
        AppliedMutantTracker.clear()
        MutantRegistry.getInstance().setActiveMutant(m.getMutantId)
        val tagged = postHoc.apply(analyzed)
        val rewritten = optimizer.apply(tagged)
        filtersIn(rewritten).exists(_.condition.isInstanceOf[Literal])
      }
      .getOrElse(fail("no FILTER idx-2 mutant flows into the analyzed tree"))
    MutantRegistry.getInstance().reset()
    AppliedMutantTracker.clear()
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    val tagged = postHoc.apply(analyzed)
    assert(tagged eq analyzed, "post-hoc matching must not rewrite the plan")

    val first = optimizer.apply(tagged)
    val firstConditions = filtersIn(first).map(_.condition)
    assert(firstConditions.exists(_.isInstanceOf[Literal]),
      "expected the filter condition to be rewritten to Literal(false)")

    val second = postHoc.apply(first)
    assert(second eq first, "already-rewritten node must not be re-matched")
    val third = optimizer.apply(second)
    assert(third == second, "already-mutated node must be skipped, not mutated again")
    assert(filtersIn(third).map(_.condition) == firstConditions,
      "condition must still be Literal(false), not Not(Literal(false))")
  }

  test("Unknown active mutant is a no-op: planning succeeds and plan is unmutated") {
    MutantRegistry.getInstance().setActiveMutant("ffffffffffffffff")
    val plan = buildJoinQuery().queryExecution.optimizedPlan
    val joinNodes = joinsIn(plan)
    assert(joinNodes.nonEmpty)
    assert(joinNodes.forall(_.joinType == Inner), "uncatalogued mutant must not mutate anything")
  }

  test("Post-hoc discovery catalogs one coordinate per logical node (single-shape)") {
    // Inner join + top-level And filter: the WP-15 defect catalogued the same
    // logical node under several coordinates because the OPTIMIZER re-presented
    // it in every batch (analyzer shape, post-pushdown, constraint-inferred
    // copy). At the post-hoc resolution point the rule runs exactly once per
    // analysis, so one rule invocation over one plan must yield exactly one
    // coordinate per logical node.
    val analyzed = buildJoinQuery().where("lval > 1 AND rval < 3").queryExecution.analyzed

    // Construction triggered Spark's own eager analyses; reset the catalog so
    // the assertions below see ONLY this single discovery invocation.
    MutationCatalogAccess.clearForTesting()
    val rule = new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.PostHoc)
    rule.apply(analyzed)

    val entries = catalogEntries
    // No duplicate mutants: the same (coordinate, operator, index) triple must
    // never be catalogued twice by a single discovery invocation.
    val keys = entries.map(e => (e.getCoordinateHex, e.getOperatorType, e.getMutationIndex))
    assert(keys.distinct.size == entries.size,
      "single-shape discovery must not catalogue duplicate mutants; got: "
        + entries.map(e => (e.getOperatorType, e.getMutationIndex, e.getCoordinateHex)))

    val joinEntries = entries.filter(_.getOperatorType == OperatorTypeDto.JOIN)
    assert(joinEntries.map(_.getMutationIndex).sorted == Seq(0, 1, 2),
      "expected JOIN mutation indices 0/1/2, got: " + joinEntries.map(_.getMutationIndex))
    assert(joinEntries.map(_.getCoordinateHex).distinct.size == 1,
      "the single Join node must yield exactly one coordinate, got: "
        + joinEntries.map(_.getCoordinateHex).distinct)

    val filterEntries = entries.filter(_.getOperatorType == OperatorTypeDto.FILTER)
    assert(filterEntries.map(_.getMutationIndex).sorted == Seq(0, 1, 2, 3),
      "expected FILTER mutation indices 0/1/2/3, got: " + filterEntries.map(_.getMutationIndex))
    assert(filterEntries.map(_.getCoordinateHex).distinct.size == 1,
      "the single Filter node must yield exactly one coordinate (WP-15 saw three), got: "
        + filterEntries.map(_.getCoordinateHex).distinct)
  }

  test("Active mode records the applied mutant id (applied-mutation tracking)") {
    assert(AppliedMutantTracker.lastOrNull() == null, "nothing applied yet")

    // Discovery run populates the catalog while the registry is IDLE.
    buildJoinQuery().queryExecution.optimizedPlan

    // Only the coordinate whose plan flows into the executed query gets a
    // rewrite (and therefore a tracker record); assert at least one does.
    val recorded = findJoinMutants(1).exists { mutant =>
      MutantRegistry.getInstance().reset()
      AppliedMutantTracker.clear()
      MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)
      buildJoinQuery().queryExecution.optimizedPlan
      AppliedMutantTracker.lastOrNull() == mutant.getMutantId
    }
    MutantRegistry.getInstance().reset()
    assert(recorded,
      "the optimizer phase must record the active mutant id after a successful rewrite")
  }

  test("No-match does not record an applied mutant") {
    // Discovery run populates the catalog with JOIN mutants.
    buildJoinQuery().queryExecution.optimizedPlan

    val mutant = findJoinMutant(1)
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    // A query with no Join node: the active mutant's coordinate matches
    // nothing, the rule returns the plan unchanged, and the tracker must
    // stay empty so the harness can classify the fork as not-applied.
    val plan = spark.range(0, 3).selectExpr("id").queryExecution.optimizedPlan
    assert(joinsIn(plan).isEmpty)
    assert(AppliedMutantTracker.lastOrNull() == null,
      "a no-match must not be recorded as an applied mutation")
  }

  test("ShimMutationException from the shim propagates unchanged") {
    val analyzed = spark.range(0, 3).selectExpr("id").queryExecution.analyzed
    val rule = new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.PostHoc)

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

  test("Post-hoc active mode defers the rewrite to the optimizer phase") {
    // Discovery run populates the catalog while the registry is IDLE.
    buildJoinQuery().queryExecution.optimizedPlan

    val mutant = findJoinMutants(1).find { m =>
      // pick a coordinate whose plan flows into the executed query
      MutantRegistry.getInstance().reset()
      MutantRegistry.getInstance().setActiveMutant(m.getMutantId)
      joinsIn(buildJoinQuery().queryExecution.optimizedPlan).exists(_.joinType == Cross)
    }.getOrElse(fail("no flowing JOIN idx-1 mutant"))
    MutantRegistry.getInstance().reset()
    MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)

    // Analysis ONLY (no optimizer): the session's post-hoc rule tags the
    // matched node, and the rewrite must NOT have happened yet.
    val analyzed = buildJoinQuery().queryExecution.analyzed
    assert(joinsIn(analyzed).forall(_.joinType == Inner),
      "post-hoc matching must defer the rewrite; plan:\n" + analyzed.toString)

    // The optimizer-phase rule performs the deferred rewrite.
    val optimizer = new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.Optimizer)
    val rewritten = optimizer.apply(analyzed)
    assert(joinsIn(rewritten).exists(_.joinType == Cross),
      "the optimizer-phase rule must rewrite the tagged node")
    assert(AppliedMutantTracker.lastOrNull() == mutant.getMutantId,
      "the optimizer phase must record the applied mutant id")
  }

  test("INNER -> ANTI executes at the analyzed shape (USING-join honesty)") {
    // WP-17 regression guard for the WP-15 (JOIN, 2) transition: at the
    // post-hoc point the join sits below the analyzer-manufactured USING-join
    // dedup Project (referencing customer_name). Rewriting INNER -> ANTI there
    // would be rejected by the Finish Analysis check; deferring the rewrite to
    // the optimizer lets CollapseProject merge that Project away first, so the
    // ANTI plan analyzes and executes.
    buildReport(orders, customers).collect() // discovery (IDLE)

    val antiMutants = catalogEntries.filter(e =>
      e.getOperatorType == OperatorTypeDto.JOIN && e.getMutationIndex == 2)
    assert(antiMutants.nonEmpty, "expected catalogued JOIN idx-2 (ANTI) mutants")

    val failures = antiMutants.flatMap { mutant =>
      MutantRegistry.getInstance().reset()
      AppliedMutantTracker.clear()
      MutantRegistry.getInstance().setActiveMutant(mutant.getMutantId)
      try {
        val rows = buildReport(orders, customers).collect().length
        info(s"ANTI mutant ${mutant.getMutantId} executed, rows=$rows, " +
          s"applied=${AppliedMutantTracker.lastOrNull()}")
        None
      } catch {
        case t: Throwable =>
          Some(s"mutant ${mutant.getMutantId} threw ${t.getClass.getSimpleName}: " +
            String.valueOf(t.getMessage).take(160))
      }
    }
    MutantRegistry.getInstance().reset()
    assert(failures.isEmpty,
      "ANTI mutants must execute, not break analysis: " + failures.mkString("; "))
    assert(antiMutants.exists(m => AppliedMutantTracker.lastOrNull() == m.getMutantId) ||
      antiMutants.exists { m =>
        // re-run one flowing mutant to confirm the tracker records the rewrite
        MutantRegistry.getInstance().reset()
        AppliedMutantTracker.clear()
        MutantRegistry.getInstance().setActiveMutant(m.getMutantId)
        scala.util.Try(buildReport(orders, customers).collect().length).isSuccess &&
          AppliedMutantTracker.lastOrNull() == m.getMutantId
      },
      "at least one ANTI mutant must actually execute (tracker records the rewrite)")
    MutantRegistry.getInstance().reset()
  }
}
