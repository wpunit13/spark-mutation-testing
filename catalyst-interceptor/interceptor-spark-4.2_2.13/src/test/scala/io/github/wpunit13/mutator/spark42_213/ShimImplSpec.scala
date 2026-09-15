package io.github.wpunit13.mutator.spark42_213

import io.github.wpunit13.mutator.api.{NodeCoordinateFactory, OperatorType}
import io.github.wpunit13.mutator.api.ShimMutationException
import io.github.wpunit13.mutator.hash.DeterministicHasher
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, count, lit, sum}
import org.apache.spark.sql.catalyst.expressions.{
  Alias, And, Ascending, Descending, Literal, Not, SortOrder,
  SpecifiedWindowFrame, UnaryMinus, UnboundedPreceding
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.{Cross, LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Window}
import org.apache.spark.sql.types.{BooleanType, LongType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * WP-20 golden cross-version tests (VERSION_ADDITION_SOP step 3).
 *
 * Every value below is PINNED, never computed from this implementation:
 *  - AGGREGATE and WINDOW reuse the 3.5 goldens verbatim (the same values the
 *    3.5 ShimImplSpec pins) — the non-negotiable cross-version guard.
 *  - JOIN, FILTER and PROJECT are new goldens for families the 3.5 spec had
 *    not pinned; they were computed ONCE from the trusted 3.5 shim via a
 *    throwaway runner (never from this 4.2 implementation), reviewed, and
 *    frozen here. The runner's 3.5-vs-4.2 output diff was empty — every
 *    signature, coordinate and mutant id is byte-identical across the two
 *    Spark lines on these canonical fixtures.
 *
 * If any assertion fails, a Catalyst rendering/structure drift between Spark
 * versions has broken cross-version mutant reproducibility — stop and fix the
 * canonicalization, never the golden values.
 */
class ShimImplSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("Spark42ShimImplSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  private val shim: ShimImpl = new ShimImpl

  // -- canonical fixtures (identical to the ones the goldens were pinned on) --

  private def orders: DataFrame = {
    val s = spark
    import s.implicits._
    Seq(
      (1, "C1", 150, "COMPLETED"),
      (2, "C2", 150, "COMPLETED"),
      (3, "C1", 150, "PENDING"),
      (4, "C2", 50, "COMPLETED")
    ).toDF("order_id", "customer_id", "amount", "status")
  }

  private def customers: DataFrame = {
    val s = spark
    import s.implicits._
    Seq(("C1", "Customer One"), ("C2", "Customer Two")).toDF("customer_id", "customer_name")
  }

  private def groupedAggregated(): DataFrame = {
    val s = spark
    import s.implicits._
    Seq(("Engineering", "US-East", 100000L), ("Sales", "US-West", 80000L))
      .toDF("dept", "region", "salary")
      .groupBy("dept", "region")
      .agg(sum("salary"))
  }

  private def windowed(): DataFrame = {
    val s = spark
    import s.implicits._
    Seq(("Engineering", 100000L), ("Sales", 80000L))
      .toDF("dept", "salary")
      .select(
        col("dept"),
        col("salary"),
        org.apache.spark.sql.functions.row_number()
          .over(org.apache.spark.sql.expressions.Window.partitionBy("dept").orderBy("salary")).as("rn")
      )
  }

  private def coalesced(): DataFrame = {
    val s = spark
    import s.implicits._
    Seq(("Alice", 100), (null, 200))
      .toDF("name", "val")
      .select(coalesce(col("name"), lit("UNKNOWN")).as("c_name"), col("val"))
  }

  /** Pre-order walk returning the FIRST matching node with its (depth, ordinal). */
  private def findNode(df: DataFrame, pick: PartialFunction[LogicalPlan, LogicalPlan]): (LogicalPlan, Int, Int) = {
    val analyzed = df.queryExecution.analyzed
    def walk(node: LogicalPlan, depth: Int, ordinal: Int): Option[(LogicalPlan, Int, Int)] =
      if (pick.isDefinedAt(node)) Some((node, depth, ordinal))
      else {
        var found: Option[(LogicalPlan, Int, Int)] = None
        val it = node.children.zipWithIndex.iterator
        while (found.isEmpty && it.hasNext) {
          val (c, i) = it.next()
          found = walk(c, depth + 1, i)
        }
        found
      }
    walk(analyzed, 0, -1).getOrElse(fail("canonical fixture node not found in the analyzed plan"))
  }

  // ---------------------------------------------------------------------------
  // JOIN golden (new: computed once from the trusted 3.5 shim, frozen)
  // ---------------------------------------------------------------------------

  test("golden cross-version: pinned canonical join query yields byte-identical NodeCoordinate and MutantID") {
    // Canonical query: orders JOIN customers ON customer_id (USING join).
    // Classified at the node's real analyzed-plan position (depth 1, ordinal 0:
    // the analyzer's dedup Project sits above the join).
    //   coordinate = 1|JOIN|0|<exprSig>
    //   mutantId   = test/path|<coordinateHex>|JOIN|<index>
    val GOLDEN_SIG = "INNER;(#1 = #1)"
    val GOLDEN_COORDINATE = "523338459279a153"
    val GOLDEN_MUTANT_IDS = Map(
      0 -> "9a8161b978ff8f54",
      1 -> "2a7e335fa2c10d16",
      2 -> "89c4588b903bc4bd")

    val (joinNode, depth, ordinal) = findNode(orders.join(customers, Seq("customer_id")), { case j: Join => j })
    assert((depth, ordinal) == (1, 0), "canonical join plan shape drifted")

    val sig = shim.canonicalExprSig(joinNode, OperatorType.Join)
    assert(sig == GOLDEN_SIG, s"join signature drifted:\n  got  = $sig\n  want = $GOLDEN_SIG")

    val coordinate = NodeCoordinateFactory(depth, OperatorType.Join, ordinal, sig)
    assert(coordinate.toHex == GOLDEN_COORDINATE)

    val (_, candidates) = shim.classify(joinNode, depth, ordinal).getOrElse(fail("expected Join candidates"))
    assert(candidates.map(_.mutationIndex) == Seq(0, 1, 2))
    assert(candidates.forall(_.coordinate.toHex == GOLDEN_COORDINATE))

    GOLDEN_MUTANT_IDS.foreach { case (index, goldenId) =>
      val mutantId = DeterministicHasher.computeMutantId("test/path", coordinate.toHex, "JOIN", index)
      assert(mutantId == goldenId, s"JOIN mutant id drifted for index $index")
    }
  }

  // ---------------------------------------------------------------------------
  // FILTER golden (new: computed once from the trusted 3.5 shim, frozen)
  // ---------------------------------------------------------------------------

  test("golden cross-version: pinned canonical filter query yields byte-identical NodeCoordinate and MutantID") {
    // Canonical query: orders WHERE amount > 100 AND status = 'COMPLETED'
    // (top-level And — all four filter mutants). The Filter is the analyzed
    // plan's root: depth 0, ordinal -1.
    val GOLDEN_SIG = "((#2 > 100) AND (#3 = 'COMPLETED'))"
    val GOLDEN_COORDINATE = "72661923281222c8"
    val GOLDEN_MUTANT_IDS = Map(
      0 -> "3e568eb2d3b0a0de",
      1 -> "c27b1025b6fcfcac",
      2 -> "a58232bc47cc1ee6",
      3 -> "d119c06180d1b06b")

    val (filterNode, depth, ordinal) = findNode(orders.where("amount > 100 AND status = 'COMPLETED'"), { case f: Filter => f })
    assert((depth, ordinal) == (0, -1), "canonical filter plan shape drifted")

    val sig = shim.canonicalExprSig(filterNode, OperatorType.Filter)
    assert(sig == GOLDEN_SIG, s"filter signature drifted:\n  got  = $sig\n  want = $GOLDEN_SIG")

    val coordinate = NodeCoordinateFactory(depth, OperatorType.Filter, ordinal, sig)
    assert(coordinate.toHex == GOLDEN_COORDINATE)

    val (_, candidates) = shim.classify(filterNode, depth, ordinal).getOrElse(fail("expected Filter candidates"))
    assert(candidates.map(_.mutationIndex) == Seq(0, 1, 2, 3))
    assert(candidates.forall(_.coordinate.toHex == GOLDEN_COORDINATE))

    GOLDEN_MUTANT_IDS.foreach { case (index, goldenId) =>
      val mutantId = DeterministicHasher.computeMutantId("test/path", coordinate.toHex, "FILTER", index)
      assert(mutantId == goldenId, s"FILTER mutant id drifted for index $index")
    }
  }

  // ---------------------------------------------------------------------------
  // AGGREGATE golden (reuses the 3.5-pinned values verbatim)
  // ---------------------------------------------------------------------------

  test("golden cross-version: pinned canonical aggregate query yields byte-identical NodeCoordinate and MutantID") {
    // Canonical query: groupBy("dept", "region").agg(sum("salary")) — the SAME
    // fixture and pinned values as the 3.5 ShimImplSpec's golden test. The
    // node is classified at the same synthetic position (depth 1, ordinal 0)
    // the 3.5 golden was pinned at.
    //   coordinate = 1|AGGREGATE|0|<exprSig>
    //   mutantId   = test/path|<coordinateHex>|AGGREGATE|0
    val GOLDEN_SIG = "#0;#1;sum(#x) AS `sum(#x)`;#0;#1"
    val GOLDEN_COORDINATE = "5fa3d5da36b4095e"
    val GOLDEN_MUTANT_ID = "6896f83575625e63"

    val aggNode = findNode(groupedAggregated(), { case a: Aggregate => a })._1

    val sig = shim.canonicalExprSig(aggNode, OperatorType.Aggregate)
    assert(sig == GOLDEN_SIG, s"aggregate signature drifted:\n  got  = $sig\n  want = $GOLDEN_SIG")

    val coordinate = NodeCoordinateFactory(1, OperatorType.Aggregate, 0, sig)
    assert(coordinate.toHex == GOLDEN_COORDINATE)

    val (_, candidates) = shim.classify(aggNode, 1, 0).getOrElse(fail("expected Aggregate candidates"))
    assert(candidates.map(_.mutationIndex) == Seq(0, 1, 2))
    assert(candidates.forall(_.coordinate.toHex == GOLDEN_COORDINATE))

    val mutantId = DeterministicHasher.computeMutantId("test/path", coordinate.toHex, "AGGREGATE", 0)
    assert(mutantId == GOLDEN_MUTANT_ID)
  }

  // ---------------------------------------------------------------------------
  // WINDOW golden (reuses the 3.5-pinned values verbatim)
  // ---------------------------------------------------------------------------

  test("golden cross-version: pinned canonical window query yields byte-identical NodeCoordinate and MutantID") {
    // Canonical query: row_number().over(Window.partitionBy("dept").orderBy("salary"))
    // — the SAME fixture and pinned values as the 3.5 ShimImplSpec's golden
    // test; the node is classified at the same synthetic (2, 0) position.
    val GOLDEN_WIN_SIG = "row_number() OVER (PARTITION BY #0 ORDER BY #1 ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn;#0;#1 ASC NULLS FIRST"
    val GOLDEN_WIN_COORDINATE = "b580d553f8217aac"
    val GOLDEN_WIN_MUTANT_ID = "3aec48466076d976"

    val winNode = findNode(windowed(), { case w: Window => w })._1

    val sig = shim.canonicalExprSig(winNode, OperatorType.Window)
    assert(sig == GOLDEN_WIN_SIG, s"window signature drifted:\n  got  = $sig\n  want = $GOLDEN_WIN_SIG")

    val coordinate = NodeCoordinateFactory(2, OperatorType.Window, 0, sig)
    assert(coordinate.toHex == GOLDEN_WIN_COORDINATE)

    val (_, candidates) = shim.classify(winNode, 2, 0).getOrElse(fail("expected Window candidates"))
    assert(candidates.map(_.mutationIndex) == Seq(0, 1))
    assert(candidates.forall(_.coordinate.toHex == GOLDEN_WIN_COORDINATE))

    val mutantId = DeterministicHasher.computeMutantId("test/path", coordinate.toHex, "WINDOW", 0)
    assert(mutantId == GOLDEN_WIN_MUTANT_ID)
  }

  // ---------------------------------------------------------------------------
  // PROJECT golden (new: computed once from the trusted 3.5 shim, frozen)
  // ---------------------------------------------------------------------------

  test("golden cross-version: pinned canonical project query yields byte-identical NodeCoordinate and MutantID") {
    // Canonical query: select(coalesce(name, 'UNKNOWN') AS c_name, val) — the
    // Project is the analyzed plan's root: depth 0, ordinal -1.
    val GOLDEN_SIG = "coalesce(#x, 'UNKNOWN') AS c_#x;#1"
    val GOLDEN_COORDINATE = "435897d126803574"
    val GOLDEN_MUTANT_IDS = Map(
      0 -> "ecb78e13d4ac5ef4",
      1 -> "fb8ca8b5f2494d7f")

    val (projNode, depth, ordinal) = findNode(coalesced(), { case p: Project => p })
    assert((depth, ordinal) == (0, -1), "canonical project plan shape drifted")

    val sig = shim.canonicalExprSig(projNode, OperatorType.Project)
    assert(sig == GOLDEN_SIG, s"project signature drifted:\n  got  = $sig\n  want = $GOLDEN_SIG")

    val coordinate = NodeCoordinateFactory(depth, OperatorType.Project, ordinal, sig)
    assert(coordinate.toHex == GOLDEN_COORDINATE)

    val (_, candidates) = shim.classify(projNode, depth, ordinal).getOrElse(fail("expected Project candidates"))
    assert(candidates.map(_.mutationIndex) == Seq(0, 1))
    assert(candidates.forall(_.coordinate.toHex == GOLDEN_COORDINATE))

    GOLDEN_MUTANT_IDS.foreach { case (index, goldenId) =>
      val mutantId = DeterministicHasher.computeMutantId("test/path", coordinate.toHex, "PROJECT", index)
      assert(mutantId == goldenId, s"PROJECT mutant id drifted for index $index")
    }
  }

  // ---------------------------------------------------------------------------
  // OTHER family: non-target nodes classify to nothing with an empty signature
  // ---------------------------------------------------------------------------

  test("golden cross-version: non-target nodes classify to OTHER (no candidates, empty signature)") {
    // spark.range's analyzed plan root is a Range node — a logical node outside
    // the five mutation-target families.
    val (node, _, _) = findNode(spark.range(0, 3).toDF(), { case n: LogicalPlan => n })

    assert(shim.classify(node, 0, -1).isEmpty,
      "a non-target logical node must yield no mutation candidates")
    assert(shim.canonicalExprSig(node, OperatorType.Other) == "",
      "the OTHER family's canonical signature is the empty string")
  }

  // ---------------------------------------------------------------------------
  // Mutation execution: the apply path on real Spark 4.2 plans (node-level,
  // mirroring the 3.5 shared spec's mutation assertions). The goldens above
  // pin Discovery; these pin the rewrite constructors the optimizer phase
  // calls — the surface the SOP's compile proof cannot exercise.
  // ---------------------------------------------------------------------------

  private def findJoin(df: DataFrame): Join =
    df.queryExecution.analyzed.collectFirst { case j: Join => j }
      .getOrElse(fail("no Join node found in analyzed plan"))

  private def findFilter(df: DataFrame): Filter =
    df.queryExecution.analyzed.collectFirst { case f: Filter => f }
      .getOrElse(fail("no Filter node found in analyzed plan"))

  private def findAggregate(df: DataFrame): Aggregate =
    df.queryExecution.analyzed.collectFirst { case a: Aggregate => a }
      .getOrElse(fail("no Aggregate node found in analyzed plan"))

  private def findWindow(df: DataFrame): Window =
    df.queryExecution.analyzed.collectFirst { case w: Window => w }
      .getOrElse(fail("no Window node found in analyzed plan"))

  private def findProject(df: DataFrame): Project =
    df.queryExecution.analyzed.collectFirst { case p: Project => p }
      .getOrElse(fail("no Project node found in analyzed plan"))

  private def joined(): DataFrame = {
    val s = spark
    import s.implicits._
    val left = Seq((1, 10), (2, 20)).toDF("l_id", "amount")
    val right = Seq((1, "COMPLETED"), (2, "FAILED")).toDF("r_id", "status")
    left.join(right, col("l_id") === col("r_id"), "inner")
  }

  private def andFiltered(): DataFrame = {
    val s = spark
    import s.implicits._
    Seq(("COMPLETED", 5), ("FAILED", -1)).toDF("status", "amount")
      .filter(col("status") === "COMPLETED" && col("amount") > 0)
  }

  private def singleFiltered(): DataFrame = {
    val s = spark
    import s.implicits._
    Seq(("COMPLETED", 5), ("FAILED", -1)).toDF("status", "amount")
      .filter(col("status") === "COMPLETED")
  }

  private def countAggregated(): DataFrame = {
    val s = spark
    import s.implicits._
    Seq(("D1", 100L)).toDF("dept", "salary").groupBy("dept").agg(count("salary"))
  }

  private def singleKeyAggregated(): DataFrame = {
    val s = spark
    import s.implicits._
    Seq(("D1", 100L)).toDF("dept", "salary").groupBy("dept").agg(sum("salary"))
  }

  test("mutateJoin with mutationIndex 1 produces a conditionless Cross join") {
    val joinNode = findJoin(joined())
    val mutated = shim.mutateJoin(joinNode, 1).asInstanceOf[Join]
    assert(mutated.joinType == Cross)
    assert(mutated.condition.isEmpty,
      "CROSS must drop the join predicate or the mutation is a semantic no-op")
  }

  test("mutateJoin with mutationIndex 0 produces a LeftOuter join (condition preserved)") {
    val joinNode = findJoin(joined())
    val mutated = shim.mutateJoin(joinNode, 0).asInstanceOf[Join]
    assert(mutated.joinType == LeftOuter)
    assert(mutated.condition == joinNode.condition)
  }

  test("mutateJoin with mutationIndex 2 produces a LeftAnti join") {
    val joinNode = findJoin(joined())
    val mutated = shim.mutateJoin(joinNode, 2).asInstanceOf[Join]
    assert(mutated.joinType == LeftAnti)
  }

  test("mutateJoin with an out-of-range mutationIndex throws ShimMutationException") {
    val joinNode = findJoin(joined())
    intercept[ShimMutationException] {
      shim.mutateJoin(joinNode, 99)
    }
  }

  test("mutateJoin on a non-Join node throws ShimMutationException") {
    val filterNode = findFilter(andFiltered())
    intercept[ShimMutationException] {
      shim.mutateJoin(filterNode, 0)
    }
  }

  test("mutateFilter with mutationIndex 0 keeps the left conjunct (1 the right)") {
    val filterNode = findFilter(andFiltered())
    val original = filterNode.condition.asInstanceOf[And]
    val left = shim.mutateFilter(filterNode, 0).asInstanceOf[Filter]
    assert(left.condition == original.left)
    val right = shim.mutateFilter(filterNode, 1).asInstanceOf[Filter]
    assert(right.condition == original.right)
  }

  test("mutateFilter with mutationIndex 2 forces the condition to literal false") {
    val filterNode = findFilter(andFiltered())
    val mutated = shim.mutateFilter(filterNode, 2).asInstanceOf[Filter]
    assert(mutated.condition == Literal(false, BooleanType))
  }

  test("mutateFilter with mutationIndex 3 negates the condition") {
    val filterNode = findFilter(andFiltered())
    val mutated = shim.mutateFilter(filterNode, 3).asInstanceOf[Filter]
    assert(mutated.condition == Not(filterNode.condition))
  }

  test("mutateFilter with mutationIndex 0 on a non-And condition throws ShimMutationException") {
    val filterNode = findFilter(singleFiltered())
    intercept[ShimMutationException] {
      shim.mutateFilter(filterNode, 0)
    }
  }

  test("mutateAggregate with mutationIndex 0 rewrites sum to max") {
    val aggNode = findNode(groupedAggregated(), { case a: Aggregate => a })._1.asInstanceOf[Aggregate]
    val mutated = shim.mutateAggregate(aggNode, 0).asInstanceOf[Aggregate]
    assert(!mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Sum])))
    assert(mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Max])))
  }

  test("mutateAggregate with mutationIndex 0 rewrites count to literal 1 (LongType preserved)") {
    val aggNode = findNode(countAggregated(), { case a: Aggregate => a })._1.asInstanceOf[Aggregate]
    val mutated = shim.mutateAggregate(aggNode, 0).asInstanceOf[Aggregate]
    assert(!mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Count])))
    val countAlias = mutated.aggregateExpressions.collectFirst {
      case a: org.apache.spark.sql.catalyst.expressions.Alias if a.name == "count(salary)" => a
    }.getOrElse(fail("expected count(salary) alias"))
    val literal = countAlias.child.asInstanceOf[Literal]
    assert(literal.value == 1L)
    // Count produces LongType; the mutation must preserve it rather than
    // silently downgrading the output column to IntegerType.
    assert(literal.dataType == LongType)
  }

  test("mutateAggregate with mutationIndex 1 drops the last grouping key") {
    val aggNode = findNode(groupedAggregated(), { case a: Aggregate => a })._1.asInstanceOf[Aggregate]
    val mutated = shim.mutateAggregate(aggNode, 1).asInstanceOf[Aggregate]
    assert(mutated.groupingExpressions.length == aggNode.groupingExpressions.length - 1)
    assert(mutated.groupingExpressions == aggNode.groupingExpressions.dropRight(1))
  }

  test("mutateAggregate with mutationIndex 2 zeros the aggregate") {
    val aggNode = findNode(groupedAggregated(), { case a: Aggregate => a })._1.asInstanceOf[Aggregate]
    val mutated = shim.mutateAggregate(aggNode, 2).asInstanceOf[Aggregate]
    assert(mutated.aggregateExpressions != aggNode.aggregateExpressions,
      "zeroing must replace at least one aggregate expression")
  }

  test("mutateAggregate with mutationIndex 1 on single-key groupBy throws ShimMutationException") {
    val aggNode = findNode(singleKeyAggregated(), { case a: Aggregate => a })._1.asInstanceOf[Aggregate]
    intercept[ShimMutationException] {
      shim.mutateAggregate(aggNode, 1)
    }
  }

  test("mutateWindow with mutationIndex 0 inverts the sort order") {
    val winNode = findNode(windowed(), { case w: Window => w })._1.asInstanceOf[Window]
    val mutated = shim.mutateWindow(winNode, 0).asInstanceOf[Window]
    assert(mutated.orderSpec.map(_.direction) == winNode.orderSpec.map(_.direction).map {
      case org.apache.spark.sql.catalyst.expressions.Ascending => org.apache.spark.sql.catalyst.expressions.Descending
      case _ => org.apache.spark.sql.catalyst.expressions.Ascending
    })
  }

  test("mutateWindow with mutationIndex 1 truncates the unbounded-preceding frame") {
    val winNode = findNode(windowed(), { case w: Window => w })._1.asInstanceOf[Window]
    val mutated = shim.mutateWindow(winNode, 1).asInstanceOf[Window]
    val newLower = mutated.windowExpressions.flatMap(_.collect {
      case f: SpecifiedWindowFrame => f.lower
    })
    assert(newLower.nonEmpty)
    assert(newLower.forall(_ == UnaryMinus(org.apache.spark.sql.catalyst.expressions.Literal(1))),
      "UnboundedPreceding must become UnaryMinus(1)")
  }

  test("mutateProject with mutationIndex 0 bypasses coalesce") {
    val projNode = findNode(coalesced(), { case p: Project => p })._1.asInstanceOf[Project]
    val mutated = shim.mutateProject(projNode, 0).asInstanceOf[Project]
    assert(!mutated.projectList.exists(_.exists {
      case c: org.apache.spark.sql.catalyst.expressions.Coalesce => c.children.nonEmpty
      case _ => false
    }), "the coalesce must be replaced by its first child")
  }

  test("mutateProject with mutationIndex 1 injects null") {
    val projNode = findNode(coalesced(), { case p: Project => p })._1.asInstanceOf[Project]
    val mutated = shim.mutateProject(projNode, 1).asInstanceOf[Project]
    val nullChild = mutated.projectList.collectFirst {
      case a: org.apache.spark.sql.catalyst.expressions.Alias
        if a.child.isInstanceOf[org.apache.spark.sql.catalyst.expressions.Literal] =>
        a.child.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Literal]
    }.getOrElse(fail("expected a null literal in the mutated project list"))
    assert(nullChild.value == null)
  }
}