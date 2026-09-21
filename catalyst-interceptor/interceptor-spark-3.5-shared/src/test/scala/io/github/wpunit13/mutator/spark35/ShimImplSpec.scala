package io.github.wpunit13.mutator.spark35

import io.github.wpunit13.mutator.api.{NodeCoordinateFactory, OperatorType, ShimMutationException, SparkShimVersion}
import io.github.wpunit13.mutator.hash.DeterministicHasher
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  Alias, And, Ascending, Cast, Coalesce, Descending, Literal, Not, SortOrder,
  SpecifiedWindowFrame, UnaryMinus, UnboundedPreceding, WindowExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LocalRelation, Project, Window => LogicalWindow}
import org.apache.spark.sql.catalyst.plans.{Cross, LeftOuter}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, count, lit, max, min, row_number, sum}
import org.apache.spark.sql.types.{BooleanType, DoubleType, LongType, StringType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Version-agnostic mutation tests for [[Spark35ShimBase]]. This source is
 * compiled into both the `_2.12` and `_2.13` shim modules (via
 * build-helper-maven-plugin add-test-source), so the shared mutation logic is
 * exercised against each Scala binary's Spark runtime. The concrete
 * `ShimImpl`'s `supportedVersion` is supplied by the per-combination subclass, not
 * asserted here.
 */
class ShimImplSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("ShimImplSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  // Exercise the shared mutation logic directly via an anonymous subclass;
  // the real per-combination ShimImpl only differs by its supportedVersion string.
  private val shim: Spark35ShimBase = new Spark35ShimBase {
    override val supportedVersion: SparkShimVersion = SparkShimVersion("3.5", "2.13")
  }

  private def findJoin(df: DataFrame): Join =
    df.queryExecution.analyzed.collectFirst { case j: Join => j }
      .getOrElse(fail("no Join node found in analyzed plan"))

  private def findFilter(df: DataFrame): Filter =
    df.queryExecution.analyzed.collectFirst { case f: Filter => f }
      .getOrElse(fail("no Filter node found in analyzed plan"))

  private def findAggregate(df: DataFrame): Aggregate =
    df.queryExecution.analyzed.collectFirst { case a: Aggregate => a }
      .getOrElse(fail("no Aggregate node found in analyzed plan"))

  private def findWindow(df: DataFrame): LogicalWindow =
    df.queryExecution.analyzed.collectFirst { case w: LogicalWindow => w }
      .getOrElse(fail("no Window node found in analyzed plan"))

  private def findProject(df: DataFrame): Project =
    df.queryExecution.analyzed.collectFirst { case p: Project => p }
      .getOrElse(fail("no Project node found in analyzed plan"))

  private def groupedAggregated(): DataFrame = {
    import spark.implicits._
    Seq(("Engineering", "US-East", 100000L), ("Sales", "US-West", 80000L))
      .toDF("dept", "region", "salary")
      .groupBy("dept", "region")
      .agg(sum("salary"))
  }

  private def windowed(): DataFrame = {
    import spark.implicits._
    Seq(("Engineering", 100000L), ("Sales", 80000L))
      .toDF("dept", "salary")
      .select(
        col("dept"),
        col("salary"),
        row_number().over(Window.partitionBy("dept").orderBy("salary")).as("rn")
      )
  }

  private def coalesced(): DataFrame = {
    import spark.implicits._
    Seq(("Alice", 100), (null, 200))
      .toDF("name", "val")
      .select(
        coalesce(col("name"), lit("UNKNOWN")).as("c_name"),
        col("val")
      )
  }

  private def bareProject(): DataFrame = {
    import spark.implicits._
    Seq(("Alice", 100))
      .toDF("name", "val")
      .select(col("name"), col("val"))
  }

  private def decimalProjected(): DataFrame = {
    import spark.implicits._
    Seq(BigDecimal("12345678901234.5678901234"))
      .toDF("balance")
      .select(col("balance"))
  }

  private def decimalAliasProjected(): DataFrame = {
    import spark.implicits._
    Seq(BigDecimal("12345678901234.5678901234"))
      .toDF("balance")
      .select(col("balance").as("reported_balance"))
  }

  private def windowA(): DataFrame = {
    import spark.implicits._
    Seq(("Engineering", 100000L), ("Sales", 80000L))
      .toDF("dept", "salary").as("winA")
      .select(
        col("dept"),
        col("salary"),
        row_number().over(Window.partitionBy("dept").orderBy("salary")).as("rn")
      )
  }

  private def windowB(): DataFrame = {
    import spark.implicits._
    Seq(("Engineering", 100000L), ("Sales", 80000L))
      .toDF("dept", "salary").as("winB")
      .select(
        col("dept"),
        col("salary"),
        row_number().over(Window.partitionBy("dept").orderBy("salary")).as("rn")
      )
  }

  private def projectA(): DataFrame = {
    import spark.implicits._
    Seq(("Alice", 100), (null, 200))
      .toDF("name", "val").as("projA")
      .select(
        coalesce(col("name"), lit("UNKNOWN")).as("c_name"),
        col("val")
      )
  }

  private def projectB(): DataFrame = {
    import spark.implicits._
    Seq(("Alice", 100), (null, 200))
      .toDF("name", "val").as("projB")
      .select(
        coalesce(col("name"), lit("UNKNOWN")).as("c_name"),
        col("val")
      )
  }

  private def findLocalRelation(df: DataFrame): LocalRelation =
    df.queryExecution.analyzed.collectFirst { case l: LocalRelation => l }
      .getOrElse(fail("no LocalRelation node found in analyzed plan"))

  private def joined(): DataFrame = {
    import spark.implicits._
    val left = Seq((1, 10), (2, 20)).toDF("l_id", "amount")
    val right = Seq((1, "COMPLETED"), (2, "FAILED")).toDF("r_id", "status")
    left.join(right, col("l_id") === col("r_id"), "inner")
  }

  private def andFiltered(): DataFrame = {
    import spark.implicits._
    Seq(("COMPLETED", 5), ("FAILED", -1)).toDF("status", "amount")
      .filter(col("status") === "COMPLETED" && col("amount") > 0)
  }

  private def singleFiltered(): DataFrame = {
    import spark.implicits._
    Seq(("COMPLETED", 5), ("FAILED", -1)).toDF("status", "amount")
      .filter(col("status") === "COMPLETED")
  }

  // Same logical shape as chainA below: identical column names and literals,
  // but constructed independently with different relation aliases and
  // different intermediate variable names, so every exprId differs.
  private def chainA(): DataFrame = {
    import spark.implicits._
    val leftA = Seq((1, 10), (2, 20)).toDF("l_id", "amount").as("A")
    val rightA = Seq((1, "OK"), (2, "NO")).toDF("r_id", "st").as("B")
    leftA.join(rightA, col("l_id") === col("r_id"), "inner")
      .filter(col("st") === "OK" && col("amount") > 0)
  }

  private def chainB(): DataFrame = {
    import spark.implicits._
    val leftSideOfSecondBuild = Seq((1, 10), (2, 20)).toDF("l_id", "amount").as("left-alias-two")
    val rightSideOfSecondBuild = Seq((1, "OK"), (2, "NO")).toDF("r_id", "st").as("right-alias-two")
    leftSideOfSecondBuild.join(rightSideOfSecondBuild, col("l_id") === col("r_id"), "inner")
      .filter(col("st") === "OK" && col("amount") > 0)
  }

  test("classify on an INNER Join node returns Join with exactly candidates 0, 1, 2") {
    val joinNode = findJoin(joined())
    val result = shim.classify(joinNode, 2, 0)

    val (opType, candidates) = result.getOrElse(fail("expected Some for a Join node"))
    assert(opType == OperatorType.Join)
    assert(candidates.map(_.mutationIndex) == Seq(0, 1, 2))
    assert(candidates.map(_.description) == Seq("INNER -> LEFT", "INNER -> CROSS", "INNER -> ANTI"))
    assert(candidates.forall(_.operatorType == OperatorType.Join))

    // Every candidate carries the identical coordinate computed from the
    // same canonical signature used by canonicalExprSig.
    val sig = shim.canonicalExprSig(joinNode, OperatorType.Join)
    // Spark 3.5 renders the Inner join type's .sql as "INNER".
    assert(sig.startsWith("INNER;"))
    val expectedCoord = NodeCoordinateFactory(2, OperatorType.Join, 0, sig)
    assert(candidates.forall(_.coordinate == expectedCoord))
  }

  test("classify renders the node's ACTUAL join type in descriptions (LEFT/CROSS/ANTI sources)") {
    import spark.implicits._
    val left = Seq((1, 10)).toDF("l_id", "amount")
    val right = Seq((1, "OK")).toDF("r_id", "status")

    val leftJoin = findJoin(left.join(right, col("l_id") === col("r_id"), "left"))
    val (_, leftCands) = shim.classify(leftJoin, 1, 0).getOrElse(fail("expected Join candidates"))
    // Index 0 is the no-op LEFT -> LEFT and must be omitted.
    assert(leftCands.map(_.mutationIndex) == Seq(1, 2))
    assert(leftCands.map(_.description) == Seq("LEFT -> CROSS", "LEFT -> ANTI"))

    val crossJoin = findJoin(left.crossJoin(right))
    val (_, crossCands) = shim.classify(crossJoin, 1, 0).getOrElse(fail("expected Join candidates"))
    assert(crossCands.map(_.mutationIndex) == Seq(0, 2))
    assert(crossCands.map(_.description) == Seq("CROSS -> LEFT", "CROSS -> ANTI"))

    val antiJoin = findJoin(left.join(right, col("l_id") === col("r_id"), "left_anti"))
    val (_, antiCands) = shim.classify(antiJoin, 1, 0).getOrElse(fail("expected Join candidates"))
    assert(antiCands.map(_.mutationIndex) == Seq(0, 1))
    assert(antiCands.map(_.description) == Seq("ANTI -> LEFT", "ANTI -> CROSS"))
  }

  test("mutateJoin with mutationIndex 1 produces a conditionless Cross join") {
    val joinNode = findJoin(joined())
    val mutated = shim.mutateJoin(joinNode, 1).asInstanceOf[Join]
    assert(mutated.joinType == Cross)
    // INNER -> CROSS must drop the join predicate to produce a true Cartesian
    // product; retaining it would keep the join semantically identical to the
    // original Inner join and render the mutation a no-op.
    assert(mutated.condition.isEmpty)
    assert(mutated.left == joinNode.left)
    assert(mutated.right == joinNode.right)
  }

  test("mutateJoin with mutationIndex 0 produces a LeftOuter join") {
    val joinNode = findJoin(joined())
    val mutated = shim.mutateJoin(joinNode, 0).asInstanceOf[Join]
    assert(mutated.joinType == LeftOuter)
    assert(mutated.condition == joinNode.condition)
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

  test("classify on a top-level And Filter returns exactly candidates 0, 1, 2, 3") {
    val filterNode = findFilter(andFiltered())
    assert(filterNode.condition.isInstanceOf[And])

    val (opType, candidates) =
      shim.classify(filterNode, 1, 0).getOrElse(fail("expected Some for a Filter node"))
    assert(opType == OperatorType.Filter)
    assert(candidates.map(_.mutationIndex) == Seq(0, 1, 2, 3))
    assert(candidates.map(_.description) == Seq(
      "FILTER -> keep left conjunct",
      "FILTER -> keep right conjunct",
      "FILTER -> FALSE",
      "FILTER -> NOT(predicate)"))
    assert(candidates.forall(_.operatorType == OperatorType.Filter))
  }

  test("classify on a single-predicate Filter returns only candidates 2 and 3") {
    val filterNode = findFilter(singleFiltered())
    assert(!filterNode.condition.isInstanceOf[And])

    val (_, candidates) =
      shim.classify(filterNode, 1, 0).getOrElse(fail("expected Some for a Filter node"))
    assert(candidates.map(_.mutationIndex) == Seq(2, 3))
  }

  test("mutateFilter with mutationIndex 0 keeps the left conjunct") {
    val filterNode = findFilter(andFiltered())
    val original = filterNode.condition.asInstanceOf[And]
    val mutated = shim.mutateFilter(filterNode, 0).asInstanceOf[Filter]
    assert(mutated.condition == original.left)
  }

  test("mutateFilter with mutationIndex 2 forces the condition to literal false") {
    val filterNode = findFilter(andFiltered())
    val mutated = shim.mutateFilter(filterNode, 2).asInstanceOf[Filter]
    val cond = mutated.condition
    assert(cond.isInstanceOf[Literal])
    assert(cond.asInstanceOf[Literal].value == false)
    assert(cond.dataType == BooleanType)
  }

  test("mutateFilter with mutationIndex 3 negates the condition") {
    val filterNode = findFilter(andFiltered())
    val mutated = shim.mutateFilter(filterNode, 3).asInstanceOf[Filter]
    val cond = mutated.condition
    assert(cond.isInstanceOf[Not])
    assert(cond.asInstanceOf[Not].child == filterNode.condition)
  }

  test("mutateFilter with mutationIndex 0 on a non-And condition throws ShimMutationException") {
    val filterNode = findFilter(singleFiltered())
    intercept[ShimMutationException] {
      shim.mutateFilter(filterNode, 0)
    }
  }

  test("canonicalExprSig is byte-identical across independent builds with different exprIds") {
    val joinA = findJoin(chainA())
    val joinB = findJoin(chainB())
    val filterA = findFilter(chainA())
    val filterB = findFilter(chainB())

    // Precondition: the two independent builds really do carry different
    // runtime exprIds, so equality of the signatures proves non-leakage.
    assert(joinA.output.map(_.exprId.id) != joinB.output.map(_.exprId.id))

    val joinSigA = shim.canonicalExprSig(joinA, OperatorType.Join)
    val joinSigB = shim.canonicalExprSig(joinB, OperatorType.Join)
    assert(joinSigA == joinSigB,
      s"join signatures differ:\n  A = $joinSigA\n  B = $joinSigB")

    val filterSigA = shim.canonicalExprSig(filterA, OperatorType.Filter)
    val filterSigB = shim.canonicalExprSig(filterB, OperatorType.Filter)
    assert(filterSigA == filterSigB,
      s"filter signatures differ:\n  A = $filterSigA\n  B = $filterSigB")

    // No raw exprId from build B may appear in build A's signature.
    val maxBId = joinB.output.map(_.exprId.id).max
    assert(!joinSigA.contains("#" + maxBId))
  }

  test("classify returns None and canonicalExprSig returns empty for unsupported nodes") {
    val localRelation = findLocalRelation(singleFiltered())
    assert(shim.classify(localRelation, 0, -1).isEmpty)
    assert(shim.canonicalExprSig(localRelation, OperatorType.Project) == "")
    assert(shim.canonicalExprSig(localRelation, OperatorType.Other) == "")
  }

  test("classify on an Aggregate node returns Aggregate with exactly candidates 0, 1, 2") {
    val aggNode = findAggregate(groupedAggregated())
    val result = shim.classify(aggNode, 1, 0)

    val (opType, candidates) = result.getOrElse(fail("expected Some for an Aggregate node"))
    assert(opType == OperatorType.Aggregate)
    assert(candidates.map(_.mutationIndex) == Seq(0, 1, 2))
    assert(candidates.map(_.description) == Seq(
      "AGGREGATE -> swap function",
      "AGGREGATE -> drop grouping key",
      "AGGREGATE -> zero aggregate"))
    assert(candidates.forall(_.operatorType == OperatorType.Aggregate))

    val sig = shim.canonicalExprSig(aggNode, OperatorType.Aggregate)
    val expectedCoord = NodeCoordinateFactory(1, OperatorType.Aggregate, 0, sig)
    assert(candidates.forall(_.coordinate == expectedCoord))
  }

  test("classify on an Aggregate node with 1 grouping key returns only candidates 0 and 2") {
    import spark.implicits._
    val df = Seq(("dept1", 100L)).toDF("dept", "salary").groupBy("dept").agg(sum("salary"))
    val aggNode = findAggregate(df)
    val (_, candidates) = shim.classify(aggNode, 1, 0).getOrElse(fail("expected Some for an Aggregate node"))
    assert(candidates.map(_.mutationIndex) == Seq(0, 2))
  }

  test("mutateAggregate with mutationIndex 0 rewrites sum to max") {
    val aggNode = findAggregate(groupedAggregated())
    val mutated = shim.mutateAggregate(aggNode, 0).asInstanceOf[Aggregate]
    assert(!mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Sum])))
    assert(mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Max])))
  }

  test("mutateAggregate with mutationIndex 1 drops the region grouping key") {
    val aggNode = findAggregate(groupedAggregated())
    val mutated = shim.mutateAggregate(aggNode, 1).asInstanceOf[Aggregate]
    assert(mutated.groupingExpressions.length == 1)
    assert(!mutated.groupingExpressions.exists(_.sql.contains("region")))
    assert(!mutated.aggregateExpressions.exists(_.name == "region"))
    assert(mutated.aggregateExpressions.exists(_.name == "dept"))
    assert(mutated.aggregateExpressions.exists(_.name == "sum(salary)"))
  }

  test("mutateAggregate with mutationIndex 2 zeros the aggregate") {
    val aggNode = findAggregate(groupedAggregated())
    val mutated = shim.mutateAggregate(aggNode, 2).asInstanceOf[Aggregate]
    assert(!mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Sum])))
    val zeroedAlias = mutated.aggregateExpressions.collectFirst {
      case a: Alias if a.name == "sum(salary)" => a
    }.getOrElse(fail("expected sum(salary) alias in aggregate expressions"))
    assert(zeroedAlias.child.isInstanceOf[Literal])
    assert(zeroedAlias.child.asInstanceOf[Literal].value == 0L || zeroedAlias.child.asInstanceOf[Literal].value == 0)
  }

  test("mutateAggregate with mutationIndex 0 rewrites max to min") {
    import spark.implicits._
    val df = Seq(("D1", 100L)).toDF("dept", "salary").groupBy("dept").agg(max("salary"))
    val node = findAggregate(df)
    val mutated = shim.mutateAggregate(node, 0).asInstanceOf[Aggregate]
    assert(!mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Max])))
    assert(mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Min])))
  }

  test("mutateAggregate with mutationIndex 0 rewrites min to sum") {
    import spark.implicits._
    val df = Seq(("D1", 100L)).toDF("dept", "salary").groupBy("dept").agg(min("salary"))
    val node = findAggregate(df)
    val mutated = shim.mutateAggregate(node, 0).asInstanceOf[Aggregate]
    assert(!mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Min])))
    assert(mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Sum])))
  }

  test("mutateAggregate with mutationIndex 0 rewrites count to literal 1") {
    import spark.implicits._
    val df = Seq(("D1", 100L)).toDF("dept", "salary").groupBy("dept").agg(count("salary"))
    val node = findAggregate(df)
    val mutated = shim.mutateAggregate(node, 0).asInstanceOf[Aggregate]
    assert(!mutated.aggregateExpressions.exists(_.exists(_.isInstanceOf[Count])))
    val countAlias = mutated.aggregateExpressions.collectFirst {
      case a: Alias if a.name == "count(salary)" => a
    }.getOrElse(fail("expected count(salary) alias"))
    assert(countAlias.child.isInstanceOf[Literal])
    val lit = countAlias.child.asInstanceOf[Literal]
    assert(lit.value == 1L)
    // Count produces LongType; the mutation must preserve it rather than
    // silently downgrading the output column to IntegerType.
    assert(lit.dataType == LongType)
  }

  test("mutateAggregate with mutationIndex 1 on single-key groupBy throws ShimMutationException") {
    import spark.implicits._
    val df = Seq(("D1", 100L)).toDF("dept", "salary").groupBy("dept").agg(sum("salary"))
    val node = findAggregate(df)
    intercept[ShimMutationException] {
      shim.mutateAggregate(node, 1)
    }
  }

  test("mutateAggregate with an out-of-range mutationIndex throws ShimMutationException") {
    val aggNode = findAggregate(groupedAggregated())
    intercept[ShimMutationException] {
      shim.mutateAggregate(aggNode, 99)
    }
  }

  test("mutateAggregate on a non-Aggregate node throws ShimMutationException") {
    val filterNode = findFilter(andFiltered())
    intercept[ShimMutationException] {
      shim.mutateAggregate(filterNode, 0)
    }
  }

  test("canonicalExprSig for Aggregate is byte-identical across independent builds with different exprIds") {
    import spark.implicits._
    val aggA = findAggregate(
      Seq(("dept1", "region1", 100L)).toDF("dept", "region", "salary").as("A")
        .groupBy("dept", "region").agg(sum("salary"))
    )
    val aggB = findAggregate(
      Seq(("dept1", "region1", 100L)).toDF("dept", "region", "salary").as("B")
        .groupBy("dept", "region").agg(sum("salary"))
    )

    assert(aggA.output.map(_.exprId.id) != aggB.output.map(_.exprId.id))

    val sigA = shim.canonicalExprSig(aggA, OperatorType.Aggregate)
    val sigB = shim.canonicalExprSig(aggB, OperatorType.Aggregate)
    assert(sigA == sigB, s"aggregate signatures differ:\n  A = $sigA\n  B = $sigB")
  }

  test("golden cross-version: pinned canonical aggregate query yields byte-identical NodeCoordinate and MutantID") {
    // Pinned independently with `shasum -a 256` (the same "compute no expected
    // value from the implementation" discipline as prompts_execution/packets/
    // README.md's golden-hash table). Do NOT recompute these from the code.
    //
    // Canonical query: groupBy("dept", "region").agg(sum("salary"))
    //   exprSig    = #0;#1;sum(#x) AS `sum(#x)`;#0;#1
    //   coordinate = 1|AGGREGATE|0|<exprSig>
    //   mutantId   = test/path|<coordinateHex>|AGGREGATE|0
    val GOLDEN_SIG = "#0;#1;sum(#x) AS `sum(#x)`;#0;#1"
    val GOLDEN_COORDINATE = "5fa3d5da36b4095e"
    val GOLDEN_MUTANT_ID = "6896f83575625e63"

    val aggNode = findAggregate(groupedAggregated())

    // This spec is compiled into BOTH the _2.12 and _2.13 shim modules (via
    // build-helper add-test-source), so it runs against each Scala binary's own
    // Spark35ShimBase. If either binary drifts (string formatting, .sql
    // rendering, collection ordering), exactly that module's copy fails this
    // assertion — that is the cross-version guard.
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

  test("classify on Window node returns Window with candidate 0 (INVERT_WINDOW_ORDER); TRUNCATE is not offered for ranking functions") {
    val winNode = findWindow(windowed())
    val result = shim.classify(winNode, 2, 0)

    val (opType, candidates) = result.getOrElse(fail("expected Some for a Window node"))
    assert(opType == OperatorType.Window)
    // row_number() ignores the frame entirely, so TRUNCATE_WINDOW_FRAME would
    // be a guaranteed no-op — a mutant that applies, changes nothing, and is
    // reported SURVIVED (a false blind spot). Only INVERT_WINDOW_ORDER is
    // offered for ranking-function windows.
    assert(candidates.map(_.mutationIndex) == Seq(0))
    assert(candidates.map(_.description) == Seq("INVERT_WINDOW_ORDER"))
    assert(candidates.forall(_.operatorType == OperatorType.Window))

    val sig = shim.canonicalExprSig(winNode, OperatorType.Window)
    val expectedCoord = NodeCoordinateFactory(2, OperatorType.Window, 0, sig)
    assert(candidates.forall(_.coordinate == expectedCoord))
  }

  test("mutateWindow with mutationIndex 0 inverts orderBy direction and preserves schema") {
    val winNode = findWindow(windowed())
    assert(winNode.orderSpec.head.direction == Ascending)

    val mutated = shim.mutateWindow(winNode, 0).asInstanceOf[LogicalWindow]
    assert(mutated.orderSpec.head.direction == Descending)

    // WindowSpecDefinition within windowExpressions must also have its orderSpec inverted
    val nestedOrderSpec = mutated.windowExpressions.collectFirst {
      case Alias(WindowExpression(_, spec), _) => spec.orderSpec
    }.getOrElse(fail("expected WindowExpression in windowExpressions"))
    assert(nestedOrderSpec.head.direction == Descending)

    // Schema output strictly preserved
    assert(mutated.schema == winNode.schema)
    assert(mutated.output.map(_.dataType) == winNode.output.map(_.dataType))
    assert(mutated.output.map(_.name) == winNode.output.map(_.name))
  }

  test("mutateWindow with mutationIndex 1 truncates frame starting with UnboundedPreceding and preserves schema") {
    val winNode = findWindow(windowed())
    val originalFrame = winNode.windowExpressions.collectFirst {
      case Alias(WindowExpression(_, spec), _) => spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
    }.getOrElse(fail("expected SpecifiedWindowFrame in windowExpressions"))
    assert(originalFrame.lower == UnboundedPreceding)

    val mutated = shim.mutateWindow(winNode, 1).asInstanceOf[LogicalWindow]
    val mutatedFrame = mutated.windowExpressions.collectFirst {
      case Alias(WindowExpression(_, spec), _) => spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
    }.getOrElse(fail("expected SpecifiedWindowFrame in mutated windowExpressions"))

    assert(mutatedFrame.lower == UnaryMinus(Literal(1)))

    // Schema output strictly preserved
    assert(mutated.schema == winNode.schema)
    assert(mutated.output.map(_.dataType) == winNode.output.map(_.dataType))
    assert(mutated.output.map(_.name) == winNode.output.map(_.name))
  }

  test("mutateWindow with out-of-range mutationIndex or non-Window node throws ShimMutationException") {
    val winNode = findWindow(windowed())
    val filterNode = findFilter(andFiltered())

    intercept[ShimMutationException] {
      shim.mutateWindow(winNode, 99)
    }
    intercept[ShimMutationException] {
      shim.mutateWindow(filterNode, 0)
    }
  }

  test("classify on Project with Coalesce returns Project with candidates 0 and 1") {
    val projNode = findProject(coalesced())
    val result = shim.classify(projNode, 1, 0)

    val (opType, candidates) = result.getOrElse(fail("expected Some for a Project node"))
    assert(opType == OperatorType.Project)
    assert(candidates.map(_.mutationIndex) == Seq(0, 1))
    assert(candidates.map(_.description) == Seq("COALESCE_BYPASS", "INJECT_NULL"))
    assert(candidates.forall(_.operatorType == OperatorType.Project))

    val sig = shim.canonicalExprSig(projNode, OperatorType.Project)
    val expectedCoord = NodeCoordinateFactory(1, OperatorType.Project, 0, sig)
    assert(candidates.forall(_.coordinate == expectedCoord))
  }

  test("classify on Project without Coalesce returns Project with only candidate 1 (INJECT_NULL)") {
    val projNode = findProject(bareProject())
    val (_, candidates) = shim.classify(projNode, 1, 0).getOrElse(fail("expected Some for a Project node"))
    assert(candidates.map(_.mutationIndex) == Seq(1))
    assert(candidates.map(_.description) == Seq("INJECT_NULL"))
  }

  test("classify on Project with a Decimal expression adds candidate 2 (DECIMAL_TO_DOUBLE)") {
    val projNode = findProject(decimalProjected())
    val (_, candidates) = shim.classify(projNode, 1, 0).getOrElse(fail("expected Some for a Project node"))
    assert(candidates.map(_.mutationIndex) == Seq(1, 2))
    assert(candidates.map(_.description) == Seq("INJECT_NULL", "DECIMAL_TO_DOUBLE"))
    assert(candidates.forall(_.operatorType == OperatorType.Project))
  }

  test("classify on an alias-wrapped Decimal projection also yields DECIMAL_TO_DOUBLE") {
    val projNode = findProject(decimalAliasProjected())
    val (_, candidates) = shim.classify(projNode, 1, 0).getOrElse(fail("expected Some for a Project node"))
    assert(candidates.map(_.mutationIndex) == Seq(1, 2))
    assert(candidates.map(_.description) == Seq("INJECT_NULL", "DECIMAL_TO_DOUBLE"))
  }

  test("mutateProject with mutationIndex 2 casts the first Decimal projection to Double preserving identity") {
    val projNode = findProject(decimalProjected())
    val originalAttr = projNode.projectList.head

    val mutated = shim.mutateProject(projNode, 2).asInstanceOf[Project]
    val target = mutated.projectList.head
    // The whole point of the mutation: the output column's type is downgraded.
    assert(target.dataType == DoubleType)
    // Column identity stable: name and exprId survive the rewrite.
    assert(target.name == "balance")
    assert(target.exprId == originalAttr.exprId)
    val cast = target match {
      case a: Alias => a.child
      case other    => other
    }
    assert(cast.isInstanceOf[Cast])
    assert(cast.asInstanceOf[Cast].child == originalAttr)
  }

  test("mutateProject with mutationIndex 2 casts an alias-wrapped Decimal child in place") {
    val projNode = findProject(decimalAliasProjected())
    val originalAlias = projNode.projectList.collectFirst {
      case a: Alias => a
    }.getOrElse(fail("expected an Alias in projectList"))

    val mutated = shim.mutateProject(projNode, 2).asInstanceOf[Project]
    val target = mutated.projectList.collectFirst {
      case a: Alias if a.name == "reported_balance" => a
    }.getOrElse(fail("expected reported_balance alias in mutated projectList"))
    assert(target.dataType == DoubleType)
    assert(target.exprId == originalAlias.exprId)
    val cast = target.child.asInstanceOf[Cast]
    assert(cast.child == originalAlias.child)
  }

  test("mutateProject with mutationIndex 2 on a Decimal-free Project throws ShimMutationException") {
    val projNode = findProject(bareProject())
    intercept[ShimMutationException] {
      shim.mutateProject(projNode, 2)
    }
  }

  test("mutateProject with mutationIndex 0 bypasses coalesce and preserves schema") {
    val projNode = findProject(coalesced())
    assert(projNode.projectList.exists(_.exists(_.isInstanceOf[Coalesce])))

    val mutated = shim.mutateProject(projNode, 0).asInstanceOf[Project]
    assert(!mutated.projectList.exists(_.exists(_.isInstanceOf[Coalesce])))

    // Check that the coalesce alias child is now the first child (col "name")
    val cAlias = mutated.projectList.collectFirst {
      case a: Alias if a.name == "c_name" => a
    }.getOrElse(fail("expected c_name alias in projectList"))
    assert(cAlias.child.sql.contains("name"))

    // Schema output strictly preserved (column names and data types)
    assert(mutated.output.map(_.dataType) == projNode.output.map(_.dataType))
    assert(mutated.output.map(_.name) == projNode.output.map(_.name))
  }

  test("mutateProject with mutationIndex 1 replaces first non-alias expression with literal null and preserves schema") {
    val projNode = findProject(bareProject())
    val mutated = shim.mutateProject(projNode, 1).asInstanceOf[Project]

    val firstExpr = mutated.projectList.head
    assert(firstExpr.name == "name")
    assert(firstExpr.dataType == StringType)
    assert(firstExpr.isInstanceOf[Alias])
    val lit = firstExpr.asInstanceOf[Alias].child
    assert(lit.isInstanceOf[Literal])
    assert(lit.asInstanceOf[Literal].value == null)

    // Schema output strictly preserved (column names and data types)
    assert(mutated.output.map(_.dataType) == projNode.output.map(_.dataType))
    assert(mutated.output.map(_.name) == projNode.output.map(_.name))
  }

  test("mutateProject with out-of-range mutationIndex or non-Project node throws ShimMutationException") {
    val projNode = findProject(bareProject())
    val filterNode = findFilter(andFiltered())

    intercept[ShimMutationException] {
      shim.mutateProject(projNode, 99)
    }
    intercept[ShimMutationException] {
      shim.mutateProject(filterNode, 0)
    }
    intercept[ShimMutationException] {
      shim.mutateProject(projNode, 0) // No coalesce in bareProject
    }
  }

  test("canonicalExprSig for Window and Project is byte-identical across independent builds with different exprIds") {
    val winA = findWindow(windowA())
    val winB = findWindow(windowB())
    val projA = findProject(projectA())
    val projB = findProject(projectB())

    assert(winA.output.map(_.exprId.id) != winB.output.map(_.exprId.id))
    assert(projA.output.map(_.exprId.id) != projB.output.map(_.exprId.id))

    val winSigA = shim.canonicalExprSig(winA, OperatorType.Window)
    val winSigB = shim.canonicalExprSig(winB, OperatorType.Window)
    assert(winSigA == winSigB, s"window signatures differ:\n  A = $winSigA\n  B = $winSigB")

    val projSigA = shim.canonicalExprSig(projA, OperatorType.Project)
    val projSigB = shim.canonicalExprSig(projB, OperatorType.Project)
    assert(projSigA == projSigB, s"project signatures differ:\n  A = $projSigA\n  B = $projSigB")
  }

  test("golden cross-version: pinned canonical window query yields byte-identical NodeCoordinate and MutantID") {
    // Pinned independently with `shasum -a 256` (the same discipline as the
    // aggregate golden test above).
    //
    // Canonical query: row_number().over(Window.partitionBy("dept").orderBy("salary"))
    //   exprSig    = row_number() OVER (PARTITION BY #0 ORDER BY #1 ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn;#0;#1 ASC NULLS FIRST
    //   coordinate = 2|WINDOW|0|<exprSig>
    //   mutantId   = test/path|<coordinateHex>|WINDOW|0
    val GOLDEN_WIN_SIG = "row_number() OVER (PARTITION BY #0 ORDER BY #1 ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn;#0;#1 ASC NULLS FIRST"
    val GOLDEN_WIN_COORDINATE = "b580d553f8217aac"
    val GOLDEN_WIN_MUTANT_ID = "3aec48466076d976"

    val winNode = findWindow(windowed())

    val sig = shim.canonicalExprSig(winNode, OperatorType.Window)
    assert(sig == GOLDEN_WIN_SIG, s"window signature drifted:\n  got  = $sig\n  want = $GOLDEN_WIN_SIG")

    val coordinate = NodeCoordinateFactory(2, OperatorType.Window, 0, sig)
    assert(coordinate.toHex == GOLDEN_WIN_COORDINATE)

    val (_, candidates) = shim.classify(winNode, 2, 0).getOrElse(fail("expected Window candidates"))
    // row_number() ignores the frame, so TRUNCATE_WINDOW_FRAME (index 1) is
    // deliberately not offered — see the classify test above.
    assert(candidates.map(_.mutationIndex) == Seq(0))
    assert(candidates.forall(_.coordinate.toHex == GOLDEN_WIN_COORDINATE))

    val mutantId = DeterministicHasher.computeMutantId("test/path", coordinate.toHex, "WINDOW", 0)
    assert(mutantId == GOLDEN_WIN_MUTANT_ID)
  }

  test("golden cross-version: pinned canonical decimal-project query yields byte-identical NodeCoordinate and MutantID") {
    // Canonical query: select(balance) over a single Decimal column — pins the
    // DECIMAL_TO_DOUBLE discovery coordinate (Project mutationIndex 2). Pinned
    // the same discipline as the goldens above: values frozen once, never
    // recomputed from the code under test.
    val GOLDEN_SIG = "#0"
    val GOLDEN_COORDINATE = "a0caf5b87384f134"
    val GOLDEN_MUTANT_ID = "bee810271c2f53d8"

    val projNode = findProject(decimalProjected())

    val sig = shim.canonicalExprSig(projNode, OperatorType.Project)
    assert(sig == GOLDEN_SIG, s"decimal-project signature drifted:\n  got  = $sig\n  want = $GOLDEN_SIG")

    val coordinate = NodeCoordinateFactory(0, OperatorType.Project, -1, sig)
    assert(coordinate.toHex == GOLDEN_COORDINATE)

    val (_, candidates) = shim.classify(projNode, 0, -1).getOrElse(fail("expected Project candidates"))
    assert(candidates.map(_.mutationIndex) == Seq(1, 2))
    assert(candidates.forall(_.coordinate.toHex == GOLDEN_COORDINATE))

    val mutantId = DeterministicHasher.computeMutantId("test/path", coordinate.toHex, "PROJECT", 2)
    assert(mutantId == GOLDEN_MUTANT_ID)
  }
}
