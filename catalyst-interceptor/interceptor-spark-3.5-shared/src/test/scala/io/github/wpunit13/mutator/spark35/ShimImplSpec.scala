package io.github.wpunit13.mutator.spark35

import io.github.wpunit13.mutator.api.{NodeCoordinateFactory, OperatorType, ShimMutationException, SparkShimVersion}
import io.github.wpunit13.mutator.hash.DeterministicHasher
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Literal, Not}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LocalRelation}
import org.apache.spark.sql.catalyst.plans.{Cross, LeftOuter}
import org.apache.spark.sql.functions.{col, count, max, min, sum}
import org.apache.spark.sql.types.{BooleanType, LongType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Version-agnostic mutation tests for [[Spark35ShimBase]]. This source is
 * compiled into both the `_2.12` and `_2.13` shim modules (via
 * build-helper-maven-plugin add-test-source), so the shared mutation logic is
 * exercised against each Scala binary's Spark runtime. The concrete
 * `ShimImpl`'s `supportedVersion` is supplied by the per-cell subclass, not
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
  // the real per-cell ShimImpl only differs by its supportedVersion string.
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

  private def groupedAggregated(): DataFrame = {
    import spark.implicits._
    Seq(("Engineering", "US-East", 100000L), ("Sales", "US-West", 80000L))
      .toDF("dept", "region", "salary")
      .groupBy("dept", "region")
      .agg(sum("salary"))
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

  test("mutateWindow and mutateProject are not implemented in this release") {
    val joinNode = findJoin(joined())
    intercept[UnsupportedOperationException](shim.mutateWindow(joinNode, 0))
    intercept[UnsupportedOperationException](shim.mutateProject(joinNode, 0))
  }
}
