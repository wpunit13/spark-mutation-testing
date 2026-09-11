package io.github.wpunit13.mutator.spark35_212

import io.github.wpunit13.mutator.api.{NodeCoordinateFactory, OperatorType, ShimMutationException, SparkShimVersion}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LocalRelation}
import org.apache.spark.sql.catalyst.plans.{Cross, LeftOuter}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.BooleanType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ShimImplSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("ShimImplSpec212")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  private val shim = new ShimImpl

  private def findJoin(df: DataFrame): Join =
    df.queryExecution.analyzed.collectFirst { case j: Join => j }
      .getOrElse(fail("no Join node found in analyzed plan"))

  private def findFilter(df: DataFrame): Filter =
    df.queryExecution.analyzed.collectFirst { case f: Filter => f }
      .getOrElse(fail("no Filter node found in analyzed plan"))

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

  test("supportedVersion identifies the Spark 3.5 / Scala 2.12 cell") {
    assert(shim.supportedVersion == SparkShimVersion("3.5", "2.12"))
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

  test("golden NodeCoordinate hashes match standard test queries identically to Scala 2.13") {
    // 1. Pinned specification golden fixture (ARCHITECTURE.md / README.md)
    val pinnedCoord = NodeCoordinateFactory(2, OperatorType.Join, 0, "Inner;(customer_id#0 = customer_id#1)")
    assert(pinnedCoord.toHex == "058a342dfabb7500")

    // 2. Standard joined query
    val joinNode = findJoin(joined())
    val joinSig = shim.canonicalExprSig(joinNode, OperatorType.Join)
    assert(joinSig == "INNER;(#0 = #2)")
    val joinCoord = NodeCoordinateFactory(2, OperatorType.Join, 0, joinSig)
    assert(joinCoord.toHex == "e9e480cd939740bc")

    // 3. Standard AND-filtered query
    val filterNode = findFilter(andFiltered())
    val filterSig = shim.canonicalExprSig(filterNode, OperatorType.Filter)
    assert(filterSig == "((#0 = 'COMPLETED') AND (#1 > 0))")
    val filterCoord = NodeCoordinateFactory(1, OperatorType.Filter, 0, filterSig)
    assert(filterCoord.toHex == "b8cdfda2300bd6af")

    // 4. Standard single-filtered query
    val singleNode = findFilter(singleFiltered())
    val singleSig = shim.canonicalExprSig(singleNode, OperatorType.Filter)
    assert(singleSig == "(#0 = 'COMPLETED')")
    val singleCoord = NodeCoordinateFactory(1, OperatorType.Filter, 0, singleSig)
    assert(singleCoord.toHex == "38559c33e748304c")
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

  test("mutateAggregate, mutateWindow and mutateProject are not implemented in this release") {
    val joinNode = findJoin(joined())
    intercept[UnsupportedOperationException](shim.mutateAggregate(joinNode, 0))
    intercept[UnsupportedOperationException](shim.mutateWindow(joinNode, 0))
    intercept[UnsupportedOperationException](shim.mutateProject(joinNode, 0))
  }
}
