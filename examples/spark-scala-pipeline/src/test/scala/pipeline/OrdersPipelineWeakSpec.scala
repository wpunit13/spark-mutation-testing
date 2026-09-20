package pipeline

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * WP-15 weak suite. Asserts ONLY the row count via `collect().length` —
 * never `count()`, which plans an extra whole-stage Aggregate node and shifts
 * every NodeCoordinate below it (same reason the WP-07 PySpark suite used
 * `len(df.collect())`).
 *
 * The fixture is deliberately balanced: the JOIN->ANTI and FILTER->NOT(P)
 * mutants both still produce exactly 2 rows, so this suite lets them survive.
 *
 * These specs are JUnit 5 tests written in Scala (not ScalaTest) so they reuse
 * the WP-12 `mutator-junit5` extension directly.
 */
@EnableSparkMutationTesting
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrdersPipelineWeakSpec {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OrdersPipelineWeakSpec")
    .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  // Java-example-style construction (RowFactory + explicit schema →
  // createDataFrame → LocalRelation leaves). Deliberately NOT Seq.toDF: the
  // encoder path plans SerializeFromObject + leaf Projects that discovery
  // registers but the optimizer folds away before the rewrite fires — 10
  // phantom PROJECT candidates classified NOT_APPLIED (java example: zero).
  // Plan-for-plan parity with OrdersPipelineWeakTest, per the pom's
  // "row-for-row" claim.
  private def orders: DataFrame = {
    val rows: java.util.List[Row] = new java.util.ArrayList[Row]()
    rows.add(RowFactory.create(1, "C1", 150, "COMPLETED"))
    rows.add(RowFactory.create(2, "C2", 150, "COMPLETED"))
    rows.add(RowFactory.create(3, "C1", 150, "PENDING"))
    rows.add(RowFactory.create(4, "C2", 50, "COMPLETED"))
    rows.add(RowFactory.create(5, "C99", 150, "COMPLETED"))
    rows.add(RowFactory.create(6, "C98", 150, "COMPLETED"))
    val schema = new StructType()
      .add("order_id", DataTypes.IntegerType)
      .add("customer_id", DataTypes.StringType)
      .add("amount", DataTypes.IntegerType)
      .add("status", DataTypes.StringType)
    spark.createDataFrame(rows, schema)
  }

  private def customers: DataFrame = {
    val rows: java.util.List[Row] = new java.util.ArrayList[Row]()
    rows.add(RowFactory.create("C1", "Customer One"))
    rows.add(RowFactory.create("C2", "Customer Two"))
    val schema = new StructType()
      .add("customer_id", DataTypes.StringType)
      .add("customer_name", DataTypes.StringType)
    spark.createDataFrame(rows, schema)
  }

  @Test
  def weakTestAssertsRowCountOnly(): Unit = {
    val report = OrdersPipeline.buildReport(orders, customers)
    assertEquals(2, report.collect().length)
  }
}
