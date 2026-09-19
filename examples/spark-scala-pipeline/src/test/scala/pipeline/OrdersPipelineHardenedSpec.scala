package pipeline

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * WP-15 hardened suite. Asserts exact row-set equality, so every mutant that
 * changes the rows in any way (JOIN->ANTI, FILTER->NOT(P), a nulled column,
 * ...) is killed here.
 *
 * These specs are JUnit 5 tests written in Scala (not ScalaTest) so they reuse
 * the WP-12 `mutator-junit5` extension directly.
 */
@EnableSparkMutationTesting
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrdersPipelineHardenedSpec {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("OrdersPipelineHardenedSpec")
    .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  // Java-example-style construction (see OrdersPipelineWeakSpec): Seq.toDF's
  // encoder plans register phantom PROJECT candidates the optimizer folds
  // away before the rewrite fires → NOT_APPLIED noise. Plan-for-plan parity
  // with OrdersPipelineHardenedTest.
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
  def hardenedTestAssertsExactRows(): Unit = {
    val report = OrdersPipeline.buildReport(orders, customers)

    val expected: Set[(Int, String, Int, String)] = Set(
      (1, "C1", 150, "COMPLETED"),
      (2, "C2", 150, "COMPLETED"))

    val actual: Set[(Int, String, Int, String)] =
      report.collect()
        .map(row => (row.getInt(0), row.getString(1), row.getInt(2), row.getString(3)))
        .toSet

    assertEquals(expected, actual)
  }
}
