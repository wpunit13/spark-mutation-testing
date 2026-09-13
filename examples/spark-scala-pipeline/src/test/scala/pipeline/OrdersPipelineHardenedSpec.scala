package pipeline

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting
import org.apache.spark.sql.{DataFrame, SparkSession}
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

  private def orders: DataFrame = {
    import spark.implicits._
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
    import spark.implicits._
    Seq(
      ("C1", "Customer One"),
      ("C2", "Customer Two")
    ).toDF("customer_id", "customer_name")
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
