package pipeline

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting
import org.apache.spark.sql.{DataFrame, SparkSession}
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
  def weakTestAssertsRowCountOnly(): Unit = {
    val report = OrdersPipeline.buildReport(orders, customers)
    assertEquals(2, report.collect().length)
  }
}
