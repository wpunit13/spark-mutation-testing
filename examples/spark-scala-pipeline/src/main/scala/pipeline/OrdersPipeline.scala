package pipeline

import org.apache.spark.sql.DataFrame

/**
 * WP-15 example pipeline. Logic matches the WP-07 PySpark example
 * row-for-row: inner join on customer_id, filter
 * `amount > 100 AND status == 'COMPLETED'`, then project the four report
 * columns.
 */
object OrdersPipeline {

  def buildReport(orders: DataFrame, customers: DataFrame): DataFrame =
    orders
      .join(customers, Seq("customer_id"))
      .filter(orders("amount") > 100 && orders("status") === "COMPLETED")
      .select("order_id", "customer_id", "amount", "status")
}
