package pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * WP-15 example pipeline. Logic matches the WP-07 PySpark example
 * row-for-row: inner join on customer_id, filter
 * {@code amount > 100 AND status == 'COMPLETED'}, then project the four
 * report columns.
 */
public final class OrdersPipeline {

    private OrdersPipeline() {
    }

    public static Dataset<Row> buildReport(Dataset<Row> orders, Dataset<Row> customers) {
        return orders
                .join(customers, "customer_id")
                .filter(orders.col("amount").gt(100).and(orders.col("status").equalTo("COMPLETED")))
                .select("order_id", "customer_id", "amount", "status");
    }
}
