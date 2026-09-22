package consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.upper;

/**
 * Downstream pipeline logic. Static, dependency-free Spark transformations —
 * the kind of code a real team ships. The mutation engine rewrites the
 * Catalyst plans these methods produce.
 */
public final class Pipelines {

    private Pipelines() {
    }

    /** Simplest possible stage: filter on one column. */
    public static Dataset<Row> filterHighAmount(Dataset<Row> orders, int threshold) {
        return orders.filter(orders.col("amount").gt(threshold));
    }

    /** Join + aggregate + filter-on-agg. */
    public static Dataset<Row> totalsByCustomer(Dataset<Row> orders, Dataset<Row> customers) {
        return orders
                .join(customers, "customer_id")
                .groupBy("customer_id", "customer_name")
                .agg(sum("amount").as("total"))
                .filter(col("total").gt(200))
                .select("customer_id", "customer_name", "total");
    }

    /** Window functions: per-partition ranking plus a partition-wide total. */
    public static Dataset<Row> rankedSalaries(Dataset<Row> employees) {
        WindowSpec byDept = Window.partitionBy("dept").orderBy(col("salary").desc());
        return employees
                .withColumn("rn", row_number().over(byDept))
                .withColumn("dept_total", sum("salary").over(Window.partitionBy("dept")))
                .select("name", "dept", "salary", "rn", "dept_total");
    }

    /**
     * Complex multi-stage pipeline: nested JSON source, explode, UDF, outer
     * join, union, aggregate-with-filter, window ranking. Returns the final
     * ranked per-user totals.
     */
    public static Dataset<Row> enrichAndRank(Dataset<Row> events, Dataset<Row> extraEvents,
                                             Dataset<Row> users) {
        Dataset<Row> flat = events
                .withColumn("tag", explode(col("tags")))
                .withColumn("src", col("meta.src").as("src"));

        Dataset<Row> extraFlat = extraEvents
                .withColumn("tag", explode(col("tags")))
                .withColumn("src", lit("batch"));

        Dataset<Row> all = flat.unionByName(extraFlat, true);

        Dataset<Row> joined = all.join(users, "user_id", "left");

        Dataset<Row> tagged = joined
                .withColumn("tag_len", callUdfTagLen(col("tag")));

        Dataset<Row> totals = tagged
                .groupBy("user_id", "tier")
                .agg(sum("amount").as("total"), count(lit(1)).as("events"))
                .filter(col("total").gt(15));

        return totals.withColumn("rn",
                row_number().over(Window.partitionBy(lit(1)).orderBy(col("total").desc())));
    }

    /** Pivot: per-user spend per tag. */
    public static Dataset<Row> spendByTag(Dataset<Row> events) {
        Dataset<Row> flat = events.withColumn("tag", explode(col("tags")));
        return flat.groupBy("user_id").pivot("tag").agg(sum("amount"));
    }

    private static org.apache.spark.sql.Column callUdfTagLen(org.apache.spark.sql.Column tag) {
        return org.apache.spark.sql.functions.callUDF("tag_len", tag);
    }
}
