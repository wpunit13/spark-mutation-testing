package pipeline;

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * WP-15 weak suite. Asserts ONLY the row count via {@code collect().length} —
 * never {@code count()}, which plans an extra whole-stage Aggregate node and
 * shifts every NodeCoordinate below it (same reason the WP-07 PySpark suite
 * used {@code len(df.collect())}).
 *
 * <p>The fixture is deliberately balanced: the JOIN&rarr;ANTI and
 * FILTER&rarr;NOT(P) mutants both still produce exactly 2 rows, so this suite
 * lets them survive.
 */
@EnableSparkMutationTesting
class OrdersPipelineWeakTest {

    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("OrdersPipelineWeakTest")
                .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static Dataset<Row> orders() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, "C1", 150, "COMPLETED"));
        rows.add(RowFactory.create(2, "C2", 150, "COMPLETED"));
        rows.add(RowFactory.create(3, "C1", 150, "PENDING"));
        rows.add(RowFactory.create(4, "C2", 50, "COMPLETED"));
        rows.add(RowFactory.create(5, "C99", 150, "COMPLETED"));
        rows.add(RowFactory.create(6, "C98", 150, "COMPLETED"));

        StructType schema = new StructType()
                .add("order_id", DataTypes.IntegerType)
                .add("customer_id", DataTypes.StringType)
                .add("amount", DataTypes.IntegerType)
                .add("status", DataTypes.StringType);
        return spark.createDataFrame(rows, schema);
    }

    private static Dataset<Row> customers() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("C1", "Customer One"));
        rows.add(RowFactory.create("C2", "Customer Two"));

        StructType schema = new StructType()
                .add("customer_id", DataTypes.StringType)
                .add("customer_name", DataTypes.StringType);
        return spark.createDataFrame(rows, schema);
    }

    @Test
    void weakTestAssertsRowCountOnly() {
        Dataset<Row> report = OrdersPipeline.buildReport(orders(), customers());
        // collectAsList(), not count(): count() plans an extra Aggregate node
        // and shifts every NodeCoordinate below it. collectAsList() wraps the
        // same collect() action; the List form is required because against
        // spark-sql_2.13 the Array[T] return of collect() erases to Object for
        // javac and is not directly usable from Java.
        assertEquals(2, report.collectAsList().size());
    }
}
