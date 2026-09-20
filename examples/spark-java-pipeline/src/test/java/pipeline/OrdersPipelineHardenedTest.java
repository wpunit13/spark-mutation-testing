package pipeline;

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * WP-15 hardened suite. Asserts exact row-set equality, so every mutant that
 * changes the rows in any way (JOIN&rarr;ANTI, FILTER&rarr;NOT(P), a nulled
 * column, &hellip;) is killed here.
 */
class OrdersPipelineHardenedTest {

    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("OrdersPipelineHardenedTest")
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
    void hardenedTestAssertsExactRows() {
        Dataset<Row> report = OrdersPipeline.buildReport(orders(), customers());

        Set<List<Object>> expected = Set.of(
                List.of(1, "C1", 150, "COMPLETED"),
                List.of(2, "C2", 150, "COMPLETED"));

        Set<List<Object>> actual = new HashSet<>();
        // collectAsList(): see the note in OrdersPipelineWeakTest — collect()
        // erases to Object for javac under spark-sql_2.13.
        for (Row row : report.collectAsList()) {
            actual.add(Arrays.asList(row.get(0), row.get(1), row.get(2), row.get(3)));
        }

        assertEquals(expected, actual);
    }
}
