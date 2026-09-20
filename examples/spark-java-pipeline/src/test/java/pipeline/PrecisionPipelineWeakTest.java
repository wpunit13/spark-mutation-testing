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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * WP-27 weak suite. Asserts the decimal amount only through a cent-level
 * tolerance — exactly the rounded-comparison pattern the
 * {@code DECIMAL_TO_DOUBLE} mutant is designed to slip past: the double cast
 * truncates the fixture's 24-significant-digit balance starting at the 3rd
 * decimal, which a 0.01 tolerance cannot see.
 *
 * <p>The report is consumed through a file boundary (write, then read back):
 * a type-changing mutant invalidates the in-memory Dataset's row encoder
 * (built from the pre-mutation schema), so a direct {@code collect} of the
 * mutated plan fails decoding (the WP-24 schema-breaking ERRORED population)
 * before any assertion can run. Reading the written report back gives the
 * suite the mutated output with a fresh schema.
 */
class PrecisionPipelineWeakTest {

    private static final String OUT = "target/precision-report-weak";

    private static SparkSession spark;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("PrecisionPipelineWeakTest")
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

    private static Dataset<Row> ledger() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, new BigDecimal("12345678901234.5678901234")));

        StructType schema = new StructType()
                .add("entry_id", DataTypes.IntegerType)
                .add("balance", DataTypes.createDecimalType(38, 10));
        // Parquet-backed fixture: a Project directly over a LocalRelation is
        // folded away by ConvertToLocalRelation before the mutation rule's
        // optimizer-phase walk sees it, which would leave DECIMAL_TO_DOUBLE
        // nothing to mutate (NOT_APPLIED). A file scan keeps the projection.
        String path = "target/precision-ledger-weak";
        spark.createDataFrame(rows, schema).repartition(1).write().mode("overwrite").parquet(path);
        return spark.read().parquet(path);
    }

    @Test
    void weakAssertsOnlyToTheCent() {
        Dataset<Row> report = PrecisionPipeline.buildReport(ledger());
        report.write().mode("overwrite").parquet(OUT);
        List<Row> rows = spark.read().parquet(OUT).collectAsList();
        assertEquals(1, rows.size());

        // Number-typed read: BigDecimal on the baseline, Double under the
        // mutant — the tolerance comparison cannot tell them apart.
        double reported = ((Number) rows.get(0).get(1)).doubleValue();
        assertEquals(new BigDecimal("12345678901234.5678901234").doubleValue(), reported, 0.01);
    }
}
