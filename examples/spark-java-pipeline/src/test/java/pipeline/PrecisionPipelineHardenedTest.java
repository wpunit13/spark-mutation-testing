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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * WP-27 hardened suite. Asserts the exact decimal value, so the
 * {@code DECIMAL_TO_DOUBLE} mutant (whose Double rendering of the balance
 * differs from the 10th decimal place on) is killed here.
 *
 * <p>The report is consumed through a file boundary (write, then read back):
 * a type-changing mutant invalidates the in-memory Dataset's row encoder
 * (built from the pre-mutation schema), so a direct {@code collect} of the
 * mutated plan fails decoding (the WP-24 schema-breaking ERRORED population)
 * before any assertion can run. Reading the written report back gives the
 * suite the mutated output with a fresh schema.
 */
class PrecisionPipelineHardenedTest {

    private static SparkSession spark;
    // Per-fork scratch dir: fixed target/ paths collide when two mutation
    // workers run the suite concurrently (Hadoop _temporary staging races →
    // FileNotFound → a false KILLED for whatever mutant was active).
    private static Path runDir;

    @BeforeAll
    static void setUp() throws IOException {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("PrecisionPipelineHardenedTest")
                .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        runDir = Files.createTempDirectory("precision-hardened");
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static Dataset<Row> ledger() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, new java.math.BigDecimal("12345678901234.5678901234")));

        StructType schema = new StructType()
                .add("entry_id", DataTypes.IntegerType)
                .add("balance", DataTypes.createDecimalType(38, 10));
        // Parquet-backed fixture: see PrecisionPipelineWeakTest.ledger().
        String path = runDir.resolve("ledger").toString();
        spark.createDataFrame(rows, schema).repartition(1).write().mode("overwrite").parquet(path);
        return spark.read().parquet(path);
    }

    @Test
    void hardenedAssertsExactDecimalValue() {
        Dataset<Row> report = PrecisionPipeline.buildReport(ledger());
        String out = runDir.resolve("report").toString();
        report.write().mode("overwrite").parquet(out);
        List<Row> rows = spark.read().parquet(out).collectAsList();
        assertEquals(1, rows.size());

        // Exact-value assertion: BigDecimal.toString() on the baseline is the
        // full 24-significant-digit value; the mutant's Double.toString() is
        // not, so the mutant cannot survive this assertion.
        assertEquals("12345678901234.5678901234", rows.get(0).get(1).toString());
    }
}
