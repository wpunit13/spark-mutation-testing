package pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Complex-plan suite (mirrors mutator-junit5's ComplexPlanStressTest
 * pipeline): multiple sources, USING join, window function, cache used twice,
 * parquet write-then-read (plan boundary), typed casts, aliases, and a
 * same-shape "twin" filter on a weak branch. Runs through the fork loop as
 * the complex&times;fork smoke: the hardened exact-totals assert kills the
 * pre-cache filter/window mutants; the twin sits behind a LEFT join with a
 * count-only assert and survives.
 */
class ComplexPipelineHardenedTest {

    private static SparkSession spark;
    private static String parquetPath;

    @BeforeAll
    static void setUp() throws IOException {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("ComplexPipelineHardenedTest")
                .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        parquetPath = Files.createTempDirectory("spark-mutator-cpx").resolve("branch").toString();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void complexPipelineAssertsExactTotals() {
        // Multiple sources + USING join (dedup Project) + cast drift below
        Dataset<Row> dim = srcA(spark).join(srcC(spark), "grp");

        // Window over the join; rn <= 1 is deliberately BINDING (2 rows per
        // grp) so window mutants flip the surviving set.
        Dataset<Row> w = dim.withColumn("rn",
                functions.row_number().over(Window.partitionBy("grp").orderBy("id")));

        Dataset<Row> filtered = w.filter(
                functions.col("valA").gt(10).and(functions.col("rn").leq(1)));

        // Alias-select + cache, used twice below
        Dataset<Row> cached = filtered.select("id", "grp", "valA").cache();

        // Aggregation over the cached branch — hardened: exact totals.
        Dataset<Row> agg = cached.groupBy("grp").agg(functions.sum("valA").as("total"));
        List<Row> aggRows = agg.orderBy("grp").collectAsList();
        assertEquals(2, aggRows.size());
        assertEquals("g1", aggRows.get(0).get(0));
        assertEquals(15L, aggRows.get(0).get(1));
        assertEquals("g3", aggRows.get(1).get(0));
        assertEquals(20L, aggRows.get(1).get(1));

        // Second use of the cache + parquet write-then-read (plan boundary).
        Dataset<Row> writeDf = cached.join(srcB(spark), "id").select("id", "grp", "valA", "valB");
        writeDf.write().mode("overwrite").parquet(parquetPath);
        Dataset<Row> readBack = spark.read().parquet(parquetPath);

        // Twin filter: same class as the pre-cache filter, schema-overlapping,
        // but on the weak branch behind a LEFT join with a count-only assert.
        Dataset<Row> twin = readBack.select("id", "grp", "valA")
                .filter(functions.col("valA").gt(15));
        Dataset<Row> finalDf = agg.join(twin, "grp", "left");
        assertEquals(2, finalDf.collectAsList().size());
    }

    private static Dataset<Row> srcA(SparkSession spark) {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1L, "g1", 15L));
        rows.add(RowFactory.create(2L, "g1", 25L));
        rows.add(RowFactory.create(3L, "g2", 5L));
        rows.add(RowFactory.create(4L, "g2", 35L));
        rows.add(RowFactory.create(5L, "g3", 20L));
        rows.add(RowFactory.create(6L, "g3", 8L));
        StructType schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("grp", DataTypes.StringType)
                .add("valA", DataTypes.LongType);
        return spark.createDataFrame(rows, schema);
    }

    private static Dataset<Row> srcB(SparkSession spark) {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1L, 100L));
        rows.add(RowFactory.create(4L, 400L));
        rows.add(RowFactory.create(5L, 500L));
        rows.add(RowFactory.create(9L, 900L));
        StructType schema = new StructType()
                .add("id", DataTypes.LongType)
                .add("valB", DataTypes.LongType);
        return spark.createDataFrame(rows, schema);
    }

    private static Dataset<Row> srcC(SparkSession spark) {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("g1", "R1"));
        rows.add(RowFactory.create("g2", "R2"));
        rows.add(RowFactory.create("g3", "R3"));
        StructType schema = new StructType()
                .add("grp", DataTypes.StringType)
                .add("region", DataTypes.StringType);
        return spark.createDataFrame(rows, schema);
    }
}
