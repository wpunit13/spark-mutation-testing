package consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Level 4 — very complex: nested JSON schema (struct + array), explode,
 * registered UDF, LEFT join, unionByName, aggregate + filter, window ranking,
 * pivot, cache, partitioned parquet write/read roundtrip.
 */
@EnableSparkMutationTesting
class ComplexPipelineTest {

    private static final String EVENTS_JSON =
            "{\"event_id\":1,\"user_id\":\"u1\",\"amount\":10,\"tags\":[\"a\",\"b\"],\"meta\":{\"src\":\"web\",\"v\":1}}\n"
            + "{\"event_id\":2,\"user_id\":\"u2\",\"amount\":20,\"tags\":[\"b\"],\"meta\":{\"src\":\"app\",\"v\":2}}\n"
            + "{\"event_id\":3,\"user_id\":\"u1\",\"amount\":30,\"tags\":[\"a\"],\"meta\":{\"src\":\"web\",\"v\":1}}\n"
            + "{\"event_id\":4,\"user_id\":\"u3\",\"amount\":40,\"tags\":[\"c\"],\"meta\":{\"src\":\"web\",\"v\":3}}\n";

    private static Dataset<Row> events() {
        Path dir = SparkTestSupport.tempDir();
        try {
            Files.write(dir.resolve("events.json"), List.of(EVENTS_JSON));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return SparkTestSupport.spark.read().json(dir.resolve("events.json").toString());
    }

    private static Dataset<Row> extraEvents() {
        return SparkTestSupport.spark.createDataFrame(Arrays.asList(
                        RowFactory.create(5, "u2", 25)),
                new StructType()
                        .add("event_id", DataTypes.IntegerType)
                        .add("user_id", DataTypes.StringType)
                        .add("amount", DataTypes.IntegerType))
                .withColumn("tags", functions.array(functions.lit("b")));
    }

    @Test
    void enrichAndRankExactRows() {
        SparkTestSupport.spark.udf().register("tag_len",
                (String s) -> s == null ? -1 : s.length(), DataTypes.IntegerType);

        Dataset<Row> ranked = Pipelines.enrichAndRank(
                events(), extraEvents(), SparkTestSupport.users());

        // u1 GOLD: 10+10+30=50 (3 exploded events). u2 SILVER: 20+25=45 (2).
        // u3 (no user row, LEFT join keeps, tier null): 40 (1).
        // sum/count -> bigint (Long).
        Set<List<Object>> expected = Set.of(
                Arrays.asList("u1", "GOLD", 50L, 3L, 1),
                Arrays.asList("u2", "SILVER", 45L, 2L, 2),
                Arrays.asList("u3", null, 40L, 1L, 3));

        Set<List<Object>> actual = new HashSet<>();
        for (Row row : ranked.collectAsList()) {
            actual.add(Arrays.asList(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)));
        }
        assertEquals(expected, actual);
    }

    @Test
    void pivotSpendByTag() {
        Dataset<Row> pivoted = Pipelines.spendByTag(events());

        // Column order from pivot is unspecified -> resolve indices by name.
        Map<String, Integer> idx = new HashMap<>();
        for (String f : pivoted.schema().fieldNames()) {
            idx.put(f, pivoted.schema().fieldIndex(f));
        }
        assertTrue(idx.containsKey("a") && idx.containsKey("b") && idx.containsKey("c"));

        Map<String, List<Object>> byUser = new HashMap<>();
        for (Row row : pivoted.collectAsList()) {
            byUser.put((String) row.get(0), Arrays.asList(
                    row.get(idx.get("a")), row.get(idx.get("b")), row.get(idx.get("c"))));
        }
        Map<String, List<Object>> expected = new HashMap<>();
        expected.put("u1", Arrays.asList(40L, 10L, null));
        expected.put("u2", Arrays.asList(null, 20L, null));
        expected.put("u3", Arrays.asList(null, null, 40L));
        assertEquals(expected, byUser);
    }

    @Test
    void partitionedParquetRoundtrip() {
        SparkTestSupport.spark.udf().register("tag_len",
                (String s) -> s == null ? -1 : s.length(), DataTypes.IntegerType);

        Dataset<Row> ranked = Pipelines.enrichAndRank(
                events(), extraEvents(), SparkTestSupport.users());
        ranked.cache();
        assertEquals(3, ranked.count());

        Path dir = SparkTestSupport.tempDir();
        String out = dir.resolve("ranked-parquet").toString();
        ranked.write().mode("overwrite").partitionBy("user_id").parquet(out);
        Dataset<Row> readBack = SparkTestSupport.spark.read().parquet(out);

        Map<String, Object> totals = new HashMap<>();
        for (Row row : readBack.collectAsList()) {
            totals.put((String) row.getAs("user_id"), row.getAs("total"));
        }
        Map<String, Object> expected = new HashMap<>();
        expected.put("u1", 50L);
        expected.put("u2", 45L);
        expected.put("u3", 40L);
        assertEquals(expected, totals);
    }
}
