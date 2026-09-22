package consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/**
 * Level 1 — extremely simple: read a DataFrame, filter ONE column, write,
 * read back, assert.
 */
@EnableSparkMutationTesting
class SimpleReadWriteTest {

    @Test
    void readJsonFilterOneColumnWriteParquetRoundtrip() {
        Path dir = SparkTestSupport.tempDir();
        try {
            Files.write(dir.resolve("in.json"), List.of(
                    "{\"order_id\":1,\"customer_id\":\"C1\",\"amount\":150}",
                    "{\"order_id\":2,\"customer_id\":\"C2\",\"amount\":50}",
                    "{\"order_id\":3,\"customer_id\":\"C1\",\"amount\":100}"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Dataset<Row> read = SparkTestSupport.spark.read().json(dir.resolve("in.json").toString());
        Dataset<Row> filtered = Pipelines.filterHighAmount(read, 100);
        String out = dir.resolve("out-parquet").toString();
        filtered.write().mode("overwrite").parquet(out);
        Dataset<Row> readBack = SparkTestSupport.spark.read().parquet(out);

        // JSON inference orders columns alphabetically; resolve by name.
        Set<List<Object>> expected = Set.of(Arrays.asList(1L, "C1", 150L));
        Set<List<Object>> actual = new HashSet<>();
        for (Row row : readBack.collectAsList()) {
            actual.add(Arrays.asList(
                    ((Number) row.getAs("order_id")).longValue(),
                    row.getAs("customer_id"),
                    ((Number) row.getAs("amount")).longValue()));
        }
        assertEquals(expected, actual);
    }

    @Test
    void createDataFrameFilterWriteJsonRoundtrip() {
        Path dir = SparkTestSupport.tempDir();
        Dataset<Row> df = SparkTestSupport.orders();
        Dataset<Row> filtered = Pipelines.filterHighAmount(df, 100);
        String out = dir.resolve("out-json").toString();
        filtered.write().mode("overwrite").json(out);
        Dataset<Row> readBack = SparkTestSupport.spark.read().json(out);

        List<Long> ids = new ArrayList<>();
        for (Row row : readBack.orderBy("order_id").collectAsList()) {
            ids.add(((Number) row.getAs("order_id")).longValue());
        }
        assertEquals(Arrays.asList(1L, 4L), ids);
        assertTrue(readBack.count() == 2);
    }
}
