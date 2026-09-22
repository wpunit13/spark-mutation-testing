package consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/** Level 3 — window functions: ranking plus partition-wide aggregate. */
@EnableSparkMutationTesting
class WindowTest {

    @Test
    void rankedSalariesExactRows() {
        Dataset<Row> ranked = Pipelines.rankedSalaries(SparkTestSupport.employees());

        // sum(int) over window -> bigint (Long).
        List<List<Object>> expected = Arrays.asList(
                Arrays.asList("Ann", "eng", 120, 1, 220L),
                Arrays.asList("Bob", "eng", 100, 2, 220L),
                Arrays.asList("Cid", "ops", 80, 1, 80L));

        List<List<Object>> actual = new ArrayList<>();
        for (Row row : ranked.orderBy("name").collectAsList()) {
            actual.add(Arrays.asList(row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)));
        }
        assertEquals(expected, actual);
    }
}
