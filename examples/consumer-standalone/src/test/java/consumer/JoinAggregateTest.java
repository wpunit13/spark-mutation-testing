package consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/** Level 2 — inner join, groupBy agg, filter on aggregate. */
@EnableSparkMutationTesting
class JoinAggregateTest {

    @Test
    void totalsByCustomerExactRows() {
        Dataset<Row> report = Pipelines.totalsByCustomer(
                SparkTestSupport.orders(), SparkTestSupport.customers());

        // C1: 150+100=250 (>200, kept). C2: 50 (dropped). C3: 250 but no
        // customer row -> inner join drops it. sum(int) -> bigint (Long).
        Set<List<Object>> expected = Set.of(Arrays.asList("C1", "Alice", 250L));

        Set<List<Object>> actual = new HashSet<>();
        for (Row row : report.collectAsList()) {
            actual.add(Arrays.asList(row.get(0), row.get(1), row.get(2)));
        }
        assertEquals(expected, actual);
    }
}
