package io.github.wpunit13.mutator.reset;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionResetFacadeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("SessionResetFacadeTest")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void clearsCacheAndDropsTempViewsAndReturnsDiagnosticJson() throws Exception {
        Dataset<Row> df = spark.range(0, 10).toDF();
        df.cache();
        df.count(); // force materialization so the cache is actually populated
        df.createOrReplaceTempView("v_test");
        assertFalse(spark.sharedState().cacheManager().isEmpty(),
                "precondition: the cached plan must be registered before reset");

        String json = SessionResetFacade.resetSessionState(spark, System.currentTimeMillis());

        JsonNode node = MAPPER.readTree(json);
        assertNotNull(node.get("clearedCacheEntries"), "clearedCacheEntries key missing");
        assertNotNull(node.get("unpersistedRddCount"), "unpersistedRddCount key missing");
        assertNotNull(node.get("droppedTempViews"), "droppedTempViews key missing");

        boolean containsView = false;
        for (JsonNode name : node.get("droppedTempViews")) {
            if ("v_test".equals(name.asText())) {
                containsView = true;
            }
        }
        assertTrue(containsView, "droppedTempViews must contain v_test");

        // Cache is cleared: the cache manager reports no cached plans.
        // (spark.catalog().isCached("v_test") cannot be used here: it resolves
        // the view, which the reset has just dropped. The spec allows either
        // isCached(...) == false or "the cache manager reports empty".)
        assertTrue(spark.sharedState().cacheManager().isEmpty(),
                "cache manager must be empty after reset");

        // Temp view is gone.
        boolean stillListed = false;
        for (Table table : spark.catalog().listTables().collectAsList()) {
            if ("v_test".equals(table.name())) {
                stillListed = true;
            }
        }
        assertFalse(stillListed, "v_test must not appear in listTables after reset");
    }

    @Test
    void returnedStringParsesAsJsonWithAllThreeKeys() throws Exception {
        String json = SessionResetFacade.resetSessionState(spark, System.currentTimeMillis());
        JsonNode node = MAPPER.readTree(json);
        assertEquals(3, node.size());
        assertTrue(node.has("clearedCacheEntries"));
        assertTrue(node.has("unpersistedRddCount"));
        assertTrue(node.has("droppedTempViews"));
        assertTrue(node.get("droppedTempViews").isArray());
    }

    @Test
    void nonSparkSessionObjectThrowsRuntimeException() {
        assertThrows(RuntimeException.class,
                () -> SessionResetFacade.resetSessionState("definitely not a SparkSession", 0L));
    }
}
