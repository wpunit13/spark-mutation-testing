package io.github.wpunit13.mutator.reset;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs the full canonical reset sequence against the given SparkSession,
 * EXCLUDING the final MutantRegistry.clearActiveMutant step (callers invoke
 * that separately via MutantRegistry so the two systems remain independently
 * testable).
 *
 * <p>Every Spark interaction goes through java.lang.reflect: this class lives
 * in mutator-core, which must keep zero compile-time Spark dependencies.
 */
public final class SessionResetFacade {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SessionResetFacade() {
    }

    /**
     * Runs the reset sequence in the canonical order:
     * 1. spark.catalog().clearCache()                     (checklist #1)
     * 2. sparkContext.getPersistentRDDs unpersist(true)   (checklist #2)
     * 3. spark.sessionState().catalog().invalidateAll()   (checklist #3)
     * 4. drop temporary views                             (checklist #4)
     *
     * @param spark the active org.apache.spark.sql.SparkSession, passed
     *              across Py4J as the existing bridged Java object the Python
     *              `SparkSession._jsparkSession` already exposes.
     * @param sinceTimestampMillis accepted for contract compatibility but
     *              currently unused: Spark's public catalog API exposes no
     *              view-creation timestamp, so the parameter cannot be
     *              honored. Dropping ALL temp views is intentionally more
     *              conservative than time-scoping and is safe because
     *              re-registration is idempotent for well-behaved pipelines
     *              under test.
     * @return JSON string: {"clearedCacheEntries": &lt;int&gt;,
     *              "unpersistedRddCount": &lt;int&gt;, "droppedTempViews": [&lt;string&gt;, ...]}
     *              -- diagnostic only, not used for control flow.
     * @throws RuntimeException wrapping any underlying Spark exception;
     *              caller classifies as ERRORED.
     */
    public static String resetSessionState(Object spark, long sinceTimestampMillis) {
        try {
            // Step 1: clear the cache manager. Spark's clearCache() returns
            // void and gives no count, so clearedCacheEntries is deliberately
            // reported as -1 rather than a fabricated number.
            Object catalog = invoke(spark, "catalog");
            invoke(catalog, "clearCache");

            // Step 2: safety net for raw persisted RDDs registered directly
            // with the SparkContext, which cacheManager.clearCache() does not
            // track. getPersistentRDDs returns a Scala collection.Map whose
            // values/iterator must be traversed reflectively. If that Scala
            // collection traversal fails on some future Spark version, we
            // record unpersistedRddCount as -1 and continue with the
            // remaining steps instead of aborting the whole reset.
            int unpersistedRddCount = 0;
            try {
                unpersistedRddCount = unpersistPersistentRdds(spark);
            } catch (ReflectiveOperationException | RuntimeException reflectionFailure) {
                unpersistedRddCount = -1;
            }

            // Step 3: invalidate session catalog metadata so stale resolved
            // CatalogTable/file-index fragments cannot be reused.
            // TODO(spec-gap): the checklist names "spark.sessionState.catalog.invalidateAll()",
            //  but Spark 3.5.3's SessionCatalog has no such method; the same
            //  metadata-cache invalidation is exposed as invalidateAllCachedTables().
            //  We try the documented name first and fall back to the 3.5 name.
            Object sessionState = invoke(spark, "sessionState");
            Object sessionCatalog = invoke(sessionState, "catalog");
            try {
                invoke(sessionCatalog, "invalidateAll");
            } catch (NoSuchMethodException renamedInSpark35) {
                invoke(sessionCatalog, "invalidateAllCachedTables");
            }

            // Step 4: snapshot temp views and drop all temporary ones.
            List<String> droppedTempViews = dropTempViews(spark);

            ObjectNode json = MAPPER.createObjectNode();
            json.put("clearedCacheEntries", -1);
            json.put("unpersistedRddCount", unpersistedRddCount);
            ArrayNode dropped = json.putArray("droppedTempViews");
            droppedTempViews.forEach(dropped::add);
            return MAPPER.writeValueAsString(json);
        } catch (Exception e) {
            throw new RuntimeException("Session reset sequence failed", e);
        }
    }

    /**
     * Reflectively walks sparkContext.getPersistentRDDs (a Scala
     * scala.collection.Map), unpersisting each value with blocking = true.
     */
    private static int unpersistPersistentRdds(Object spark) throws ReflectiveOperationException {
        Object sparkContext = invoke(spark, "sparkContext");
        Object persistentRdds = invoke(sparkContext, "getPersistentRDDs");
        Object values = invoke(persistentRdds, "values");
        Object iterator = invoke(values, "iterator");
        java.util.Iterator<?> it = (java.util.Iterator<?>) iterator;
        int count = 0;
        while (it.hasNext()) {
            Object rdd = it.next();
            rdd.getClass().getMethod("unpersist", boolean.class).invoke(rdd, true);
            count++;
        }
        return count;
    }

    /**
     * Reflectively lists the session catalog tables and drops every
     * temporary view. All temp views are dropped unconditionally (see the
     * sinceTimestampMillis note above).
     */
    private static List<String> dropTempViews(Object spark) throws ReflectiveOperationException {
        List<String> dropped = new ArrayList<>();
        Object catalog = invoke(spark, "catalog");
        Object tables = invoke(catalog, "listTables");
        java.util.List<?> rows = (java.util.List<?>) invoke(tables, "collectAsList");
        for (Object row : rows) {
            boolean isTemporary = (Boolean) invoke(row, "isTemporary");
            if (isTemporary) {
                String name = (String) invoke(row, "name");
                invoke(catalog, "dropTempView", new Class<?>[] {String.class}, name);
                dropped.add(name);
            }
        }
        return dropped;
    }

    private static Object invoke(Object target, String method) throws ReflectiveOperationException {
        return target.getClass().getMethod(method).invoke(target);
    }

    private static Object invoke(Object target, String method, Class<?>[] parameterTypes, Object... args)
            throws ReflectiveOperationException {
        return target.getClass().getMethod(method, parameterTypes).invoke(target, args);
    }
}
