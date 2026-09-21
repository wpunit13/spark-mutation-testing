package io.github.wpunit13.mutator.junit5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.CatalystMutationRule;
import io.github.wpunit13.mutator.MutantRegistry;
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess;
import io.github.wpunit13.mutator.dispatch.ShimDispatcher;
import io.github.wpunit13.mutator.report.ReportSink;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

/**
 * Complex-plan stress test: the mutation loop against a plan with everything
 * that makes Catalyst replan between the PostHoc match and the Optimizer
 * rewrite — multiple sources, USING join, window function, cache used twice,
 * parquet write-then-read (a plan boundary), typed-column casts (drift via
 * SimplifyCasts), aliases (drift via CollapseProject), and two same-shape
 * "twin" filters whose downstream assertions differ so that a fallback
 * misattribution flips an observable status.
 *
 * <p>Two fixtures share one pipeline:
 *
 * <ul>
 *   <li>{@link ComplexPipelineCase} — default session (AQE is default-on in
 *       Spark 3.2+; not forced here).</li>
 *   <li>{@link AqeComplexPipelineCase} — AQE explicitly pinned ON, broadcast
 *       joins DISABLED (forcing sort-merge joins → shuffle query stages →
 *       runtime SMJ→BHJ conversion and partition coalescing), and the
 *       Optimizer-phase rule ADDITIONALLY injected via
 *       {@code injectRuntimeOptimizerRule} — so the rule re-fires during AQE
 *       re-optimization rounds on rebuilt logical plans. This is the
 *       aggressive scenario: the consumed-pending one-shot semantics must
 *       hold, or double-application flips statuses and the parity check
 *       fails.</li>
 * </ul>
 *
 * <p>Pipeline (per mutant re-run, fresh session):
 *
 * <pre>
 * srcA(6) ⋈ srcC(3) → +window rn → filter#1 (valA&gt;10 AND rn≤1, cast drift)
 *   → select → CACHE ─┬─ agg (sum by grp)          → HARDENED exact-rows assert
 *                     └─ ⋈ srcB → parquet WRITE → READ back → twin filter
 *                          (valA&gt;15, schema-overlapping) → LEFT join → WEAK count assert
 * </pre>
 *
 * <p>Attribution instrument: filter#1 sits on the hardened branch — rn ≤ 1 is
 * deliberately BINDING (2 rows per grp), so every filter#1 mutant (FALSE / NOT /
 * keep-left / keep-right) changes the cached rows and must KILL via the
 * exact-totals assert; the same bound makes the WINDOW family observable
 * (INVERT_WINDOW_ORDER swaps which row gets rn=1 → KILLED; frame truncation on
 * a frameless row_number → NOT_APPLIED/SURVIVED). The twin sits on the weak
 * branch behind a LEFT join (its mutants must SURVIVE — the count is
 * invariant) and provides the SURVIVED contrast. If the identity fallback
 * re-identifies a twin mutant onto filter#1 (or vice versa), one of these
 * statuses flips and the test fails.
 */
class ComplexPlanStressTest {

    @TempDir
    Path tempDir;

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void resetState() {
        resetEngineState();
    }

    @AfterEach
    void cleanupState() {
        resetEngineState();
    }

    private void resetEngineState() {
        MutantRegistry.getInstance().reset();
        MutationCatalogAccess.clearForTesting();
        ReportSink.clearForTesting();
        System.clearProperty(SparkMutatorExtension.PROPERTY_ACTIVE_MUTANT);
        System.clearProperty(SparkMutatorExtension.PROPERTY_MUTATOR_DISABLED);
        System.clearProperty(SparkMutatorExtension.PROPERTY_MUTATOR_ENABLED);
        System.clearProperty("spark.mutator.outputDirectory");
    }

    // ------------------------------------------------------------------
    // Test 1: default session — classification + branch attribution
    // ------------------------------------------------------------------

    @Test
    void complexPlanClassifiesWithoutErrorsAndAttributesPerBranch() throws Exception {
        JsonNode report = runFixture(ComplexPipelineCase.class, tempDir.resolve("main"));

        assertReportInvariants(report, "main");

        // Branch attribution via diff-snippet markers:
        //  - filter#1 family: subtree contains the Window ("row_number"), not the parquet read
        //  - twin family:     subtree contains the parquet Relation node, not the window
        int filter1Killed = 0;
        int filter1Survived = 0;
        int twinSurvived = 0;
        List<String> twinViolations = new ArrayList<>();
        for (JsonNode m : report.get("mutants")) {
            if (!m.get("operatorType").asText().equals("FILTER")) {
                continue;
            }
            String snippet = m.get("astDiffSnippet").asText("");
            boolean cacheSide = snippet.contains("row_number");
            boolean parquetSide = snippet.contains("parquet");
            String status = m.get("result").get("status").asText();
            if (cacheSide && !parquetSide) {
                if (status.equals("KILLED")) {
                    filter1Killed++;
                } else if (status.equals("SURVIVED")) {
                    filter1Survived++;
                }
            } else if (parquetSide && !cacheSide) {
                if (status.equals("SURVIVED")) {
                    twinSurvived++;
                } else {
                    twinViolations.add(m.get("description").asText() + " -> " + status);
                }
            }
        }

        // Hardened pre-cache filter family: the FALSE / NOT / keep-right mutants
        // change the cached rows and must be killed by the exact-totals assert.
        assertTrue(filter1Killed >= 1,
                "Pre-cache filter mutants must be killed by the hardened branch");

        // Twin mutants sit behind a LEFT join with a count-only assert: they
        // must survive. A killed twin means the fallback re-identified a twin
        // mutant onto the hardened branch (misattribution).
        assertTrue(twinSurvived >= 1, "Twin filter mutants must be discovered and survive");
        assertEquals(0, twinViolations.size(),
                "Twin filter mutants must NOT be killed — that is fallback misattribution "
                        + "onto the hardened branch:\n" + twinViolations);
    }

    // ------------------------------------------------------------------
    // Test 2: AQE parity — AQE-on (with the rule re-injected into AQE's
    // runtime re-optimizer) must classify identically to the default session
    // ------------------------------------------------------------------

    @Test
    void aqeClassificationParityWithDefaultSession() throws Exception {
        JsonNode defaultReport = runFixture(ComplexPipelineCase.class, tempDir.resolve("aqe-off"));
        assertReportInvariants(defaultReport, "aqe-off");

        JsonNode aqeReport = runFixture(AqeComplexPipelineCase.class, tempDir.resolve("aqe-on"));
        assertReportInvariants(aqeReport, "aqe-on");

        // Parity: the same pipeline under AQE (with runtime re-optimization and
        // the rule re-fired on rebuilt plans). AQE's replans expose application
        // opportunities the default session's single initial optimization does
        // not have, so the AQE leg may legitimately classify MORE mutants as
        // KILLED (stronger verdicts). What parity must still guarantee:
        //   - AQE must never WEAKEN a verdict the default session recorded
        //     (default KILLED/TIMED_OUT must stay KILLED/TIMED_OUT under AQE —
        //     a flip to SURVIVED/NOT_APPLIED/SKIPPED means AQE re-planning
        //     lost an application or misattributed it);
        //   - no ERRORED anywhere (a crash in either leg is a harness/engine
        //     failure, never an acceptable classification);
        //   - every default-session mutant must exist in the AQE run.
        // (Historical note: before the per-class ReportSink reset this
        // assertion passed vacuously — the write-once outcome sink made the
        // AQE leg's report mirror the default leg's statuses.)
        Map<String, String> defaultStatuses = statusMap(defaultReport);
        Map<String, String> aqeStatuses = statusMap(aqeReport);

        java.util.Set<String> strong = java.util.Set.of("KILLED", "TIMED_OUT");
        java.util.Set<String> weak = java.util.Set.of("SURVIVED", "NOT_APPLIED", "SKIPPED");

        List<String> diffs = new ArrayList<>();
        for (String id : defaultStatuses.keySet()) {
            String other = aqeStatuses.get(id);
            if (other == null) {
                diffs.add(id + ": missing in AQE run");
            } else if (strong.contains(defaultStatuses.get(id)) && weak.contains(other)) {
                diffs.add(id + ": " + defaultStatuses.get(id) + " (default) weakened to "
                        + other + " (AQE)");
            }
        }
        for (String id : aqeStatuses.keySet()) {
            String status = aqeStatuses.get(id);
            if (status.equals("ERRORED")) {
                diffs.add(id + ": ERRORED in AQE run");
            }
        }
        for (String id : defaultStatuses.keySet()) {
            if (defaultStatuses.get(id).equals("ERRORED")) {
                diffs.add(id + ": ERRORED in default run");
            }
        }
        assertEquals(0, diffs.size(),
                "AQE must not weaken mutation classification or crash:\n" + diffs);
    }

    // ------------------------------------------------------------------
    // Fixture: the complex pipeline, driven per mutant by the loop
    // ------------------------------------------------------------------

    @EnableSparkMutationTesting
    static class ComplexPipelineCase {

        static SparkSession spark;
        static String parquetPath;

        @BeforeAll
        static void setUp() throws IOException {
            spark = baseSessionBuilder()
                    .appName("ComplexPlanStress")
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
        void complexPipeline() {
            runPipeline(spark, parquetPath);
        }
    }

    /**
     * Same pipeline under an explicitly AQE-forced session: broadcast joins
     * disabled (sort-merge joins → shuffle query stages → runtime SMJ→BHJ
     * conversion), partition coalescing and skew handling on, and the
     * Optimizer-phase rule re-injected through {@code injectRuntimeOptimizerRule}
     * so it re-fires during every AQE re-optimization round on rebuilt plans.
     */
    @EnableSparkMutationTesting
    static class AqeComplexPipelineCase {

        static SparkSession spark;
        static String parquetPath;

        @BeforeAll
        static void setUp() throws IOException {
            spark = baseSessionBuilder()
                    .appName("AqeComplexPlanStress")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.adaptive.skewJoin.enabled", "true")
                    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                    .config("spark.sql.shuffle.partitions", "4")
                    .withExtensions(extensions -> {
                        extensions.injectRuntimeOptimizerRule(
                                sparkSession -> new CatalystMutationRule(
                                        ShimDispatcher.activeShim(),
                                        CatalystMutationRule.Optimizer$.MODULE$));
                        return scala.runtime.BoxedUnit.UNIT;
                    })
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
        void complexPipelineUnderAqe() {
            runPipeline(spark, parquetPath);
        }
    }

    private static SparkSession.Builder baseSessionBuilder() {
        return SparkSession.builder()
                .master("local[2]")
                .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "false");
    }

    /**
     * The shared complex pipeline. Unmutated expectations: agg = [(g1,15),
     * (g3,20)] (rn ≤ 1 keeps only each grp's lowest-id valA>10 row); the
     * write-side inner join keeps ids 1, 5; the weak left-join count is 2.
     */
    private static void runPipeline(SparkSession spark, String parquetPath) {
        // Multiple sources + USING join (dedup Project) + cast drift below
        Dataset<Row> dim = srcA(spark).join(srcC(spark), "grp");

        // Window over the join
        Dataset<Row> w = dim.withColumn("rn",
                functions.row_number().over(Window.partitionBy("grp").orderBy("id")));

        // AND filter → keep-left / keep-right / FALSE / NOT candidates.
        // rn ≤ 1 is deliberately BINDING (2 rows per grp): it makes window
        // mutations observable — INVERT_WINDOW_ORDER swaps which row gets
        // rn=1 and flips the surviving set, giving the WINDOW family kill
        // power on this plan (rn ≤ 3 never bound → designed SURVIVED only).
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

        // Twin filter: same class as filter#1, schema-overlapping, but on the
        // weak branch behind a LEFT join with a count-only assert.
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

    // ------------------------------------------------------------------
    // Report loading + shared invariant checks
    // ------------------------------------------------------------------

    private JsonNode runFixture(Class<?> fixtureClass, Path outputDir) {
        // The stress fixture legitimately produces crash-class ERRORED
        // (schema-breaking mutants) and designed not-applied (cache-hidden
        // shapes) — disable the WP-24 population gates for the run; the
        // assertions below pin the populations directly.
        System.setProperty("spark.mutator.maxErroredCount", "-1");
        System.setProperty("spark.mutator.maxNotAppliedRatio", "-1");
        System.setProperty("spark.mutator.outputDirectory", outputDir.toAbsolutePath().toString());
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(fixtureClass))
                .build();
        LauncherFactory.create().execute(request);

        Path reportFile = outputDir.resolve("mutation-report.json");
        assertTrue(Files.exists(reportFile),
                "mutation-report.json should be written to " + outputDir);
        try {
            return mapper.readTree(reportFile.toFile());
        } catch (IOException e) {
            throw new IllegalStateException("Could not read " + reportFile, e);
        }
    }

    private void assertReportInvariants(JsonNode report, String leg) {
        JsonNode summary = report.get("summary");
        int total = summary.get("totalMutants").asInt();
        int killed = summary.get("killed").asInt();
        int survived = summary.get("survived").asInt();

        StringBuilder dump = new StringBuilder();
        for (JsonNode m : report.get("mutants")) {
            dump.append(m.get("operatorType").asText()).append(' ')
                    .append(m.get("description").asText()).append(" -> ")
                    .append(m.get("result").get("status").asText())
                    .append(" detail=").append(m.get("result").get("failureDetailOrNull"))
                    .append('\n');
        }

        assertTrue(total > 0, "[" + leg + "] Expected mutants to be discovered");

        // Three legitimate ERRORED classes on a complex plan:
        //  1. designed not-applied guard (node hidden inside an
        //     InMemoryRelation / eliminated before execution),
        //  2. schema-breaking mutation crashes the pipeline (e.g. INNER→ANTI
        //     on a join whose right columns are consumed downstream),
        //  3. any driver-level mutation crash.
        // What is NEVER acceptable: dead sessions (lifecycle regression) or
        // ShimMutationException (fallback applied a mutation index to a node
        // whose shape cannot support it).
        List<String> forbiddenErrors = new ArrayList<>();
        for (JsonNode m : report.get("mutants")) {
            if (!m.get("result").get("status").asText().equals("ERRORED")) {
                continue;
            }
            String detail = m.get("result").get("failureDetailOrNull").asText("");
            if (detail.contains("stopped SparkContext") || detail.contains("ShimMutationException")) {
                forbiddenErrors.add(m.get("operatorType").asText() + " "
                        + m.get("description").asText() + ": " + detail);
            }
        }
        assertEquals(0, forbiddenErrors.size(),
                "[" + leg + "] ERRORED must never be a dead session or a shim shape violation:\n"
                        + forbiddenErrors);

        assertTrue(killed > 0, "[" + leg + "] Hardened branch must kill mutants:\n" + dump);
        assertTrue(survived > 0, "[" + leg + "] Weak branch must let mutants survive:\n" + dump);

        assertFamilyCoverage(report, leg);

        // Every applied mutant carries attribution evidence.
        for (JsonNode m : report.get("mutants")) {
            if (!m.get("result").get("status").asText().equals("ERRORED")) {
                assertNotNull(m.get("astDiffSnippet"),
                        "[" + leg + "] Applied mutant " + m.get("mutantId").asText()
                                + " must carry a diff snippet");
            }
        }
    }

    /**
     * Family-level pins: every mutator family must be discovered AND
     * classified on the complex plan — not just the FILTER attribution
     * instrument. Expectations are designed from the pipeline shape
     * ({@code runPipeline} javadoc), not from whatever the current run
     * happens to produce.
     */
    private void assertFamilyCoverage(JsonNode report, String leg) {
        Map<String, List<String>> byFamily = new LinkedHashMap<>();
        for (JsonNode m : report.get("mutants")) {
            byFamily.computeIfAbsent(m.get("operatorType").asText(), k -> new ArrayList<>())
                    .add(m.get("result").get("status").asText());
        }

        // JOIN: LEFT mutants are semantically equivalent on this pipeline
        // (srcC covers every grp; every srcB id is cached) → designed
        // SURVIVED; CROSS/ANTI change rows → KILLED; ANTI on the write join
        // drops valB → designed schema ERRORED (guarded globally above).
        assertFamilyStatuses(byFamily, "JOIN", leg,
                "must include KILLED and SURVIVED, never NOT_APPLIED",
                s -> s.contains("KILLED") && s.contains("SURVIVED") && !s.contains("NOT_APPLIED"));

        // WINDOW: rn ≤ 1 binds (2 rows per grp), so INVERT_WINDOW_ORDER swaps
        // which row gets rn=1 → KILLED. TRUNCATE_WINDOW_FRAME on a frameless
        // row_number is unobservable → SURVIVED, or NOT_APPLIED. Must never
        // schema-crash.
        assertFamilyStatuses(byFamily, "WINDOW", leg,
                "must include KILLED, never ERRORED",
                s -> s.contains("KILLED") && !s.contains("ERRORED"));

        // AGGREGATE: sum mutations change the exact-totals assert → KILLED.
        assertFamilyStatuses(byFamily, "AGGREGATE", leg,
                "must include KILLED",
                s -> s.contains("KILLED"));

        // PROJECT: INJECT_NULL on the cached branch changes totals → KILLED;
        // on the weak branch the count is invariant → SURVIVED.
        assertFamilyStatuses(byFamily, "PROJECT", leg,
                "must include KILLED and SURVIVED",
                s -> s.contains("KILLED") && s.contains("SURVIVED"));

        // FILTER: both branches designed (see the attribution instrument).
        assertFamilyStatuses(byFamily, "FILTER", leg,
                "must include KILLED and SURVIVED",
                s -> s.contains("KILLED") && s.contains("SURVIVED"));
    }

    private static void assertFamilyStatuses(
            Map<String, List<String>> byFamily,
            String family,
            String leg,
            String expectation,
            java.util.function.Predicate<List<String>> pin) {
        List<String> statuses = byFamily.get(family);
        assertNotNull(statuses,
                "[" + leg + "] " + family + " mutants must be discovered on the complex plan");
        assertTrue(pin.test(statuses),
                "[" + leg + "] " + family + " " + expectation + " but was: " + statuses);
    }

    private Map<String, String> statusMap(JsonNode report) {
        Map<String, String> statuses = new LinkedHashMap<>();
        for (JsonNode m : report.get("mutants")) {
            statuses.put(m.get("mutantId").asText(), m.get("result").get("status").asText());
        }
        return statuses;
    }
}
