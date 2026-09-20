package io.github.wpunit13.mutator.junit5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.MutantRegistry;
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess;
import io.github.wpunit13.mutator.report.ReportSink;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

class SparkMutatorExtensionTest {

    private static SparkSession spark;
    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    @BeforeAll
    static void setupSpark() {
        System.setProperty("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension");
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("SparkMutatorExtensionTest")
                .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @BeforeEach
    void resetState() {
        MutantRegistry.getInstance().reset();
        MutationCatalogAccess.clearForTesting();
        ReportSink.clearForTesting();
        System.clearProperty(SparkMutatorExtension.PROPERTY_ACTIVE_MUTANT);
        System.clearProperty(SparkMutatorExtension.PROPERTY_MUTATOR_DISABLED);
        System.clearProperty(SparkMutatorExtension.PROPERTY_MUTATOR_ENABLED);
        System.setProperty("spark.mutator.outputDirectory", tempDir.toAbsolutePath().toString());
        // The unit fixtures exercise classification, not gating — disable the
        // WP-24 population gates (the gate evaluator has its own dedicated
        // unit tests in mutator-core: ReportWriterGateTest).
        System.setProperty("spark.mutator.maxErroredCount", "-1");
        System.setProperty("spark.mutator.maxNotAppliedRatio", "-1");
    }

    @AfterEach
    void cleanupState() {
        MutantRegistry.getInstance().reset();
        MutationCatalogAccess.clearForTesting();
        ReportSink.clearForTesting();
        System.clearProperty(SparkMutatorExtension.PROPERTY_ACTIVE_MUTANT);
        System.clearProperty(SparkMutatorExtension.PROPERTY_MUTATOR_DISABLED);
        System.clearProperty(SparkMutatorExtension.PROPERTY_MUTATOR_ENABLED);
        System.clearProperty("spark.mutator.outputDirectory");
        System.clearProperty("spark.mutator.maxErroredCount");
        System.clearProperty("spark.mutator.maxNotAppliedRatio");
        System.clearProperty("spark.mutator.timeoutMultiplier");
        System.clearProperty("spark.mutator.timeoutEnforced");
        SparkMutatorExtension.watchdogGraceMillis = 5_000L;
    }

    // -----------------------------------------------------------------------
    // Test Case Classes driven via JUnit 5 Launcher
    // -----------------------------------------------------------------------

    @EnableSparkMutationTesting
    static class WeakPipelineTestCase {
        @Test
        void testWeak() {
            SparkSession session = SparkSession.getActiveSession().get();
            Dataset<Row> df = session.range(0, 5).toDF("id").filter("id > 2");
            // Weak assertion: count >= 0 holds whether filter condition is true, false, or inverted
            assertTrue(df.count() >= 0);
        }
    }

    @EnableSparkMutationTesting
    static class HardenedPipelineTestCase {
        @Test
        void testHardened() {
            SparkSession session = SparkSession.getActiveSession().get();
            Dataset<Row> df = session.range(0, 5).toDF("id").filter("id > 2");
            // Hardened assertion: exact count is 2 (ids 3, 4)
            assertEquals(2L, df.count());
        }
    }

    @EnableSparkMutationTesting
    static class CrashingPipelineTestCase {
        @Test
        void testCrash() {
            SparkSession session = SparkSession.getActiveSession().get();
            // collectAsList never returns null, so this assertion cannot change
            // the fixture's outcome: the RuntimeException below is the point.
            assertNotNull(
                    session.range(0, 5).toDF("id").filter("id > 2").collectAsList(),
                    "pipeline must produce a result before the simulated crash");
            throw new RuntimeException("Simulated unhandled exception during test execution");
        }
    }

    @EnableSparkMutationTesting(enabled = false)
    static class DisabledPipelineTestCase {
        @Test
        void testDisabled() {
            SparkSession session = SparkSession.getActiveSession().get();
            Dataset<Row> df = session.range(0, 5).toDF("id").filter("id > 2");
            assertEquals(2L, df.count());
        }
    }

    /** Hangs only under an active mutant — the WP-25 canonical failure shape. */
    @EnableSparkMutationTesting
    static class HangingPipelineTestCase {
        private static SparkSession spark;

        // Own getOrCreate: prior fixtures in this suite may have stopped the
        // shared SparkContext, so the fixture must be able to rebuild it
        // (same pattern as StaticSessionLifecycleTestCase) to stay
        // order-independent. Deliberately no @AfterAll stop: Spark is one
        // context per JVM, so stopping here would kill the shared context
        // other fixtures use.
        @BeforeAll
        static void setUp() {
            spark = SparkSession.builder()
                    .master("local[1]")
                    .appName("HangingPipelineTestCase")
                    .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate();
        }

        @Test
        void testHang() throws Exception {
            Dataset<Row> df = spark.range(0, 5).toDF("id").filter("id > 2");
            assertEquals(2L, df.count());
            if (MutantRegistry.getInstance().getActiveMutantOrNull() != null) {
                Thread.sleep(60_000); // mutated rewrite hangs the driver
            }
        }
    }

    /** Hangs AND ignores interrupts — trips the abandon-and-flush breaker. */
    @EnableSparkMutationTesting
    static class UnkillablePipelineTestCase {
        private static SparkSession spark;

        @BeforeAll
        static void setUp() {
            spark = SparkSession.builder()
                    .master("local[1]")
                    .appName("UnkillablePipelineTestCase")
                    .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate();
        }

        @Test
        void testUnkillable() throws Exception {
            Dataset<Row> df = spark.range(0, 5).toDF("id").filter("id > 2");
            assertEquals(2L, df.count());
            if (MutantRegistry.getInstance().getActiveMutantOrNull() != null) {
                while (true) {
                    try {
                        Thread.sleep(60_000);
                    } catch (InterruptedException swallowed) {
                        // pretend the interrupt never happened
                    }
                }
            }
        }
    }

    /**
     * Mirrors the real-world suite shape (examples/spark-java-pipeline): the
     * session lives in a STATIC field created by {@code @BeforeAll} and stopped
     * by {@code @AfterAll}. The extension's in-process loop runs AFTER the
     * outer run's @AfterAll, so without full-lifecycle re-runs every mutant
     * re-run would execute against the stopped session and be classified
     * ERRORED instead of KILLED/SURVIVED.
     */
    @EnableSparkMutationTesting
    static class StaticSessionLifecycleTestCase {

        private static SparkSession spark;

        @BeforeAll
        static void setUp() {
            spark = SparkSession.builder()
                    .master("local[1]")
                    .appName("StaticSessionLifecycleTestCase")
                    .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate();
        }

        @AfterAll
        static void tearDown() {
            if (spark != null) {
                spark.stop();
            }
        }

        @Test
        void testHardened() {
            Dataset<Row> df = spark.range(0, 5).toDF("id").filter("id > 2");
            assertEquals(2L, df.count());
        }
    }

    // -----------------------------------------------------------------------
    // Specifications Verification Tests
    // -----------------------------------------------------------------------

    @Test
    void weakTestsAllowSurvivingMutants() throws Exception {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(WeakPipelineTestCase.class))
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request);

        Path reportFile = tempDir.resolve("mutation-report.json");
        assertTrue(Files.exists(reportFile), "mutation-report.json should be written");

        JsonNode report = mapper.readTree(reportFile.toFile());
        int totalMutants = report.get("summary").get("totalMutants").asInt();
        int survived = report.get("summary").get("survived").asInt();

        assertTrue(totalMutants > 0, "Expected at least one mutant to be discovered");
        assertTrue(survived > 0, "Expected weak test to allow at least one mutant to survive");
    }

    @Test
    void hardenedTestsKillMutants() throws Exception {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(HardenedPipelineTestCase.class))
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request);

        Path reportFile = tempDir.resolve("mutation-report.json");
        assertTrue(Files.exists(reportFile), "mutation-report.json should be written");

        JsonNode report = mapper.readTree(reportFile.toFile());
        int totalMutants = report.get("summary").get("totalMutants").asInt();
        int killed = report.get("summary").get("killed").asInt();

        assertTrue(totalMutants > 0, "Expected at least one mutant to be discovered");
        assertTrue(killed > 0, "Expected hardened test to kill at least one mutant");
    }

    @Test
    void activeMutantIsAlwaysClearedInFinallyBlocksEvenWhenTestsThrowUnhandledExceptions() {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(CrashingPipelineTestCase.class))
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request);

        // MutantRegistry must be cleanly reset and IDLE despite the unhandled exception
        assertNull(
                MutantRegistry.getInstance().getActiveMutantOrNull(),
                "Active mutant must be cleanly cleared in finally block after unhandled exception");
    }

    @Test
    void directExecutionGeneratesValidMutationReportJson() throws Exception {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(HardenedPipelineTestCase.class))
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request);

        Path reportFile = tempDir.resolve("mutation-report.json");
        assertTrue(Files.exists(reportFile), "Expected mutation-report.json to be generated in output directory");

        JsonNode rootNode = mapper.readTree(reportFile.toFile());
        assertNotNull(rootNode, "mutation-report.json must be valid JSON");
        assertTrue(rootNode.has("schemaVersion"), "mutation report must contain schemaVersion");
        assertTrue(rootNode.has("summary"), "mutation report must contain summary");
        assertTrue(rootNode.has("mutants"), "mutation report must contain mutants");
    }

    @Test
    void watchdogClassifiesHungMutantAsTimedOutAndLoopContinues() throws Exception {
        // deadline = ceil(baseline × 0.001) → single-digit ms; the re-run
        // (Spark query + hang) can never beat it.
        System.setProperty("spark.mutator.timeoutMultiplier", "0.001");
        SparkMutatorExtension.watchdogGraceMillis = 200L;

        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(HangingPipelineTestCase.class))
                .build();
        LauncherFactory.create().execute(request);

        JsonNode rootNode = mapper.readTree(tempDir.resolve("mutation-report.json").toFile());
        assertTrue(rootNode.get("config").get("timeoutEnforced").asBoolean(),
                "watchdog run must record timeoutEnforced=true");

        int total = rootNode.get("summary").get("totalMutants").asInt();
        assertTrue(total > 0, "catalog must be non-empty for this fixture");
        assertEquals(total, rootNode.get("summary").get("timedOut").asInt(),
                "every mutant must be TIMED_OUT (loop continued past the deadline hit)");
        assertEquals(0, rootNode.get("summary").get("errored").asInt(),
                "a continued loop must not synthesize missing-outcome ERROREDs");
    }

    @Test
    void watchdogAbandonsLoopAndFlushesReportWhenWorkerUnkillable() throws Exception {
        System.setProperty("spark.mutator.timeoutMultiplier", "0.001");
        SparkMutatorExtension.watchdogGraceMillis = 200L;

        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(UnkillablePipelineTestCase.class))
                .build();
        LauncherFactory.create().execute(request);

        // WP-25 acceptance: the report survives the abandon — partial results
        // with a TIMED_OUT tail beat no artifact.
        Path reportFile = tempDir.resolve("mutation-report.json");
        assertTrue(Files.exists(reportFile), "abandoned run must still flush the report");

        JsonNode rootNode = mapper.readTree(reportFile.toFile());
        assertTrue(rootNode.get("config").get("timeoutEnforced").asBoolean(),
                "config=" + rootNode.get("config") + " summary=" + rootNode.get("summary"));
        boolean sawTimedOut = false;
        for (JsonNode mutant : rootNode.get("mutants")) {
            JsonNode result = mutant.get("result");
            if (result != null && "TIMED_OUT".equals(result.get("status").asText())) {
                sawTimedOut = true;
            }
        }
        assertTrue(sawTimedOut, "the hanging mutant must be classified TIMED_OUT");
    }

    @Test
    void defaultOutputDirGeneratesMutationReport() {
        System.clearProperty("spark.mutator.outputDirectory");
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(HardenedPipelineTestCase.class))
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request);

        Path defaultReport = Path.of("target", "spark-mutator-reports", "mutation-report.json");
        assertTrue(Files.exists(defaultReport), "Expected default target/spark-mutator-reports/mutation-report.json to exist");
    }

    @Test
    void fullLifecycleReRunsClassifyMutantsAgainstALiveSession() throws Exception {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(StaticSessionLifecycleTestCase.class))
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request);

        Path reportFile = tempDir.resolve("mutation-report.json");
        assertTrue(Files.exists(reportFile), "mutation-report.json should be written");

        JsonNode report = mapper.readTree(reportFile.toFile());
        int totalMutants = report.get("summary").get("totalMutants").asInt();
        int killed = report.get("summary").get("killed").asInt();

        StringBuilder stoppedSession = new StringBuilder();
        for (JsonNode m : report.get("mutants")) {
            String detail = m.get("result").get("failureDetailOrNull").asText("");
            if (detail.contains("Cannot call methods on a stopped SparkContext")) {
                stoppedSession.append(m.get("mutantId").asText()).append(": ").append(detail).append("\n");
            }
        }

        assertTrue(totalMutants > 0, "Expected at least one mutant to be discovered");
        assertTrue(stoppedSession.length() == 0,
                "Mutant re-runs must re-run the @BeforeAll/@AfterAll lifecycle so each one gets a "
                        + "live session; re-runs executed against the stopped session:\n" + stoppedSession);
        assertTrue(killed > 0, "Expected the hardened assertion to kill at least one mutant");
    }

    @Test
    void disabledViaAnnotationDoesNotRunMutationPhase() {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(DisabledPipelineTestCase.class))
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request);

        Path reportFile = tempDir.resolve("mutation-report.json");
        assertFalse(Files.exists(reportFile), "Disabled extension should not generate mutation report");
    }

    @Test
    void disabledViaSystemPropertyDoesNotRunMutationPhase() {
        System.setProperty(SparkMutatorExtension.PROPERTY_MUTATOR_DISABLED, "true");

        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(HardenedPipelineTestCase.class))
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.execute(request);

        Path reportFile = tempDir.resolve("mutation-report.json");
        assertFalse(Files.exists(reportFile), "System property disabled extension should not generate mutation report");
    }
}
