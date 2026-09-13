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
