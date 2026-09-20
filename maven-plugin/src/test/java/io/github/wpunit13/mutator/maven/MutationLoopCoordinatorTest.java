package io.github.wpunit13.mutator.maven;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.catalog.MutationCatalogIo;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import io.github.wpunit13.mutator.report.AppliedMarkerStore;
import io.github.wpunit13.mutator.report.OutcomeFileStore;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MutationLoopCoordinator} verifying the file-based
 * cross-process contract: the baseline fork writes {@code catalog.json}, the
 * coordinator forks per mutant and writes {@code outcomes/<id>.json}, then
 * merges and writes the final reports.
 */
class MutationLoopCoordinatorTest {

    private static final String MUTANT_1 = "1111111111111111";
    private static final String MUTANT_2 = "2222222222222222";
    private static final String MUTANT_3 = "3333333333333333";

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private static MutantMetadata meta(String mutantId, String... mappedTests) {
        return new MutantMetadata(
                mutantId,
                "io/github/wpunit13/pipeline/OrdersPipeline.scala",
                42,
                OperatorTypeDto.JOIN,
                1,
                "INNER -> CROSS",
                "9f8e7d6c5b4a3210",
                "INNER -> CROSS",
                List.of(mappedTests));
    }

    /**
     * Simulates the forked Surefire JVM: on the baseline request it writes
     * {@code catalog.json} (as the fork's bridge would), and on each mutant
     * request it writes the applied marker for the mutants in
     * {@code appliedMutantIds} (as the fork's bridge would after verifying
     * {@code AppliedMutantTracker}) and returns the canned outcome for that
     * mutant id.
     */
    private static class StubExecutor extends SurefireExecutor {
        private final List<SurefireRequest> requests = new ArrayList<>();
        private final Path outputDir;
        private final List<MutantMetadata> catalog;
        private final Map<String, SurefireResult> outcomes;
        private final long baselineElapsedMillis;
        private final Set<String> appliedMutantIds;

        StubExecutor(Path outputDir, List<MutantMetadata> catalog, Map<String, SurefireResult> outcomes) {
            this(outputDir, catalog, outcomes, 100L);
        }

        StubExecutor(Path outputDir, List<MutantMetadata> catalog, Map<String, SurefireResult> outcomes, long baselineElapsedMillis) {
            this(outputDir, catalog, outcomes, baselineElapsedMillis,
                    catalog.stream().map(MutantMetadata::getMutantId).collect(Collectors.toSet()));
        }

        StubExecutor(Path outputDir, List<MutantMetadata> catalog, Map<String, SurefireResult> outcomes,
                     long baselineElapsedMillis, Set<String> appliedMutantIds) {
            super();
            this.outputDir = outputDir;
            this.catalog = catalog;
            this.outcomes = outcomes;
            this.baselineElapsedMillis = baselineElapsedMillis;
            this.appliedMutantIds = appliedMutantIds;
        }

        @Override
        public SurefireResult execute(SurefireRequest request) {
            requests.add(request);
            if ("baseline".equals(request.getSystemProperties().get("spark.mutator.phase"))) {
                try {
                    MutationCatalogIo.writeCatalogJson(outputDir, catalog);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return SurefireResult.success(baselineElapsedMillis);
            }
            String mutantId = request.getSystemProperties().get("spark.mutator.active.mutant");
            if (appliedMutantIds.contains(mutantId)) {
                try {
                    AppliedMarkerStore.write(outputDir, mutantId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return outcomes.getOrDefault(mutantId, SurefireResult.success(10L));
        }
    }

    @Test
    void forkTestResultsFlowIntoTheReport() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.success(60L).withTestResults(
                List.of("pipeline.OrdersPipelineTest.test1", "pipeline.OrdersPipelineTest.test2"),
                List.of()));
        outcomes.put(MUTANT_2, SurefireExecutor.SurefireResult.failure(60L, "Expected [42] but found [0]", 1).withTestResults(
                List.of("pipeline.OrdersPipelineTest.test1"),
                List.of("pipeline.OrdersPipelineTest.test1")));

        StubExecutor mockExecutor = new StubExecutor(
                tempDir,
                List.of(meta(MUTANT_1), meta(MUTANT_2)),
                outcomes);

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);
        coordinator.execute();

        JsonNode report = mapper.readTree(tempDir.resolve("mutation-report.json").toFile());
        JsonNode survived = mutantById(report, MUTANT_1);
        JsonNode killed = mutantById(report, MUTANT_2);

        // The survivor's mapped tests = the tests that ran against it — the
        // fork path's conservative attribution (no TIA yet, whole suite runs).
        assertEquals(2, survived.get("mappedTestIds").size());
        assertEquals("pipeline.OrdersPipelineTest.test2", survived.get("mappedTestIds").get(1).asText());

        // The killed mutant names its killer in the outcome detail.
        assertEquals("KILLED", killed.get("result").get("status").asText());
        String detail = killed.get("result").get("failureDetailOrNull").asText();
        assertTrue(detail.contains("Failing tests:"), () -> detail);
        assertTrue(detail.contains("pipeline.OrdersPipelineTest.test1"), () -> detail);
    }

    @Test
    void perMutantTimingLineCarriesVerdictForkStartupExecAndDeadline() throws Exception {
        // execMs 60, suite test time 45 -> forkStartupMs 15 (the §1.3 S_fork split)
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.success(60L).withTestResults(
                List.of("pipeline.OrdersPipelineTest.test1"), List.of(), 45L));

        StubExecutor mockExecutor = new StubExecutor(tempDir, List.of(meta(MUTANT_1, "t1")), outcomes, 100L);
        List<String> lines = new ArrayList<>();

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0, List.of(), List.of(), false, lines::add, 1, 0);
        coordinator.execute();

        assertEquals(2, lines.size(), "one baseline line + one per-mutant line");
        assertEquals("baseline forkStartupMs=100 execMs=100", lines.get(0));
        assertEquals("mutant " + MUTANT_1 + " verdict=SURVIVED forkStartupMs=15 execMs=60 deadlineMs=200",
                lines.get(1));
    }

    @Test
    void shardFilterSlicesTheCatalogByIndex() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.success(10L));
        outcomes.put(MUTANT_2, SurefireExecutor.SurefireResult.success(10L));
        outcomes.put(MUTANT_3, SurefireExecutor.SurefireResult.success(10L));
        List<MutantMetadata> catalog = List.of(meta(MUTANT_1), meta(MUTANT_2), meta(MUTANT_3));

        MutationLoopCoordinator shard0 = new MutationLoopCoordinator(
                new StubExecutor(tempDir, catalog, outcomes), 2.0, tempDir.toFile(), 0.0,
                List.of(), List.of(), false, l -> {}, 2, 0);
        MutationLoopCoordinator.MutationLoopResult r0 = shard0.execute();
        assertEquals(2, r0.getTotalMutants(), "shard 0 of 3 mutants at N=2 runs indices 0 and 2");

        MutationLoopCoordinator shard1 = new MutationLoopCoordinator(
                new StubExecutor(tempDir, catalog, outcomes), 2.0, tempDir.toFile(), 0.0,
                List.of(), List.of(), false, l -> {}, 2, 1);
        MutationLoopCoordinator.MutationLoopResult r1 = shard1.execute();
        assertEquals(1, r1.getTotalMutants(), "shard 1 runs index 1 only");

        // Disjoint slices: outcomes on disk after both runs cover all 3 exactly once.
        JsonNode report = mapper.readTree(tempDir.resolve("mutation-report.json").toFile());
        assertEquals(1, report.get("mutants").size());
        assertEquals(MUTANT_2, report.get("mutants").get(0).get("mutantId").asText());
        assertEquals(2, report.get("config").get("shards").asInt());
        assertEquals(1, report.get("config").get("shard").asInt());
    }

    @Test
    void invalidShardConfigurationFailsFast() {
        assertThrows(IllegalArgumentException.class, () -> new MutationLoopCoordinator(
                new StubExecutor(tempDir, List.of(), Map.of()), 2.0, tempDir.toFile(), 0.0,
                List.of(), List.of(), false, l -> {}, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new MutationLoopCoordinator(
                new StubExecutor(tempDir, List.of(), Map.of()), 2.0, tempDir.toFile(), 0.0,
                List.of(), List.of(), false, l -> {}, 2, 2));
    }

    @Test
    void mergeFromDiskProducesFullReportFromShardOutcomes() throws Exception {
        List<MutantMetadata> catalog = List.of(meta(MUTANT_1, "t1"), meta(MUTANT_2, "t2"), meta(MUTANT_3, "t3"));
        MutationCatalogIo.writeCatalogJson(tempDir, catalog);
        OutcomeFileStore.writeOutcome(tempDir, new MutantResult(MUTANT_1, MutantStatus.KILLED, 10L, null, 1L));
        OutcomeFileStore.writeOutcome(tempDir, new MutantResult(MUTANT_2, MutantStatus.SURVIVED, 10L, null, 2L));
        OutcomeFileStore.writeOutcome(tempDir, new MutantResult(MUTANT_3, MutantStatus.NOT_APPLIED, 10L, null, 3L));

        MutationLoopCoordinator.MutationLoopResult r = new MutationLoopCoordinator(
                new StubExecutor(tempDir, catalog, Map.of()), 2.0, tempDir.toFile(), 0.0).mergeFromDisk();

        assertEquals(3, r.getTotalMutants());
        assertEquals(1, r.getKilled());
        assertEquals(1, r.getSurvived());
        assertEquals(1, r.getNotApplied());
        assertEquals(0, r.getErrored());
        assertEquals(50.0, r.getMutationScore());
        JsonNode report = mapper.readTree(tempDir.resolve("mutation-report.json").toFile());
        assertEquals(3, report.get("mutants").size());
    }

    @Test
    void mergeFailsLoudlyWhenAShardLeftNoOutcome() throws Exception {
        List<MutantMetadata> catalog = List.of(meta(MUTANT_1), meta(MUTANT_2));
        MutationCatalogIo.writeCatalogJson(tempDir, catalog);
        OutcomeFileStore.writeOutcome(tempDir, new MutantResult(MUTANT_1, MutantStatus.KILLED, 10L, null, 1L));

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                new StubExecutor(tempDir, catalog, Map.of()), 2.0, tempDir.toFile(), 0.0);
        MojoExecutionException e = assertThrows(MojoExecutionException.class, coordinator::mergeFromDisk);
        assertTrue(e.getMessage().contains("1 of 2"), e.getMessage());
        assertTrue(e.getMessage().contains(MUTANT_2), e.getMessage());
    }

    @Test
    void skipBaselineRunsLoopOnlyWithTheParentDeadline() throws Exception {
        List<MutantMetadata> catalog = List.of(meta(MUTANT_1));
        MutationCatalogIo.writeCatalogJson(tempDir, catalog);
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.success(10L));
        StubExecutor mockExecutor = new StubExecutor(tempDir, catalog, outcomes);

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0, List.of(), List.of(), false,
                l -> {}, 1, 0, true, 500L);
        coordinator.execute();

        assertEquals(1, mockExecutor.requests.size(), "no baseline request — the parent ran it");
        assertEquals(500L, mockExecutor.requests.get(0).getTimeoutMillis(),
                "children use the parent's deadline, not a recomputed one");
    }

    @Test
    void skipBaselineWithoutDeadlineFailsFast() {
        assertThrows(IllegalArgumentException.class, () -> new MutationLoopCoordinator(
                new StubExecutor(tempDir, List.of(), Map.of()), 2.0, tempDir.toFile(), 0.0,
                List.of(), List.of(), false, l -> {}, 1, 0, true, 0L));
    }

    @Test
    void childCommandReconstructsParentInvocationAndPinsDirectivesLast() {
        List<String> cmd = MutateMojo.childCommand(
                "/opt/maven/bin/mvn",
                List.of("test-compile", "spark-mutation-testing:mutate"),
                List.of("hardened"),
                Map.of("spark.version", "3.5.3", "spark.mutator.workers", "4"),
                4, 2, 21638L);

        assertEquals("/opt/maven/bin/mvn", cmd.get(0));
        assertTrue(cmd.contains("test-compile"));
        assertTrue(cmd.contains("-Phardened"));
        assertTrue(cmd.contains("-Dspark.version=3.5.3"));
        // shard directives come after the passthrough and pin workers to 1
        // so a child can never re-fan-out
        assertTrue(cmd.indexOf("-Dspark.mutator.workers=1") > cmd.indexOf("-Dspark.mutator.workers=4"));
        assertTrue(cmd.contains("-Dspark.mutator.shards=4"));
        assertTrue(cmd.contains("-Dspark.mutator.shard=2"));
        assertTrue(cmd.contains("-Dspark.mutator.skipBaseline=true"));
        assertEquals("-Dspark.mutator.deadlineMillis=21638", cmd.get(cmd.size() - 1));
    }

    private JsonNode mutantById(JsonNode report, String mutantId) {
        for (JsonNode m : report.get("mutants")) {
            if (mutantId.equals(m.get("mutantId").asText())) {
                return m;
            }
        }
        throw new AssertionError("mutant missing from report: " + mutantId);
    }

    void perTestAttributionDisablesFailFastOnMutantForks() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.success(50L));
        StubExecutor mockExecutor = new StubExecutor(tempDir, List.of(meta(MUTANT_1, "t1")), outcomes);

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0, List.of(), List.of(), true);
        coordinator.execute();

        // Baseline: no fail-fast (unchanged). Mutant fork: fail-fast OFF so the
        // surefire XML records every failing test for the test-value report.
        assertFalse(mockExecutor.requests.get(0).isFailFast());
        assertFalse(mockExecutor.requests.get(1).isFailFast());
    }

    void forkSuccessWithoutAppliedMarkerIsReclassifiedNotApplied() throws Exception {
        // Pre-WP-17 this fork outcome classified as SURVIVED — the fake
        // survivor that corrupted the mutation score. The missing applied
        // marker proves the mutation never executed. WP-24 splits this out of
        // ERRORED: it is a designed, shape-dependent outcome, not a harness
        // failure, so the gate can zero-tolerance real failures without
        // punishing it.
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.success(60L));

        StubExecutor mockExecutor = new StubExecutor(
                tempDir, List.of(meta(MUTANT_1, "OrdersPipelineTest#test1")), outcomes, 100L, Set.of());

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);

        MutationLoopCoordinator.MutationLoopResult result = coordinator.execute();

        assertEquals(1, result.getNotApplied(), "not-applied mutant must be NOT_APPLIED");
        assertEquals(0, result.getErrored());
        assertEquals(0, result.getSurvived());
        assertEquals(0, result.getKilled());
        assertEquals(0, result.getTimedOut());

        JsonNode outcome = mapper.readTree(
                tempDir.resolve("outcomes").resolve(MUTANT_1 + ".json").toFile());
        assertEquals("NOT_APPLIED", outcome.get("status").asText());
        assertEquals("mutation was not applied (coordinate matched no plan node)",
                outcome.get("failureDetailOrNull").asText());
    }

    @Test
    void appliedMarkerWithFailureIsKilled() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.failure(60L, "Expected [42] but found [0]", 1));

        StubExecutor mockExecutor = new StubExecutor(
                tempDir, List.of(meta(MUTANT_1, "OrdersPipelineTest#test1")), outcomes, 100L, Set.of(MUTANT_1));

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);

        MutationLoopCoordinator.MutationLoopResult result = coordinator.execute();

        assertEquals(1, result.getKilled(), "marker present + test failure must be KILLED");
        assertEquals(0, result.getErrored());
        assertTrue(Files.exists(tempDir.resolve("applied").resolve(MUTANT_1 + ".json")),
                "the fork's applied marker must be present");
    }

    @Test
    void timeoutTakesPrecedenceOverMissingAppliedMarker() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.timeout(250L, "Timed out after 200ms"));

        StubExecutor mockExecutor = new StubExecutor(
                tempDir, List.of(meta(MUTANT_1, "OrdersPipelineTest#test1")), outcomes, 100L, Set.of());

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);

        MutationLoopCoordinator.MutationLoopResult result = coordinator.execute();

        assertEquals(1, result.getTimedOut(), "timeout must win over the missing marker");
        assertEquals(0, result.getErrored());
    }

    @Test
    void testBaselineFailureThrowsMojoFailureException() {
        SurefireExecutor mockExecutor = new SurefireExecutor() {
            @Override
            public SurefireResult execute(SurefireRequest request) {
                return SurefireResult.failure(50L, "Compilation failure or assertion error", 1);
            }
        };

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);

        MojoFailureException ex = assertThrows(
                MojoFailureException.class,
                coordinator::execute);

        assertTrue(ex.getMessage().startsWith("Baseline test suite failed. Mutation testing aborted."));
        // Nothing was written because the baseline failed before any mutant ran.
        assertFalse(Files.exists(tempDir.resolve("catalog.json")));
    }

    @Test
    void testMutantsClassificationKilledSurvivedTimedOut() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.failure(60L, "Expected [42] but found [0]", 1));
        outcomes.put(MUTANT_2, SurefireExecutor.SurefireResult.success(70L));
        outcomes.put(MUTANT_3, SurefireExecutor.SurefireResult.timeout(250L, "Timed out after 200ms"));

        List<MutantMetadata> catalog = List.of(
                meta(MUTANT_1, "OrdersPipelineTest#test1"),
                meta(MUTANT_2, "OrdersPipelineTest#test2"),
                meta(MUTANT_3, "OrdersPipelineTest#test3"));
        StubExecutor mockExecutor = new StubExecutor(tempDir, catalog, outcomes);

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);

        MutationLoopCoordinator.MutationLoopResult result = coordinator.execute();

        assertNotNull(result);
        assertEquals(3, result.getTotalMutants());
        assertEquals(1, result.getKilled(), "Mutant 1 should be KILLED");
        assertEquals(1, result.getSurvived(), "Mutant 2 should be SURVIVED");
        assertEquals(1, result.getTimedOut(), "Mutant 3 should be TIMED_OUT");
        assertEquals(0, result.getErrored());
        assertEquals(66.67, result.getMutationScore(), 0.001);

        // Per-mutant outcome files must have been written (fan-in source of truth).
        assertTrue(Files.exists(tempDir.resolve("outcomes").resolve(MUTANT_1 + ".json")));
        assertTrue(Files.exists(tempDir.resolve("outcomes").resolve(MUTANT_2 + ".json")));
        assertTrue(Files.exists(tempDir.resolve("outcomes").resolve(MUTANT_3 + ".json")));

        // Final merged report.
        JsonNode root = mapper.readTree(tempDir.resolve("mutation-report.json").toFile());
        JsonNode summary = root.get("summary");
        assertEquals(3, summary.get("totalMutants").asInt());
        assertEquals(1, summary.get("killed").asInt());
        assertEquals(1, summary.get("survived").asInt());
        assertEquals(1, summary.get("timedOut").asInt());
        assertEquals(0, summary.get("errored").asInt());
        assertEquals(66.67, summary.get("mutationScore").asDouble(), 0.001);

        for (JsonNode m : root.get("mutants")) {
            String id = m.get("mutantId").asText();
            String status = m.get("result").get("status").asText();
            if (MUTANT_1.equals(id)) {
                assertEquals("KILLED", status);
            } else if (MUTANT_2.equals(id)) {
                assertEquals("SURVIVED", status);
            } else if (MUTANT_3.equals(id)) {
                assertEquals("TIMED_OUT", status);
            }
        }
    }

    @Test
    void testFailFastAndMappedTestsConfigured() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.success(50L));
        StubExecutor mockExecutor = new StubExecutor(tempDir, List.of(meta(MUTANT_1, "OrdersPipelineTest#testFilterMethod")), outcomes);

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);

        coordinator.execute();

        assertEquals(2, mockExecutor.requests.size(), "Expected 1 baseline request and 1 mutant request");

        SurefireExecutor.SurefireRequest baseline = mockExecutor.requests.get(0);
        assertFalse(baseline.isFailFast(), "Baseline must NOT have fail-fast enabled");
        assertNull(baseline.getTestFilter(), "Baseline runs full test suite without filter");

        SurefireExecutor.SurefireRequest mutant = mockExecutor.requests.get(1);
        assertTrue(mutant.isFailFast(), "Mutant execution MUST have fail-fast enabled");
        assertEquals("OrdersPipelineTest#testFilterMethod", mutant.getTestFilter());
        assertEquals(MUTANT_1, mutant.getSystemProperties().get("spark.mutator.active.mutant"));
        assertEquals("mutant", mutant.getSystemProperties().get("spark.mutator.phase"));
    }

    @Test
    void testMutantActivationAndPhasePassedAsSystemProperties() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.success(20L));
        StubExecutor mockExecutor = new StubExecutor(tempDir, List.of(meta(MUTANT_1, "OrdersPipelineTest#test1")), outcomes);

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);

        coordinator.execute();

        Map<String, String> baselineProps = mockExecutor.requests.get(0).getSystemProperties();
        assertEquals("baseline", baselineProps.get("spark.mutator.phase"));
        assertNull(baselineProps.get("spark.mutator.active.mutant"));

        Map<String, String> mutantProps = mockExecutor.requests.get(1).getSystemProperties();
        assertEquals("mutant", mutantProps.get("spark.mutator.phase"));
        assertEquals(MUTANT_1, mutantProps.get("spark.mutator.active.mutant"));
    }

    @Test
    void testEmptyCatalogProducesValidReport() throws Exception {
        StubExecutor mockExecutor = new StubExecutor(tempDir, List.of(), Map.of());

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 0.0);

        MutationLoopCoordinator.MutationLoopResult result = coordinator.execute();

        assertNotNull(result);
        assertEquals(0, result.getTotalMutants());
        assertEquals(0, result.getKilled());
        assertEquals(0, result.getSurvived());
        assertEquals(0, result.getTimedOut());
        assertEquals(0.0, result.getMutationScore(), 0.001);
        assertTrue(Files.exists(tempDir.resolve("mutation-report.json")));
    }

    @Test
    void testMutationScoreComputedFromOutcomes() throws Exception {
        Map<String, SurefireExecutor.SurefireResult> outcomes = new LinkedHashMap<>();
        outcomes.put(MUTANT_1, SurefireExecutor.SurefireResult.failure(20L, "fail", 1)); // KILLED
        outcomes.put(MUTANT_2, SurefireExecutor.SurefireResult.success(20L));            // SURVIVED

        StubExecutor mockExecutor = new StubExecutor(
                tempDir,
                List.of(meta(MUTANT_1, "t1"), meta(MUTANT_2, "t2")),
                outcomes);

        MutationLoopCoordinator coordinator = new MutationLoopCoordinator(
                mockExecutor, 2.0, tempDir.toFile(), 80.0);

        MutationLoopCoordinator.MutationLoopResult result = coordinator.execute();
        // (1 killed + 0 timedOut) / (1 killed + 0 timedOut + 1 survived) = 50%
        assertEquals(50.0, result.getMutationScore(), 0.001);
    }

    @Test
    void testMutateMojoExitsWithCode2WhenScoreBelowThreshold() throws Exception {
        // WP-19: the gate no longer throws MojoFailureException — it logs the
        // failure and terminates the JVM with the dedicated exit code 2. The
        // exit is captured through the package-private seam so the test fork
        // survives.
        java.util.List<Integer> exitSink = new ArrayList<>();
        MutateMojo mojo = new MutateMojo() {
            @Override
            void exitWithGateFailure(int exitCode) {
                exitSink.add(exitCode);
            }
        };
        org.apache.maven.project.MavenProject project = new org.apache.maven.project.MavenProject();
        mojo.setProject(project);
        mojo.setMinMutationScore(80.0);

        MutationLoopCoordinator mockCoordinator = new MutationLoopCoordinator(new SurefireExecutor() {
            @Override
            public SurefireResult execute(SurefireRequest request) {
                return SurefireResult.success(10L);
            }
        }) {
            @Override
            public MutationLoopResult execute() {
                return new MutationLoopResult(2, 1, 1, 0, 0, 0, 50.0, tempDir.resolve("mutation-report.json").toString());
            }
        };

        mojo.setCoordinator(mockCoordinator);

        java.util.Set<org.apache.maven.artifact.Artifact> artifacts = java.util.Collections.singleton(
                new org.apache.maven.artifact.DefaultArtifact(
                        "org.apache.spark",
                        "spark-sql_2.13",
                        "3.5.3",
                        "test",
                        "jar",
                        null,
                        new org.apache.maven.artifact.handler.DefaultArtifactHandler("jar")
                )
        );
        project.setArtifacts(artifacts);
        project.setBuild(new org.apache.maven.model.Build());

        Path tempJar = Files.createTempFile("interceptor-test-", ".jar");
        try {
            org.eclipse.aether.RepositorySystemSession session = (org.eclipse.aether.RepositorySystemSession)
                    java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[]{org.eclipse.aether.RepositorySystemSession.class},
                            (p, m, args) -> null
                    );

            org.eclipse.aether.RepositorySystem repoSystem = (org.eclipse.aether.RepositorySystem)
                    java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[]{org.eclipse.aether.RepositorySystem.class},
                            (p, m, args) -> {
                                if ("resolveArtifact".equals(m.getName())) {
                                    org.eclipse.aether.resolution.ArtifactRequest req =
                                            (org.eclipse.aether.resolution.ArtifactRequest) args[1];
                                    org.eclipse.aether.resolution.ArtifactResult res =
                                            new org.eclipse.aether.resolution.ArtifactResult(req);
                                    res.setArtifact(req.getArtifact().setFile(tempJar.toFile()));
                                    return res;
                                }
                                return null;
                            }
                    );

            mojo.setRepositorySystem(repoSystem);
            mojo.setRepositorySession(session);

            assertDoesNotThrow(mojo::execute);
            assertEquals(java.util.List.of(2), exitSink,
                    "the gate must exit with the dedicated governance-gate code 2");
        } finally {
            Files.deleteIfExists(tempJar);
        }
    }

    @Test
    void testMutateMojoSucceedsWhenScoreMeetsThreshold() throws Exception {
        MutateMojo mojo = new MutateMojo();
        org.apache.maven.project.MavenProject project = new org.apache.maven.project.MavenProject();
        mojo.setProject(project);
        mojo.setMinMutationScore(40.0);

        MutationLoopCoordinator mockCoordinator = new MutationLoopCoordinator(new SurefireExecutor() {
            @Override
            public SurefireResult execute(SurefireRequest request) {
                return SurefireResult.success(10L);
            }
        }) {
            @Override
            public MutationLoopResult execute() {
                return new MutationLoopResult(2, 1, 1, 0, 0, 0, 50.0, tempDir.resolve("mutation-report.json").toString());
            }
        };

        mojo.setCoordinator(mockCoordinator);

        java.util.Set<org.apache.maven.artifact.Artifact> artifacts = java.util.Collections.singleton(
                new org.apache.maven.artifact.DefaultArtifact(
                        "org.apache.spark",
                        "spark-sql_2.13",
                        "3.5.3",
                        "test",
                        "jar",
                        null,
                        new org.apache.maven.artifact.handler.DefaultArtifactHandler("jar")
                )
        );
        project.setArtifacts(artifacts);
        project.setBuild(new org.apache.maven.model.Build());

        Path tempJar = Files.createTempFile("interceptor-test-", ".jar");
        try {
            org.eclipse.aether.RepositorySystemSession session = (org.eclipse.aether.RepositorySystemSession)
                    java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[]{org.eclipse.aether.RepositorySystemSession.class},
                            (p, m, args) -> null
                    );

            org.eclipse.aether.RepositorySystem repoSystem = (org.eclipse.aether.RepositorySystem)
                    java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[]{org.eclipse.aether.RepositorySystem.class},
                            (p, m, args) -> {
                                if ("resolveArtifact".equals(m.getName())) {
                                    org.eclipse.aether.resolution.ArtifactRequest req =
                                            (org.eclipse.aether.resolution.ArtifactRequest) args[1];
                                    org.eclipse.aether.resolution.ArtifactResult res =
                                            new org.eclipse.aether.resolution.ArtifactResult(req);
                                    res.setArtifact(req.getArtifact().setFile(tempJar.toFile()));
                                    return res;
                                }
                                return null;
                            }
                    );

            mojo.setRepositorySystem(repoSystem);
            mojo.setRepositorySession(session);

            assertDoesNotThrow(mojo::execute);
        } finally {
            Files.deleteIfExists(tempJar);
        }
    }

    @Test
    void testSurefireRequestAndResultAccessors() {
        Map<String, String> props = java.util.Collections.singletonMap("key", "val");
        SurefireExecutor.SurefireRequest req = new SurefireExecutor.SurefireRequest(
                props, "Test#method", true, 5000L
        );
        assertEquals("val", req.getSystemProperties().get("key"));
        assertEquals("Test#method", req.getTestFilter());
        assertTrue(req.isFailFast());
        assertEquals(5000L, req.getTimeoutMillis());

        SurefireExecutor.SurefireResult ok = SurefireExecutor.SurefireResult.success(123L);
        assertTrue(ok.isSuccess());
        assertFalse(ok.isFailure());
        assertFalse(ok.isTimeout());
        assertEquals(0, ok.getExitCode());
        assertEquals(123L, ok.getElapsedMillis());
        assertNull(ok.getFailureDetail());

        SurefireExecutor.SurefireResult fail = SurefireExecutor.SurefireResult.failure(456L, "err", 1);
        assertFalse(fail.isSuccess());
        assertTrue(fail.isFailure());
        assertFalse(fail.isTimeout());
        assertEquals(1, fail.getExitCode());
        assertEquals(456L, fail.getElapsedMillis());
        assertEquals("err", fail.getFailureDetail());

        SurefireExecutor.SurefireResult timeout = SurefireExecutor.SurefireResult.timeout(789L, "timed out");
        assertFalse(timeout.isSuccess());
        assertFalse(timeout.isFailure());
        assertTrue(timeout.isTimeout());
        assertEquals(789L, timeout.getElapsedMillis());
        assertEquals("timed out", timeout.getFailureDetail());
    }
}