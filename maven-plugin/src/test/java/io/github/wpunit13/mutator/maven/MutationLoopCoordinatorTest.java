package io.github.wpunit13.mutator.maven;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.wpunit13.mutator.catalog.MutationCatalogIo;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import org.apache.maven.plugin.MojoFailureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     * request returns the canned outcome for that mutant id.
     */
    private static class StubExecutor extends SurefireExecutor {
        private final List<SurefireRequest> requests = new ArrayList<>();
        private final Path outputDir;
        private final List<MutantMetadata> catalog;
        private final Map<String, SurefireResult> outcomes;
        private final long baselineElapsedMillis;

        StubExecutor(Path outputDir, List<MutantMetadata> catalog, Map<String, SurefireResult> outcomes) {
            this(outputDir, catalog, outcomes, 100L);
        }

        StubExecutor(Path outputDir, List<MutantMetadata> catalog, Map<String, SurefireResult> outcomes, long baselineElapsedMillis) {
            super();
            this.outputDir = outputDir;
            this.catalog = catalog;
            this.outcomes = outcomes;
            this.baselineElapsedMillis = baselineElapsedMillis;
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
            return outcomes.getOrDefault(mutantId, SurefireResult.success(10L));
        }
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

        assertEquals("Baseline test suite failed. Mutation testing aborted.", ex.getMessage());
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
    void testMutateMojoThrowsWhenScoreBelowThreshold() throws Exception {
        MutateMojo mojo = new MutateMojo();
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
                return new MutationLoopResult(2, 1, 1, 0, 0, 50.0, tempDir.resolve("mutation-report.json").toString());
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

            MojoFailureException ex = assertThrows(
                    MojoFailureException.class,
                    mojo::execute
            );
            assertTrue(ex.getMessage().contains("is below minimum threshold"));
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
                return new MutationLoopResult(2, 1, 1, 0, 0, 50.0, tempDir.resolve("mutation-report.json").toString());
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

            mojo.execute();
            // Execution should succeed without throwing exception
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