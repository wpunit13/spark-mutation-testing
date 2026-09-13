package io.github.wpunit13.mutator.maven;

import org.apache.maven.plugin.MojoFailureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WP-16: verifies the Maven quality-gate contract in {@link MutateMojo#execute()} —
 * when {@code minMutationScore} is configured and the mutation score is below it,
 * the mojo fails ({@code MojoFailureException}); when the score meets the
 * threshold (including the exact boundary) or no threshold is configured
 * ({@code 0.0}), the mojo succeeds.
 *
 * <p>The mutation loop is stubbed (the loop's Surefire execution is a
 * read-only dependency here), so each test pins only the threshold decision.
 */
class QualityGateTest {

    @TempDir
    Path tempDir;

    /**
     * Builds a fully-wired {@link MutateMojo} whose mutation loop is stubbed to
     * return the given result, so the quality-gate decision in execute() can be
     * tested without running Surefire.
     */
    private MutateMojo mojoWithLoopResult(
            MutationLoopCoordinator.MutationLoopResult result, double minMutationScore) throws Exception {
        MutateMojo mojo = new MutateMojo();
        org.apache.maven.project.MavenProject project = new org.apache.maven.project.MavenProject();
        mojo.setProject(project);
        mojo.setMinMutationScore(minMutationScore);

        MutationLoopCoordinator mockCoordinator = new MutationLoopCoordinator(new SurefireExecutor() {
            @Override
            public SurefireResult execute(SurefireRequest request) {
                return SurefireResult.success(10L);
            }
        }) {
            @Override
            public MutationLoopResult execute() {
                return result;
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

        Path tempJar = tempDir.resolve("interceptor-test.jar");
        if (Files.notExists(tempJar)) {
            Files.createFile(tempJar);
        }

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
        return mojo;
    }

    private MutationLoopCoordinator.MutationLoopResult resultWithScore(double score) {
        return new MutationLoopCoordinator.MutationLoopResult(
                2, 1, 1, 0, 0, score, tempDir.resolve("mutation-report.json").toString());
    }

    @Test
    void qualityGateThrowsWhenThresholdIsBreached() throws Exception {
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(50.0), 80.0);

        MojoFailureException ex = assertThrows(
                MojoFailureException.class,
                mojo::execute
        );
        assertTrue(ex.getMessage().contains("is below minimum threshold"));
    }

    @Test
    void qualityGateSucceedsWhenScoreMeetsThreshold() throws Exception {
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(50.0), 40.0);

        assertDoesNotThrow(mojo::execute);
    }

    @Test
    void qualityGateSucceedsWhenScoreEqualsThresholdExactly() throws Exception {
        // score == threshold is a meet, not a breach (the gate is strictly "<").
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(80.0), 80.0);

        assertDoesNotThrow(mojo::execute);
    }

    @Test
    void qualityGateSucceedsWhenThresholdIsZero() throws Exception {
        // Unconfigured / zero threshold: the gate is off and never fails the
        // build, even for a 0.0% score.
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(0.0), 0.0);

        assertDoesNotThrow(mojo::execute);
    }
}