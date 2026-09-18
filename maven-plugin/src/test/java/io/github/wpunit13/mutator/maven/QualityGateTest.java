package io.github.wpunit13.mutator.maven;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WP-16/WP-19: verifies the Maven quality-gate contract in {@link MutateMojo#execute()} —
 * when {@code minMutationScore} is configured and the mutation score is below it,
 * the mojo terminates the JVM with the dedicated governance-gate exit code 2
 * (distinct from Maven's generic build-failure code 1); when the score meets the
 * threshold (including the exact boundary) or no threshold is configured
 * ({@code 0.0}), the mojo succeeds and never exits.
 *
 * <p>The mutation loop is stubbed (the loop's Surefire execution is a
 * read-only dependency here), so each test pins only the threshold decision.
 * {@code System.exit} is observed through the package-private
 * {@code exitWithGateFailure} seam — the production implementation calls
 * {@code System.exit(2)}, which would kill the test JVM.
 */
class QualityGateTest {

    @TempDir
    Path tempDir;

    /**
     * Builds a fully-wired {@link MutateMojo} whose mutation loop is stubbed to
     * return the given result, so the quality-gate decision in execute() can be
     * tested without running Surefire. When {@code exitSink} is non-null, the
     * mojo's gate exit is captured into it instead of terminating the JVM.
     */
    private MutateMojo mojoWithLoopResult(
            MutationLoopCoordinator.MutationLoopResult result,
            double minMutationScore,
            java.util.List<Integer> exitSink) throws Exception {
        MutateMojo mojo = new MutateMojo() {
            @Override
            void exitWithGateFailure(int exitCode) {
                if (exitSink != null) {
                    exitSink.add(exitCode);
                } else {
                    super.exitWithGateFailure(exitCode);
                }
            }
        };
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
                2, 1, 1, 0, 0, 0, score, tempDir.resolve("mutation-report.json").toString());
    }

    @Test
    void qualityGateExitsWithCode2WhenThresholdIsBreached() throws Exception {
        java.util.List<Integer> exitSink = new java.util.ArrayList<>();
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(50.0), 80.0, exitSink);

        // No exception: the gate logs the failure and terminates the JVM with
        // the dedicated governance-gate code 2 (WP-19), distinct from Maven's
        // generic build-failure code 1.
        assertDoesNotThrow(mojo::execute);
        assertEquals(java.util.List.of(2), exitSink,
                "the gate must exit with the dedicated code 2, not a MojoFailureException");
    }

    @Test
    void qualityGateNeverExitsWhenScoreMeetsThreshold() throws Exception {
        java.util.List<Integer> exitSink = new java.util.ArrayList<>();
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(50.0), 40.0, exitSink);

        assertDoesNotThrow(mojo::execute);
        assertTrue(exitSink.isEmpty(), "a met threshold must never trigger the gate exit");
    }

    @Test
    void qualityGateNeverExitsWhenScoreEqualsThresholdExactly() throws Exception {
        // score == threshold is a meet, not a breach (the gate is strictly "<").
        java.util.List<Integer> exitSink = new java.util.ArrayList<>();
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(80.0), 80.0, exitSink);

        assertDoesNotThrow(mojo::execute);
        assertTrue(exitSink.isEmpty(), "a met threshold must never trigger the gate exit");
    }

    @Test
    void qualityGateNeverExitsWhenThresholdIsZero() throws Exception {
        // Unconfigured / zero threshold: the gate is off and never fails the
        // build, even for a 0.0% score.
        java.util.List<Integer> exitSink = new java.util.ArrayList<>();
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(0.0), 0.0, exitSink);

        assertDoesNotThrow(mojo::execute);
        assertTrue(exitSink.isEmpty(), "a disabled gate must never trigger the exit");
    }

    @Test
    void gateViolationThrowsInsteadOfExitingInReactorSafeMode() throws Exception {
        // exitProcessOnGateFailure=false: the gate throws MojoFailureException
        // (Maven exit code 1, reactor honors --fail-at-end) and never touches
        // System.exit — the multi-module escape hatch for the WP-19 exit code.
        java.util.List<Integer> exitSink = new java.util.ArrayList<>();
        MutateMojo mojo = mojoWithLoopResult(resultWithScore(50.0), 80.0, exitSink);
        mojo.setExitProcessOnGateFailure(false);

        org.apache.maven.plugin.MojoFailureException thrown =
                org.junit.jupiter.api.Assertions.assertThrows(
                        org.apache.maven.plugin.MojoFailureException.class, mojo::execute);
        assertTrue(thrown.getMessage().contains("80.0"),
                "the failure must name the breached threshold");
        assertTrue(exitSink.isEmpty(),
                "reactor-safe mode must fail via MojoFailureException, never System.exit");
    }

    @Test
    void populationGateAlsoThrowsInReactorSafeMode() throws Exception {
        // The WP-24 ERRORED population gate follows the same delivery mode.
        MutationLoopCoordinator.MutationLoopResult errored =
                new MutationLoopCoordinator.MutationLoopResult(
                        2, 0, 0, 0, 1, 0, 0.0, tempDir.resolve("mutation-report.json").toString());
        java.util.List<Integer> exitSink = new java.util.ArrayList<>();
        MutateMojo mojo = mojoWithLoopResult(errored, 0.0, exitSink);
        mojo.setExitProcessOnGateFailure(false);

        org.junit.jupiter.api.Assertions.assertThrows(
                org.apache.maven.plugin.MojoFailureException.class, mojo::execute);
        assertTrue(exitSink.isEmpty(),
                "reactor-safe mode must fail via MojoFailureException, never System.exit");
    }
}