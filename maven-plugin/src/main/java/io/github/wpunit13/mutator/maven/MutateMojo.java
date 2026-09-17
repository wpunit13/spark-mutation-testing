package io.github.wpunit13.mutator.maven;

import org.apache.maven.execution.MavenSession;
import io.github.wpunit13.mutator.report.ReportWriter;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for mutation testing of Java and Scala Spark pipelines:
 * {@code mvn spark-mutation-testing:mutate}.
 *
 * <p>Introspects the target project's resolved test classpath to determine the exact
 * {@code org.apache.spark:spark-sql_<scala_ver>:<version>} in use, resolves the matching
 * {@code io.github.wpunit13:interceptor-spark-<major.minor>_<scala_ver>} artifact via the
 * Maven Resolver (Aether) API, configures Surefire's {@code argLine} and
 * {@code additionalClasspathElements}, and orchestrates the baseline/mutation loop.
 *
 * <p><b>Governance gate exit code (WP-19).</b> When {@code minMutationScore} is
 * configured and the mutation score is below it, this mojo logs the failure and
 * calls {@link System#exit(int) System.exit(2)} — a dedicated exit code, distinct
 * from Maven's generic build-failure code 1, so CI can route the two differently.
 * The reports are fully flushed to disk by the coordinator before the gate runs, so
 * the artifact always survives the JVM termination. <b>Tradeoff:</b> the exit
 * terminates the whole Maven JVM deliberately; in a multi-module reactor the
 * remaining modules do not build. That is the accepted cost of a distinguishable
 * governance exit code (a thrown {@code MojoFailureException} would be reported by
 * Maven as exit code 1, indistinguishable from any other build failure).
 */
@Mojo(
        name = "mutate",
        defaultPhase = LifecyclePhase.VERIFY,
        requiresDependencyResolution = ResolutionScope.TEST,
        threadSafe = false)
public class MutateMojo extends AbstractMojo {

    @Component
    private RepositorySystem repoSystem;

    @Component
    private BuildPluginManager pluginManager;

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    private RepositorySystemSession repoSession;

    @Parameter(defaultValue = "${session}", readonly = true)
    private MavenSession mavenSession;

    @Parameter(defaultValue = "${plugin.version}", readonly = true)
    private String pluginVersion;

    @Parameter(property = "spark.mutator.outputDirectory", defaultValue = "${project.build.directory}/spark-mutator-reports")
    private File outputDirectory;

    @Parameter(property = "spark.mutator.timeoutMultiplier", defaultValue = "2.0")
    private double timeoutMultiplier = 2.0;

    @Parameter(property = "spark.mutator.minMutationScore", defaultValue = "0.0")
    private double minMutationScore = 0.0;

    /**
     * WP-24 governance gate: maximum allowed real-failure ERRORED mutants
     * (dead sessions, shim violations, mutation crashes). Default 0 — zero
     * tolerance, since a real-failure ERRORED means the harness or engine
     * misbehaved. Set negative to disable the check (complex plans with
     * legitimate schema-breaking mutants may need to).
     */
    @Parameter(property = "spark.mutator.maxErroredCount", defaultValue = "0")
    private int maxErroredCount = 0;

    /**
     * WP-24 governance gate: maximum allowed ratio of designed not-applied
     * mutants, computed as {@code notApplied / (totalMutants - skipped)}.
     * Default 0.20 — complex plans with cached branches legitimately produce
     * some. Set negative to disable the check.
     */
    @Parameter(property = "spark.mutator.maxNotAppliedRatio", defaultValue = "0.20")
    private double maxNotAppliedRatio = 0.20;

    /**
     * Module-path prefixes limiting which candidates Discovery registers (WP-19).
     * Settable via {@code -Dspark.mutator.targetModules=a,b} or {@code <configuration>}
     * (comma-separated on the command line; one element per {@code <targetModules>}).
     * Empty means no filtering. The engine applies the filter against the
     * current file-path hint, which no JVM-side harness feeds today — with the
     * property set and no hint fed, Discovery registers nothing (fail-safe,
     * with a single warning).
     */
    @Parameter(property = "spark.mutator.targetModules")
    private List<String> targetModules = new ArrayList<>();

    /**
     * Operator types excluded from mutation (WP-19). Canonical keys are the
     * {@code OperatorTypeDto} names — {@code JOIN}, {@code FILTER}, {@code AGGREGATE},
     * {@code WINDOW}, {@code PROJECT}, {@code OTHER} — case-insensitive. Exclusion is
     * enforced engine-side (Discovery skips excluded operators; the rewrite path
     * refuses them even with a stale catalog entry) and echoed into the report's
     * {@code config.excludedMutators} block.
     */
    @Parameter(property = "spark.mutator.excludedMutators")
    private List<String> excludedMutators = new ArrayList<>();

    private MutationLoopCoordinator coordinator;
    private SurefireExecutor surefireExecutor;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (project == null) {
            throw new MojoExecutionException("MavenProject cannot be null");
        }

        // 1. Detect Spark version from test classpath and map to interceptor coordinate
        SparkVersionDetector detector = new SparkVersionDetector(pluginVersion);
        SparkVersionDetector.InterceptorCoordinate interceptorCoord = detector.detect(project);

        getLog().info("Detected Spark " + interceptorCoord.getSparkVersion()
                + " (Scala " + interceptorCoord.getScalaVersion() + ") on test classpath");
        getLog().info("Target interceptor coordinate: " + interceptorCoord.getCoordinate());

        // 2. Resolve interceptor artifact via Maven's RepositorySystem and Session
        File interceptorJar = resolveInterceptorJar(interceptorCoord);
        getLog().info("Resolved interceptor JAR: " + interceptorJar.getAbsolutePath());

        // 3. Configure Surefire
        SurefireConfigurator surefireConfigurator = new SurefireConfigurator();
        SurefireConfigurator.SurefireConfigResult configResult =
                surefireConfigurator.configure(project, interceptorJar);

        // 4. Log configured Surefire parameters
        getLog().info("Configured Surefire argLine: " + configResult.getArgLine());
        getLog().info("Configured Surefire additionalClasspathElements: "
                + configResult.getAdditionalClasspathElements());

        // 5. Execute Mutation Loop
        if (coordinator == null && pluginManager == null && surefireExecutor == null) {
            getLog().warn("BuildPluginManager is null; skipping mutation loop execution.");
            return;
        }

        MutationLoopCoordinator.MutationLoopResult loopResult = runMutationLoop(resolveReportsDirectory());

        getLog().info("Mutation testing finished: "
                + loopResult.getKilled() + " killed, "
                + loopResult.getSurvived() + " survived, "
                + loopResult.getTimedOut() + " timed out, "
                + loopResult.getErrored() + " errored, "
                + loopResult.getNotApplied() + " not applied. "
                + "Mutation score: " + loopResult.getMutationScore() + "%");
        getLog().info("Reports written to: " + loopResult.getReportPath());

        enforceQualityGate(loopResult);
    }

    private File resolveInterceptorJar(SparkVersionDetector.InterceptorCoordinate interceptorCoord)
            throws MojoExecutionException {
        RepositorySystemSession effectiveSession = repoSession;
        if (effectiveSession == null && mavenSession != null) {
            effectiveSession = mavenSession.getRepositorySession();
        }

        if (repoSystem == null || effectiveSession == null) {
            throw new MojoExecutionException(
                    "RepositorySystem or Session is null; cannot resolve interceptor artifact.");
        }

        Artifact artifact = new DefaultArtifact(
                interceptorCoord.getGroupId(),
                interceptorCoord.getArtifactId(),
                "jar",
                interceptorCoord.getVersion()
        );

        ArtifactRequest request = new ArtifactRequest();
        request.setArtifact(artifact);
        if (project.getRemoteProjectRepositories() != null) {
            request.setRepositories(project.getRemoteProjectRepositories());
        }

        ArtifactResult result;
        try {
            result = repoSystem.resolveArtifact(effectiveSession, request);
        } catch (ArtifactResolutionException e) {
            throw new MojoExecutionException(
                    "Failed to resolve interceptor artifact '" + interceptorCoord.getCoordinate() + "': " + e.getMessage(), e);
        }

        File interceptorJar = result.getArtifact() != null ? result.getArtifact().getFile() : null;
        if (interceptorJar == null || !interceptorJar.exists()) {
            throw new MojoExecutionException(
                    "Resolved interceptor JAR does not exist for '" + interceptorCoord.getCoordinate() + "'");
        }
        return interceptorJar;
    }

    private File resolveReportsDirectory() {
        File reportsDir = outputDirectory;
        if (reportsDir == null) {
            if (project.getBuild() != null && project.getBuild().getDirectory() != null) {
                reportsDir = new File(project.getBuild().getDirectory(), "spark-mutator-reports");
            } else {
                reportsDir = new File("target", "spark-mutator-reports");
            }
        }
        return reportsDir;
    }

    private MutationLoopCoordinator.MutationLoopResult runMutationLoop(File reportsDir)
            throws MojoExecutionException, MojoFailureException {
        MutationLoopCoordinator effectiveCoordinator = coordinator;
        if (effectiveCoordinator == null) {
            SurefireExecutor executor = surefireExecutor != null
                    ? surefireExecutor
                    : new SurefireExecutor(pluginManager, mavenSession, project);
            effectiveCoordinator = new MutationLoopCoordinator(
                    executor,
                    timeoutMultiplier,
                    reportsDir,
                    minMutationScore,
                    targetModules,
                    excludedMutators
            );
        }

        try {
            return effectiveCoordinator.execute();
        } catch (MojoFailureException e) {
            throw e;
        } catch (Exception e) {
            throw new MojoExecutionException("Mutation testing execution failed: " + e.getMessage(), e);
        }
    }

    /**
     * Governance gate (WP-19): when the score is below the floor, the reports
     * are already flushed (the coordinator writes them before returning), so
     * the Mojo terminates the Maven JVM with the dedicated exit code 2 after
     * logging the failure. See the class javadoc for the JVM-termination
     * tradeoff.
     */
    private void enforceQualityGate(MutationLoopCoordinator.MutationLoopResult loopResult) {
        // WP-24 population gates first: real-failure ERRORED is zero-tolerance
        // (a dead session or shim violation means the harness/engine is broken);
        // the designed not-applied population is ratio-gated (complex plans
        // with cached branches legitimately produce some).
        List<String> populationViolations = ReportWriter.evaluateGateViolations(
                loopResult.getTotalMutants(),
                loopResult.getErrored(),
                loopResult.getNotApplied(),
                0,
                maxErroredCount,
                maxNotAppliedRatio);
        if (!populationViolations.isEmpty()) {
            populationViolations.forEach(getLog()::error);
            getLog().error("Reports are on disk at: " + loopResult.getReportPath());
            exitWithGateFailure(GATE_FAILURE_EXIT_CODE);
        }
        if (minMutationScore > 0.0 && loopResult.getMutationScore() < minMutationScore) {
            getLog().error("Quality gate FAILED: mutation score (" + loopResult.getMutationScore()
                    + "%) is below minimum threshold (" + minMutationScore + "%).");
            getLog().error("Reports are on disk at: " + loopResult.getReportPath());
            exitWithGateFailure(GATE_FAILURE_EXIT_CODE);
        }
    }

    /**
     * Dedicated governance-gate exit code (core_idea.md §8): "gate failed (2)"
     * is distinguishable from Maven's generic "build failed (1)".
     */
    static final int GATE_FAILURE_EXIT_CODE = 2;

    /**
     * Terminates the Maven JVM with the governance-gate exit code. Package-private
     * seam so tests can observe the exit code without killing the test JVM.
     */
    void exitWithGateFailure(int exitCode) {
        System.exit(exitCode);
    }

    void setRepositorySystem(RepositorySystem repoSystem) {
        this.repoSystem = repoSystem;
    }

    void setProject(MavenProject project) {
        this.project = project;
    }

    void setRepositorySession(RepositorySystemSession repoSession) {
        this.repoSession = repoSession;
    }

    void setMavenSession(MavenSession mavenSession) {
        this.mavenSession = mavenSession;
    }

    void setPluginVersion(String pluginVersion) {
        this.pluginVersion = pluginVersion;
    }

    void setPluginManager(BuildPluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    void setOutputDirectory(File outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    void setTimeoutMultiplier(double timeoutMultiplier) {
        this.timeoutMultiplier = timeoutMultiplier;
    }

    void setMinMutationScore(double minMutationScore) {
        this.minMutationScore = minMutationScore;
    }

    void setTargetModules(List<String> targetModules) {
        this.targetModules = targetModules;
    }

    void setExcludedMutators(List<String> excludedMutators) {
        this.excludedMutators = excludedMutators;
    }

    void setCoordinator(MutationLoopCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    void setSurefireExecutor(SurefireExecutor surefireExecutor) {
        this.surefireExecutor = surefireExecutor;
    }

    RepositorySystem getRepositorySystem() {
        return repoSystem;
    }

    BuildPluginManager getPluginManager() {
        return pluginManager;
    }

    MavenProject getProject() {
        return project;
    }

    File getOutputDirectory() {
        return outputDirectory;
    }

    double getTimeoutMultiplier() {
        return timeoutMultiplier;
    }

    double getMinMutationScore() {
        return minMutationScore;
    }

    List<String> getTargetModules() {
        return targetModules;
    }

    List<String> getExcludedMutators() {
        return excludedMutators;
    }

    MutationLoopCoordinator getCoordinator() {
        return coordinator;
    }

    SurefireExecutor getSurefireExecutor() {
        return surefireExecutor;
    }
}
