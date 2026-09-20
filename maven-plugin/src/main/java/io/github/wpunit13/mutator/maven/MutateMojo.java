package io.github.wpunit13.mutator.maven;

import org.apache.maven.execution.MavenExecutionRequest;
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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * Maven as exit code 1, indistinguishable from any other build failure). Set
 * {@code -Dspark.mutator.exitProcessOnGateFailure=false} to trade the dedicated
 * exit code for reactor-friendly failure semantics (see that parameter).
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
     * Runs every mapped test per killed mutant (no fail-fast) so the fork's
     * surefire XML records ALL failing tests, not just the first. Feeds the
     * test-value report's sole-killer verdicts; costs runtime on killed
     * mutants (the suite runs to completion instead of stopping early).
     * Settable via {@code -Dspark.mutator.perTestAttribution=true}.
     */
    @Parameter(property = "spark.mutator.perTestAttribution", defaultValue = "false")
    private boolean perTestAttribution = false;

    /**
     * Post-join merge for CI sharding: skips the baseline and the mutation
     * loop, reads the full catalog + every shard's {@code outcomes/<id>.json}
     * from {@code outputDirectory}, and writes the combined reports. Gates
     * (WP-24 + {@code minMutationScore}) then evaluate once on the merged
     * counts. Fails loudly if any catalogued mutant lacks an outcome file (a
     * shard died or was truncated — rerun it, then merge).
     */
    @Parameter(property = "spark.mutator.mergeOnly", defaultValue = "false")
    private boolean mergeOnly = false;

    /**
     * Single-command orchestration: the parent runs the baseline once, spawns
     * {@code workers} child Maven processes (each running one shard of the
     * catalog with the parent's deadline), waits for all, then merges in-process
     * and evaluates the gates on the merged counts. Default 1 = today's serial
     * path, byte-identical. Each worker is a full Spark driver fork (~2 GB heap)
     * plus a child Maven JVM. Mutually exclusive with explicit
     * {@code shards}/{@code shard} and with {@code mergeOnly}.
     */
    @Parameter(property = "spark.mutator.workers", defaultValue = "1")
    private int workers = 1;

    /** Internal (orchestrator → child): skip the baseline fork; the parent
      * already ran it. Never set by hand — see §3.1. */
    @Parameter(property = "spark.mutator.skipBaseline", defaultValue = "false")
    private boolean skipBaseline = false;

    /** Internal (orchestrator → child): the parent's computed per-mutant
      * deadline. Never set by hand — see §3.1. */
    @Parameter(property = "spark.mutator.deadlineMillis", defaultValue = "0")
    private long deadlineMillis = 0;

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

    /**
     * Gate-failure delivery (WP-19 follow-up). Default {@code true}: a gate
     * violation terminates the Maven JVM with the dedicated governance-gate
     * exit code 2. Set {@code false} for multi-module reactors: the gate throws
     * {@link MojoFailureException} instead — the build still fails (Maven exit
     * code 1), but the reactor honors {@code --fail-at-end} / {@code --fail-never}
     * so remaining modules still build. Reports are flushed to disk either way;
     * pair {@code false} with {@code --fail-at-end} for full-reactor runs.
     */
    @Parameter(property = "spark.mutator.exitProcessOnGateFailure", defaultValue = "true")
    private boolean exitProcessOnGateFailure = true;

    /**
     * Injects Spark's mandatory modular-runtime JVM args (the {@code --add-opens}
     * set plus Netty/reflect flags, see {@link SurefireConfigurator#SPARK_JVM_OPEN_ARGS})
     * into Surefire's {@code argLine} alongside the extension property. Default
     * {@code true}; nothing is injected on Java 8. Disable only if the fork JDK
     * is managed by other means (e.g. Maven toolchains).
     */
    @Parameter(property = "spark.mutator.injectAddOpens", defaultValue = "true")
    private boolean injectAddOpens = true;

    /**
     * Mechanism-(c) CI sharding: total shard count N. Each CI job runs the same
     * goal with {@code -Dspark.mutator.shards=N -Dspark.mutator.shard=i}; the
     * catalog is filtered by index ({@code i % N == i_shard}), so slices are
     * deterministic and disjoint and the N jobs can run in parallel. Default 1
     * = whole catalog, byte-identical to the unsharded loop. Discovery (the
     * baseline fork) is never sharded.
     */
    @Parameter(property = "spark.mutator.shards", defaultValue = "1")
    private int shards = 1;

    /** This worker's shard index, in {@code [0, shards)}. */
    @Parameter(property = "spark.mutator.shard", defaultValue = "0")
    private int shard = 0;

    private MutationLoopCoordinator coordinator;
    private SurefireExecutor surefireExecutor;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (project == null) {
            throw new MojoExecutionException("MavenProject cannot be null");
        }

        // Sharding validation: hard below the floor (a misconfigured shard must
        // not silently run everything or nothing), soft above the cap.
        if (shards < 1) {
            throw new MojoExecutionException("spark.mutator.shards must be >= 1 (got " + shards + ")");
        }
        if (shard < 0 || shard >= shards) {
            throw new MojoExecutionException(
                    "spark.mutator.shard must be in [0," + shards + ") (got " + shard + ")");
        }
        if (shards > 8) {
            getLog().warn("spark.mutator.shards=" + shards
                    + " exceeds the soft cap of 8. Each shard runs a full Spark driver fork:"
                    + " budget ~2 GB heap per concurrent worker on a shared runner, and keep"
                    + " sessions' local[k] x workers <= cores.");
        }
        if (mergeOnly && workers > 1) {
            throw new MojoExecutionException(
                    "spark.mutator.workers has no effect with mergeOnly=true (the merge runs no forks)");
        }
        if (workers > 1 && (shards != 1 || shard != 0)) {
            throw new MojoExecutionException(
                    "Use either spark.mutator.workers (single-command orchestration) or"
                            + " spark.mutator.shards/shard (CI matrix), not both.");
        }
        if (workers < 1) {
            throw new MojoExecutionException("spark.mutator.workers must be >= 1 (got " + workers + ")");
        }
        if (workers > 8) {
            getLog().warn("spark.mutator.workers=" + workers
                    + " exceeds the soft cap of 8. Each worker runs a full Spark driver fork"
                    + " (~2 GB heap) plus a child Maven JVM (~0.5 GB); budget RAM accordingly"
                    + " and keep sessions' local[k] x workers <= cores.");
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
        SurefireConfigurator surefireConfigurator = new SurefireConfigurator(injectAddOpens);
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

        MutationLoopCoordinator.MutationLoopResult loopResult = mergeOnly
                ? runMerge(resolveReportsDirectory())
                : workers > 1
                        ? runOrchestrated(resolveReportsDirectory())
                        : runMutationLoop(resolveReportsDirectory());

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
        try {
            return effectiveCoordinator(reportsDir).execute();
        } catch (MojoFailureException e) {
            throw e;
        } catch (Exception e) {
            throw new MojoExecutionException("Mutation testing execution failed: " + e.getMessage(), e);
        }
    }

    private MutationLoopCoordinator.MutationLoopResult runMerge(File reportsDir)
            throws MojoExecutionException {
        try {
            return effectiveCoordinator(reportsDir).mergeFromDisk();
        } catch (MojoExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new MojoExecutionException("Merge failed: " + e.getMessage(), e);
        }
    }

    /**
     * Single-command orchestration: baseline once in-process, then N child
     * Maven processes (one per shard, loop-only, parent's deadline), then the
     * in-process merge + post-join gates. A failed child fails the parent
     * after all workers finish — no merge over a partial outcome set.
     */
    private MutationLoopCoordinator.MutationLoopResult runOrchestrated(File reportsDir)
            throws MojoExecutionException, MojoFailureException {
        MutationLoopCoordinator coord = effectiveCoordinator(reportsDir);
        long deadline;
        try {
            deadline = coord.runBaseline();
        } catch (MojoFailureException e) {
            throw e;
        } catch (Exception e) {
            throw new MojoExecutionException("Orchestrated baseline failed: " + e.getMessage(), e);
        }

        MavenExecutionRequest request = mavenSession.getRequest();
        Map<String, String> userProps = new HashMap<>();
        request.getUserProperties().forEach((k, v) -> userProps.put(String.valueOf(k), String.valueOf(v)));
        List<Process> children = new ArrayList<>();
        try {
            for (int i = 0; i < workers; i++) {
                List<String> cmd = childCommand(
                        mavenBinary(),
                        request.getGoals(),
                        request.getActiveProfiles(),
                        userProps,
                        workers, i, deadline);
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.directory(new File(mavenSession.getExecutionRootDirectory()));
                pb.inheritIO();
                children.add(pb.start());
            }
        } catch (IOException e) {
            throw new MojoExecutionException("Failed to launch shard workers: " + e.getMessage(), e);
        }

        List<Integer> failed = new ArrayList<>();
        try {
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i).waitFor() != 0) {
                    failed.add(i);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MojoExecutionException("Interrupted while waiting for shard workers", e);
        }
        if (!failed.isEmpty()) {
            throw new MojoExecutionException("Shard worker(s) failed: " + failed
                    + ". Rerun the failed shard(s) with -Dspark.mutator.shards=" + workers
                    + " -Dspark.mutator.shard=<i>, then merge with -Dspark.mutator.mergeOnly=true.");
        }

        try {
            return coord.mergeFromDisk();
        } catch (Exception e) {
            throw new MojoExecutionException("Orchestrated merge failed: " + e.getMessage(), e);
        }
    }

    /** Reconstructs the child Maven invocation: same goals, -P profiles and
      * CLI -D properties as the parent, then the shard directives (last, so
      * they win). workers is pinned to 1 in children — a child must never
      * re-fan-out. */
    static List<String> childCommand(
            String mavenBinary,
            List<String> goals,
            List<String> profileIds,
            Map<String, String> userProperties,
            int workers,
            int shardIndex,
            long deadlineMillis) {
        List<String> cmd = new ArrayList<>();
        cmd.add(mavenBinary);
        cmd.addAll(goals == null || goals.isEmpty() ? List.of("spark-mutation-testing:mutate") : goals);
        if (profileIds != null && !profileIds.isEmpty()) {
            cmd.add("-P" + String.join(",", profileIds));
        }
        if (userProperties != null) {
            userProperties.forEach((k, v) -> cmd.add("-D" + k + "=" + v));
        }
        // Overrides last: later -D wins on the CLI.
        cmd.add("-Dspark.mutator.workers=1");
        cmd.add("-Dspark.mutator.shards=" + workers);
        cmd.add("-Dspark.mutator.shard=" + shardIndex);
        cmd.add("-Dspark.mutator.skipBaseline=true");
        cmd.add("-Dspark.mutator.deadlineMillis=" + deadlineMillis);
        return cmd;
    }

    /** The exact Maven binary that launched this JVM (maven.home), falling
      * back to PATH — keeps wrapper-pinned versions consistent for children. */
    private static String mavenBinary() {
        String mavenHome = System.getProperty("maven.home");
        if (mavenHome != null && !mavenHome.isBlank()) {
            Path candidate = Path.of(mavenHome, "bin", "mvn");
            if (Files.exists(candidate)) {
                return candidate.toString();
            }
        }
        return "mvn";
    }

    private MutationLoopCoordinator effectiveCoordinator(File reportsDir) {
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
                    excludedMutators,
                    perTestAttribution,
                    getLog()::info,
                    shards,
                    shard,
                    skipBaseline,
                    deadlineMillis
            );
        }
        return effectiveCoordinator;
    }

    /**
     * Governance gate (WP-19): when the score is below the floor, the reports
     * are already flushed (the coordinator writes them before returning), so
     * the Mojo terminates the Maven JVM with the dedicated exit code 2 after
     * logging the failure. See the class javadoc for the JVM-termination
     * tradeoff.
     */
    private void enforceQualityGate(MutationLoopCoordinator.MutationLoopResult loopResult)
            throws MojoFailureException {
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
            failGate(loopResult, String.join("; ", populationViolations));
        }
        if (minMutationScore > 0.0 && loopResult.getMutationScore() < minMutationScore) {
            String message = "Quality gate FAILED: mutation score (" + loopResult.getMutationScore()
                    + "%) is below minimum threshold (" + minMutationScore + "%).";
            getLog().error(message);
            failGate(loopResult, message);
        }
    }

    /**
     * Delivers a gate violation per {@code exitProcessOnGateFailure}: the
     * dedicated exit code 2 (WP-19 default), or a {@link MojoFailureException}
     * for reactor-friendly multi-module builds.
     */
    private void failGate(MutationLoopCoordinator.MutationLoopResult loopResult, String message)
            throws MojoFailureException {
        getLog().error("Reports are on disk at: " + loopResult.getReportPath());
        if (exitProcessOnGateFailure) {
            exitWithGateFailure(GATE_FAILURE_EXIT_CODE);
            return; // not reached in production
        }
        throw new MojoFailureException(message);
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

    void setExitProcessOnGateFailure(boolean exitProcessOnGateFailure) {
        this.exitProcessOnGateFailure = exitProcessOnGateFailure;
    }

    void setInjectAddOpens(boolean injectAddOpens) {
        this.injectAddOpens = injectAddOpens;
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
