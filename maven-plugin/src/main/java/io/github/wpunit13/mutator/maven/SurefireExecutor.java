package io.github.wpunit13.mutator.maven;

import org.apache.maven.execution.MavenSession;
import org.apache.maven.model.Plugin;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecution;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.MojoDescriptor;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.eclipse.aether.repository.RemoteRepository;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wraps Maven's {@link BuildPluginManager} to programmatically invoke
 * {@code maven-surefire-plugin:test} with dynamic system properties,
 * test filtering, fail-fast rules, and timeout enforcement.
 */
public class SurefireExecutor {

    public static final String SUREFIRE_GROUP_ID = "org.apache.maven.plugins";
    public static final String SUREFIRE_ARTIFACT_ID = "maven-surefire-plugin";
    public static final String SUREFIRE_DEFAULT_VERSION = "3.2.5";

    private final BuildPluginManager pluginManager;
    private final MavenSession mavenSession;
    private final MavenProject project;

    public SurefireExecutor(BuildPluginManager pluginManager, MavenSession mavenSession, MavenProject project) {
        this.pluginManager = pluginManager;
        this.mavenSession = mavenSession;
        this.project = project;
    }

    /**
     * Protected constructor for test mocks and subclasses.
     */
    protected SurefireExecutor() {
        this(null, null, null);
    }

    /**
     * Executes {@code maven-surefire-plugin:test} with parameters specified in {@code request}.
     *
     * @param request configuration parameters for this execution
     * @return outcome of the Surefire execution
     * @throws MojoExecutionException if a fatal plugin manager error occurs
     */
    public SurefireResult execute(SurefireRequest request) throws MojoExecutionException {
        if (pluginManager == null || mavenSession == null || project == null) {
            throw new MojoExecutionException(
                    "Cannot execute Surefire: BuildPluginManager, MavenSession, or MavenProject is null.");
        }

        long start = System.currentTimeMillis();

        Plugin surefirePlugin = project.getPlugin(SUREFIRE_GROUP_ID + ":" + SUREFIRE_ARTIFACT_ID);
        if (surefirePlugin == null) {
            surefirePlugin = new Plugin();
            surefirePlugin.setGroupId(SUREFIRE_GROUP_ID);
            surefirePlugin.setArtifactId(SUREFIRE_ARTIFACT_ID);
            surefirePlugin.setVersion(SUREFIRE_DEFAULT_VERSION);
        } else if (surefirePlugin.getVersion() == null || surefirePlugin.getVersion().isBlank()) {
            surefirePlugin.setVersion(SUREFIRE_DEFAULT_VERSION);
        }

        List<RemoteRepository> repos = project.getRemotePluginRepositories() != null
                ? project.getRemotePluginRepositories()
                : Collections.emptyList();

        MojoDescriptor mojoDescriptor;
        try {
            pluginManager.loadPlugin(surefirePlugin, repos, mavenSession.getRepositorySession());
            mojoDescriptor = pluginManager.getMojoDescriptor(
                    surefirePlugin,
                    "test",
                    repos,
                    mavenSession.getRepositorySession()
            );
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to obtain MojoDescriptor for maven-surefire-plugin:test", e);
        }

        Xpp3Dom config = new Xpp3Dom("configuration");
        if (surefirePlugin.getConfiguration() instanceof Xpp3Dom) {
            config = new Xpp3Dom((Xpp3Dom) surefirePlugin.getConfiguration());
        }

        // Ensure required parameters have evaluation bindings for expression evaluator
        ensureDefaultParameter(config, "basedir", "${basedir}", null);
        ensureDefaultParameter(config, "project", "${project}", null);
        ensureDefaultParameter(config, "session", "${session}", null);
        ensureDefaultParameter(config, "pluginDescriptor", "${plugin}", null);
        ensureDefaultParameter(config, "projectBuildDirectory", "${project.build.directory}", null);
        ensureDefaultParameter(config, "pluginArtifactMap", null, "${plugin.artifactMap}");
        ensureDefaultParameter(config, "projectArtifactMap", null, "${project.artifactMap}");
        ensureDefaultParameter(config, "classesDirectory", "${project.build.outputDirectory}", null);
        ensureDefaultParameter(config, "testClassesDirectory", "${project.build.testOutputDirectory}", null);
        ensureDefaultParameter(config, "testSourceDirectory", "${project.build.testSourceDirectory}", null);
        ensureDefaultParameter(config, "reportsDirectory", "${project.build.directory}/surefire-reports", null);
        ensureDefaultParameter(config, "workingDirectory", null, "${basedir}");
        ensureDefaultParameter(config, "shutdown", "exit", "exit");

        // Apply dynamic system properties
        if (request.getSystemProperties() != null && !request.getSystemProperties().isEmpty()) {
            Xpp3Dom sysPropsNode = config.getChild("systemPropertyVariables");
            if (sysPropsNode == null) {
                sysPropsNode = new Xpp3Dom("systemPropertyVariables");
                config.addChild(sysPropsNode);
            }
            for (Map.Entry<String, String> entry : request.getSystemProperties().entrySet()) {
                Xpp3Dom propNode = sysPropsNode.getChild(entry.getKey());
                if (propNode == null) {
                    propNode = new Xpp3Dom(entry.getKey());
                    sysPropsNode.addChild(propNode);
                }
                propNode.setValue(entry.getValue());
            }
        }

        // Apply test filter (-Dtest=...)
        if (request.getTestFilter() != null && !request.getTestFilter().isBlank()) {
            Xpp3Dom testNode = config.getChild("test");
            if (testNode == null) {
                testNode = new Xpp3Dom("test");
                config.addChild(testNode);
            }
            testNode.setValue(request.getTestFilter().trim());
        }

        // Apply fail-fast (stop after 1 failure)
        if (request.isFailFast()) {
            Xpp3Dom skipNode = config.getChild("skipAfterFailureCount");
            if (skipNode == null) {
                skipNode = new Xpp3Dom("skipAfterFailureCount");
                config.addChild(skipNode);
            }
            skipNode.setValue("1");
        }

        // Fail-at-end must be disabled so assertion failures immediately surface
        Xpp3Dom ignoreNode = config.getChild("testFailureIgnore");
        if (ignoreNode == null) {
            ignoreNode = new Xpp3Dom("testFailureIgnore");
            config.addChild(ignoreNode);
        }
        ignoreNode.setValue("false");

        MojoExecution mojoExecution = new MojoExecution(mojoDescriptor, "spark-mutator-test");
        mojoExecution.setConfiguration(config);

        // Apply system properties to current JVM process as well (for in-process or bridging paths)
        Map<String, String> previousProps = new HashMap<>();
        if (request.getSystemProperties() != null) {
            for (Map.Entry<String, String> entry : request.getSystemProperties().entrySet()) {
                previousProps.put(entry.getKey(), System.getProperty(entry.getKey()));
                if (entry.getValue() != null) {
                    System.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }

        MavenProject previousProject = mavenSession.getCurrentProject();
        if (project != null && previousProject != project) {
            mavenSession.setCurrentProject(project);
        }

        try {
            if (request.getTimeoutMillis() <= 0L) {
                return executeSynchronously(mojoExecution, start);
            } else {
                return executeWithTimeout(mojoExecution, request.getTimeoutMillis(), start);
            }
        } finally {
            if (project != null && previousProject != null && previousProject != project) {
                mavenSession.setCurrentProject(previousProject);
            }
            for (Map.Entry<String, String> entry : previousProps.entrySet()) {
                if (entry.getValue() != null) {
                    System.setProperty(entry.getKey(), entry.getValue());
                } else {
                    System.clearProperty(entry.getKey());
                }
            }
        }
    }

    private SurefireResult executeSynchronously(MojoExecution mojoExecution, long start) {
        try {
            pluginManager.executeMojo(mavenSession, mojoExecution);
            long elapsed = System.currentTimeMillis() - start;
            return SurefireResult.success(elapsed);
        } catch (MojoFailureException e) {
            long elapsed = System.currentTimeMillis() - start;
            return SurefireResult.failure(elapsed, e.getMessage(), 1);
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - start;
            return SurefireResult.failure(elapsed, e.getMessage(), 2);
        }
    }

    private SurefireResult executeWithTimeout(MojoExecution mojoExecution, long timeoutMillis, long start) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<SurefireResult> future = executor.submit(() -> {
            try {
                pluginManager.executeMojo(mavenSession, mojoExecution);
                long elapsed = System.currentTimeMillis() - start;
                return SurefireResult.success(elapsed);
            } catch (MojoFailureException e) {
                long elapsed = System.currentTimeMillis() - start;
                return SurefireResult.failure(elapsed, e.getMessage(), 1);
            } catch (Exception e) {
                long elapsed = System.currentTimeMillis() - start;
                return SurefireResult.failure(elapsed, e.getMessage(), 2);
            }
        });

        try {
            SurefireResult res = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > timeoutMillis) {
                return SurefireResult.timeout(elapsed, "Surefire execution exceeded timeout (" + timeoutMillis + "ms)");
            }
            return res;
        } catch (TimeoutException e) {
            future.cancel(true);
            long elapsed = System.currentTimeMillis() - start;
            return SurefireResult.timeout(elapsed, "Surefire execution timed out (> " + timeoutMillis + "ms)");
        } catch (ExecutionException e) {
            long elapsed = System.currentTimeMillis() - start;
            Throwable cause = e.getCause();
            return SurefireResult.failure(elapsed, cause != null ? cause.getMessage() : e.getMessage(), 1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long elapsed = System.currentTimeMillis() - start;
            return SurefireResult.failure(elapsed, "Surefire execution interrupted: " + e.getMessage(), 1);
        } finally {
            executor.shutdownNow();
        }
    }

    public BuildPluginManager getPluginManager() {
        return pluginManager;
    }

    public MavenSession getMavenSession() {
        return mavenSession;
    }

    public MavenProject getProject() {
        return project;
    }

    /**
     * Value object describing the parameters for a Surefire execution.
     */
    public static class SurefireRequest {
        private final Map<String, String> systemProperties;
        private final String testFilter;
        private final boolean failFast;
        private final long timeoutMillis;

        public SurefireRequest(Map<String, String> systemProperties, String testFilter, boolean failFast, long timeoutMillis) {
            this.systemProperties = systemProperties != null
                    ? Collections.unmodifiableMap(new HashMap<>(systemProperties))
                    : Collections.emptyMap();
            this.testFilter = testFilter;
            this.failFast = failFast;
            this.timeoutMillis = timeoutMillis;
        }

        public Map<String, String> getSystemProperties() {
            return systemProperties;
        }

        public String getTestFilter() {
            return testFilter;
        }

        public boolean isFailFast() {
            return failFast;
        }

        public long getTimeoutMillis() {
            return timeoutMillis;
        }
    }

    /**
     * Value object capturing the terminal outcome of a Surefire execution.
     */
    public static class SurefireResult {
        private final int exitCode;
        private final long elapsedMillis;
        private final boolean timedOut;
        private final String failureDetail;

        public SurefireResult(int exitCode, long elapsedMillis, boolean timedOut, String failureDetail) {
            this.exitCode = exitCode;
            this.elapsedMillis = elapsedMillis;
            this.timedOut = timedOut;
            this.failureDetail = failureDetail;
        }

        public static SurefireResult success(long elapsedMillis) {
            return new SurefireResult(0, elapsedMillis, false, null);
        }

        public static SurefireResult failure(long elapsedMillis, String failureDetail, int exitCode) {
            return new SurefireResult(exitCode != 0 ? exitCode : 1, elapsedMillis, false, failureDetail);
        }

        public static SurefireResult timeout(long elapsedMillis, String failureDetail) {
            return new SurefireResult(-1, elapsedMillis, true, failureDetail);
        }

        public boolean isSuccess() {
            return exitCode == 0 && !timedOut;
        }

        public boolean isFailure() {
            return exitCode != 0 && !timedOut;
        }

        public boolean isTimeout() {
            return timedOut;
        }

        public int getExitCode() {
            return exitCode;
        }

        public long getElapsedMillis() {
            return elapsedMillis;
        }

        public String getFailureDetail() {
            return failureDetail;
        }
    }

    private static void ensureDefaultParameter(Xpp3Dom config, String name, String defaultValue, String value) {
        if (config.getChild(name) == null) {
            Xpp3Dom node = new Xpp3Dom(name);
            if (defaultValue != null) {
                node.setAttribute("default-value", defaultValue);
            }
            if (value != null) {
                node.setValue(value);
            }
            config.addChild(node);
        }
    }
}
