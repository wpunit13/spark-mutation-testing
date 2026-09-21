package io.github.wpunit13.mutator.maven;

import org.apache.maven.execution.MavenSession;
import org.apache.maven.model.Plugin;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecution;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.MojoDescriptor;
import org.apache.maven.plugin.descriptor.Parameter;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.eclipse.aether.repository.RemoteRepository;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
import java.util.stream.Stream;

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

        Plugin surefirePlugin = resolveSurefirePlugin();
        List<RemoteRepository> repos = project.getRemotePluginRepositories() != null
                ? project.getRemotePluginRepositories()
                : Collections.emptyList();

        MojoDescriptor mojoDescriptor = loadMojoDescriptor(surefirePlugin, repos);
        Xpp3Dom config = buildConfiguration(surefirePlugin, mojoDescriptor, request);

        MojoExecution mojoExecution = new MojoExecution(mojoDescriptor, "spark-mutator-test");
        mojoExecution.setConfiguration(config);

        // Apply system properties to current JVM process as well (for in-process or bridging paths)
        Map<String, String> previousProps = applySystemProperties(request.getSystemProperties());

        MavenProject previousProject = mavenSession.getCurrentProject();
        if (previousProject != project) {
            mavenSession.setCurrentProject(project);
        }

        Path reportsDir = surefireReportsDirectory(request);
        cleanSurefireXmlReports(reportsDir);

        SurefireResult result;
        try {
            if (request.getTimeoutMillis() <= 0L) {
                result = executeSynchronously(mojoExecution, start);
            } else {
                result = executeWithTimeout(mojoExecution, request.getTimeoutMillis(), start);
            }
        } finally {
            if (previousProject != null && previousProject != project) {
                mavenSession.setCurrentProject(previousProject);
            }
            restoreSystemProperties(previousProps);
        }
        return attachTestResults(result, reportsDir);
    }

    /** The fork's XML report directory: surefire's default
     * ${project.build.directory}/surefire-reports (mirrors the descriptor
     * default this class injects in buildConfiguration), suffixed per worker
     * when the request carries an isolation suffix. */
    private Path surefireReportsDirectory(SurefireRequest request) {
        String buildDir = project.getBuild() != null && project.getBuild().getDirectory() != null
                ? project.getBuild().getDirectory()
                : "target";
        String suffix = request.getWorkerSuffix() == null ? "" : request.getWorkerSuffix();
        return Path.of(buildDir, "surefire-reports" + suffix);
    }

    /** Deletes stale TEST-*.xml from a previous fork so the post-run parse
      * only sees THIS fork's tests (surefire may run a subset per mutant). */
    private static void cleanSurefireXmlReports(Path reportsDir) {
        if (reportsDir == null || !Files.isDirectory(reportsDir)) {
            return;
        }
        try (Stream<Path> files = Files.list(reportsDir)) {
            files.filter(p -> p.getFileName().toString().startsWith("TEST-")
                            && p.getFileName().toString().endsWith(".xml"))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                            // stale report left behind: the parse below may then
                            // see a previous fork's tests; harmless, best-effort
                        }
                    });
        } catch (IOException ignored) {
            // best-effort cleanup
        }
    }

    /** Best-effort enrichment: parses the fork's XML reports and attaches the
      * executed + failing test ids. A parse failure returns the result
      * unchanged — attribution is a report-quality feature, never worth
      * failing the loop over. */
    private static SurefireResult attachTestResults(SurefireResult result, Path reportsDir) {
        if (reportsDir == null) {
            return result;
        }
        try {
            SurefireTestResults parsed = parseSurefireReports(reportsDir);
            SurefireResult enriched = result.withTestResults(
                    parsed.executedTestIds(), parsed.failedTestIds(), parsed.testTimeMillis());
            if (!parsed.errorDetails().isEmpty()) {
                enriched = enriched.withFailureDetailEnriched(parsed.errorDetails());
            }
            return enriched;
        } catch (Exception e) {
            return result;
        }
    }

    /** Parsed view of one fork's surefire XML reports. testTimeMillis sums
      * the testsuite {@code time} attributes (suite wall time incl. setup),
      * the fork's non-orchestration time — its complement against the fork's
      * wall clock is the fixed per-fork overhead (JVM spawn + booter + teardown).
      * errorDetails carries the first test exception's rendered chain (message
      * attribute + stack text) per report file — the structural-invalidation
      * markers live in nested causes that the surefire mojo's own summary
      * message never surfaces. */
    record SurefireTestResults(List<String> executedTestIds, List<String> failedTestIds,
                               long testTimeMillis, List<String> errorDetails) {
    }

    /** Parses every TEST-*.xml in {@code reportsDir}. Executed = every
      * testcase element; failed = testcases with a failure/error child.
      * Ids are {@code classname.methodName}, sorted for determinism. */
    static SurefireTestResults parseSurefireReports(Path reportsDir) throws IOException {
        List<String> executed = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        List<String> errorDetails = new ArrayList<>();
        double suiteTimeSeconds = 0.0;
        if (reportsDir == null || !Files.isDirectory(reportsDir)) {
            return new SurefireTestResults(List.of(), List.of(), 0L, List.of());
        }
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            // The reports are locally written files; refuse DTDs anyway (XXE hardening).
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        } catch (Exception ignored) {
            // feature unsupported on this parser: proceed, input is trusted
        }
        try (Stream<Path> files = Files.list(reportsDir)) {
            List<Path> xmls = files
                    .filter(p -> p.getFileName().toString().startsWith("TEST-")
                            && p.getFileName().toString().endsWith(".xml"))
                    .sorted()
                    .toList();
            for (Path xml : xmls) {
                try {
                    Element suite = factory.newDocumentBuilder().parse(xml.toFile()).getDocumentElement();
                    String timeAttr = suite.getAttribute("time");
                    if (!timeAttr.isBlank()) {
                        suiteTimeSeconds += Double.parseDouble(timeAttr);
                    }
                    NodeList testcases = suite.getElementsByTagName("testcase");
                    for (int i = 0; i < testcases.getLength(); i++) {
                        Element tc = (Element) testcases.item(i);
                        String classname = tc.getAttribute("classname");
                        String name = tc.getAttribute("name");
                        String id = (classname.isEmpty() ? "" : classname + ".") + name;
                        if (id.isBlank()) {
                            continue;
                        }
                        executed.add(id);
                        if (tc.getElementsByTagName("failure").getLength() > 0
                                || tc.getElementsByTagName("error").getLength() > 0) {
                            failed.add(id);
                            String detail = firstExceptionDetail(tc);
                            if (detail != null && errorDetails.size() < 8) {
                                errorDetails.add(detail);
                            }
                        }
                    }
                } catch (Exception e) {
                    // malformed/partial XML (e.g. a fork killed mid-write):
                    // skip the file, keep what earlier files contributed
                }
            }
        }
        executed.sort(String::compareTo);
        failed.sort(String::compareTo);
        return new SurefireTestResults(List.copyOf(executed), List.copyOf(failed),
                Math.round(suiteTimeSeconds * 1000.0), List.copyOf(errorDetails));
    }

    /** The first {@code <error>}/{@code <failure>} child's rendered exception
      * chain: {@code type: message} plus the stack text (which carries the
      * {@code Caused by:} chain the structural-invalidation matcher needs).
      * Truncated to keep the outcome file bounded. */
    private static String firstExceptionDetail(Element testcase) {
        NodeList errors = testcase.getElementsByTagName("error");
        if (errors.getLength() == 0) {
            errors = testcase.getElementsByTagName("failure");
        }
        if (errors.getLength() == 0) {
            return null;
        }
        Element e = (Element) errors.item(0);
        String type = e.getAttribute("type");
        String message = e.getAttribute("message");
        String stack = e.getTextContent();
        String combined = (type.isBlank() ? "" : type + ": ")
                + (message.isBlank() ? "" : message + "\n")
                + (stack == null ? "" : stack);
        return combined.length() > 8000 ? combined.substring(0, 4000) : combined;
    }

    private Plugin resolveSurefirePlugin() {
        Plugin surefirePlugin = project.getPlugin(SUREFIRE_GROUP_ID + ":" + SUREFIRE_ARTIFACT_ID);
        if (surefirePlugin == null) {
            surefirePlugin = new Plugin();
            surefirePlugin.setGroupId(SUREFIRE_GROUP_ID);
            surefirePlugin.setArtifactId(SUREFIRE_ARTIFACT_ID);
            surefirePlugin.setVersion(SUREFIRE_DEFAULT_VERSION);
        } else if (surefirePlugin.getVersion() == null || surefirePlugin.getVersion().isBlank()) {
            surefirePlugin.setVersion(SUREFIRE_DEFAULT_VERSION);
        }
        return surefirePlugin;
    }

    private MojoDescriptor loadMojoDescriptor(Plugin surefirePlugin, List<RemoteRepository> repos)
            throws MojoExecutionException {
        try {
            pluginManager.loadPlugin(surefirePlugin, repos, mavenSession.getRepositorySession());
            return pluginManager.getMojoDescriptor(
                    surefirePlugin,
                    "test",
                    repos,
                    mavenSession.getRepositorySession()
            );
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to obtain MojoDescriptor for maven-surefire-plugin:test", e);
        }
    }

    private Xpp3Dom buildConfiguration(Plugin surefirePlugin, MojoDescriptor mojoDescriptor, SurefireRequest request) {
        Xpp3Dom config = new Xpp3Dom("configuration");
        if (surefirePlugin.getConfiguration() instanceof Xpp3Dom pluginConfig) {
            config = new Xpp3Dom(pluginConfig);
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

        applyDescriptorDefaults(config, mojoDescriptor);
        applyRequestOptions(config, request);
        applyWorkerIsolation(config, request);

        return config;
    }

    /** Concurrent-worker isolation (packet fix list): workers in one module
      * must not share surefire's booter-jar temp dir or the XML report dir —
      * a concurrent cleanup deletes the other fork's booter jar mid-launch
      * (fork dies, "Unable to access jarfile", 0 tests, NOT_APPLIED). Spark
      * and Derby state are split per worker via the argLine channel. */
    private void applyWorkerIsolation(Xpp3Dom config, SurefireRequest request) {
        String suffix = request.getWorkerSuffix();
        if (suffix == null) {
            return;
        }
        setOrAdd(config, "tempDir", "surefire" + suffix);
        setOrAdd(config, "reportsDirectory", "${project.build.directory}/surefire-reports" + suffix);

        String buildDir = project.getBuild() != null && project.getBuild().getDirectory() != null
                ? project.getBuild().getDirectory()
                : "target";
        String extras = " -Dspark.sql.warehouse.dir=" + buildDir + "/spark-warehouse" + suffix
                + " -Dderby.system.home=" + buildDir + "/derby" + suffix;
        Xpp3Dom argLine = config.getChild("argLine");
        if (argLine == null) {
            argLine = new Xpp3Dom("argLine");
            config.addChild(argLine);
            argLine.setValue(extras.trim());
        } else {
            argLine.setValue(argLine.getValue() + extras);
        }
    }

    private static void setOrAdd(Xpp3Dom config, String name, String value) {
        Xpp3Dom node = config.getChild(name);
        if (node == null) {
            node = new Xpp3Dom(name);
            config.addChild(node);
        }
        node.setValue(value);
    }

    /**
     * Bridges EVERY remaining mojo parameter default straight from the loaded
     * plugin descriptor. Programmatic invocation bypasses the lifecycle binder
     * that normally populates these, and surefire 3.x validates some (e.g.
     * tempDir) as non-blank — a hand-maintained list can never be complete, so
     * derive it from the descriptor instead.
     */
    private void applyDescriptorDefaults(Xpp3Dom config, MojoDescriptor mojoDescriptor) {
        for (Parameter p : mojoDescriptor.getParameters()) {
            String def = p.getDefaultValue() != null ? p.getDefaultValue() : p.getExpression();
            if (config.getChild(p.getName()) != null || def == null || def.isBlank()) {
                continue;
            }
            Xpp3Dom node = new Xpp3Dom(p.getName());
            node.setAttribute("default-value", def);
            config.addChild(node);
        }
    }

    private void applyRequestOptions(Xpp3Dom config, SurefireRequest request) {
        applySystemPropertyVariables(config, request.getSystemProperties());
        applyTestFilter(config, request.getTestFilter());
        applyFailFast(config, request.isFailFast());

        // Fail-at-end must be disabled so assertion failures immediately surface
        Xpp3Dom ignoreNode = config.getChild("testFailureIgnore");
        if (ignoreNode == null) {
            ignoreNode = new Xpp3Dom("testFailureIgnore");
            config.addChild(ignoreNode);
        }
        ignoreNode.setValue("false");
    }

    private void applySystemPropertyVariables(Xpp3Dom config, Map<String, String> systemProperties) {
        // Apply dynamic system properties
        if (systemProperties == null || systemProperties.isEmpty()) {
            return;
        }
        Xpp3Dom sysPropsNode = config.getChild("systemPropertyVariables");
        if (sysPropsNode == null) {
            sysPropsNode = new Xpp3Dom("systemPropertyVariables");
            config.addChild(sysPropsNode);
        }
        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            Xpp3Dom propNode = sysPropsNode.getChild(entry.getKey());
            if (propNode == null) {
                propNode = new Xpp3Dom(entry.getKey());
                sysPropsNode.addChild(propNode);
            }
            propNode.setValue(entry.getValue());
        }
    }

    private void applyTestFilter(Xpp3Dom config, String testFilter) {
        // Apply test filter (-Dtest=...)
        if (testFilter == null || testFilter.isBlank()) {
            return;
        }
        Xpp3Dom testNode = config.getChild("test");
        if (testNode == null) {
            testNode = new Xpp3Dom("test");
            config.addChild(testNode);
        }
        testNode.setValue(testFilter.trim());
    }

    private void applyFailFast(Xpp3Dom config, boolean failFast) {
        // Apply fail-fast (stop after 1 failure)
        if (!failFast) {
            return;
        }
        Xpp3Dom skipNode = config.getChild("skipAfterFailureCount");
        if (skipNode == null) {
            skipNode = new Xpp3Dom("skipAfterFailureCount");
            config.addChild(skipNode);
        }
        skipNode.setValue("1");
    }

    private Map<String, String> applySystemProperties(Map<String, String> systemProperties) {
        Map<String, String> previousProps = new HashMap<>();
        if (systemProperties != null) {
            for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
                previousProps.put(entry.getKey(), System.getProperty(entry.getKey()));
                if (entry.getValue() != null) {
                    System.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }
        return previousProps;
    }

    private void restoreSystemProperties(Map<String, String> previousProps) {
        for (Map.Entry<String, String> entry : previousProps.entrySet()) {
            if (entry.getValue() != null) {
                System.setProperty(entry.getKey(), entry.getValue());
            } else {
                System.clearProperty(entry.getKey());
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
        try {
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
            }
        } finally {
            executor.shutdown();
            // interrupts a task still running after a timeout or external interrupt
            executor.shutdownNow();
            try {
                // bounded reaping; a wedged fork thread is abandoned after the grace period
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
        /** Per-worker isolation suffix (concurrent shard workers in one module);
          * null = default shared surefire dirs. */
        private final String workerSuffix;

        public SurefireRequest(Map<String, String> systemProperties, String testFilter, boolean failFast, long timeoutMillis) {
            this(systemProperties, testFilter, failFast, timeoutMillis, null);
        }

        public SurefireRequest(Map<String, String> systemProperties, String testFilter, boolean failFast,
                               long timeoutMillis, String workerSuffix) {
            this.systemProperties = systemProperties != null
                    ? Collections.unmodifiableMap(new HashMap<>(systemProperties))
                    : Collections.emptyMap();
            this.testFilter = testFilter;
            this.failFast = failFast;
            this.timeoutMillis = timeoutMillis;
            this.workerSuffix = workerSuffix;
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

        public String getWorkerSuffix() {
            return workerSuffix;
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
        private final List<String> executedTestIds;
        private final List<String> failedTestIds;
        private final long testTimeMillis;

        public SurefireResult(int exitCode, long elapsedMillis, boolean timedOut, String failureDetail) {
            this(exitCode, elapsedMillis, timedOut, failureDetail, List.of(), List.of(), 0L);
        }

        private SurefireResult(int exitCode, long elapsedMillis, boolean timedOut, String failureDetail,
                               List<String> executedTestIds, List<String> failedTestIds, long testTimeMillis) {
            this.exitCode = exitCode;
            this.elapsedMillis = elapsedMillis;
            this.timedOut = timedOut;
            this.failureDetail = failureDetail;
            this.executedTestIds = executedTestIds == null ? List.of() : List.copyOf(executedTestIds);
            this.failedTestIds = failedTestIds == null ? List.of() : List.copyOf(failedTestIds);
            this.testTimeMillis = testTimeMillis;
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

        /** Copy with the fork's parsed test attribution. */
        public SurefireResult withTestResults(List<String> executedTestIds, List<String> failedTestIds) {
            return withTestResults(executedTestIds, failedTestIds, 0L);
        }

        /** Copy with the fork's parsed test attribution and suite test time. */
        public SurefireResult withTestResults(List<String> executedTestIds, List<String> failedTestIds,
                                              long testTimeMillis) {
            return new SurefireResult(exitCode, elapsedMillis, timedOut, failureDetail,
                    executedTestIds, failedTestIds, testTimeMillis);
        }

        /** Appends the parsed test exception chain to the failure detail when
          * not already present. The mojo's own summary message ("There are test
          * failures") never carries the nested causes the structural
          * invalidation matcher needs; the surefire XML does. */
        public SurefireResult withFailureDetailEnriched(List<String> errorDetails) {
            String joined = String.join("\n", errorDetails);
            if (failureDetail != null && failureDetail.contains(joined)) {
                return this;
            }
            String enriched = (failureDetail == null ? "" : failureDetail + "\n") + joined;
            return new SurefireResult(exitCode, elapsedMillis, timedOut, enriched,
                    executedTestIds, failedTestIds, testTimeMillis);
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

        /** Every test that ran against this mutant (fork path: the whole suite). */
        public List<String> getExecutedTestIds() {
            return executedTestIds;
        }

        /** The tests whose failure killed the mutant. */
        public List<String> getFailedTestIds() {
            return failedTestIds;
        }

        /** Sum of the fork's surefire suite times (0 when unparseable). */
        public long getTestTimeMillis() {
            return testTimeMillis;
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
