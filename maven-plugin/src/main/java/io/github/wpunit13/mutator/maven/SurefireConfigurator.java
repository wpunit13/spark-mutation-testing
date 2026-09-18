package io.github.wpunit13.mutator.maven;

import org.apache.maven.model.Build;
import org.apache.maven.model.Plugin;
import org.apache.maven.model.PluginExecution;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Configures the Surefire plugin execution in the target project:
 * <ul>
 *   <li>Appends {@code -Dspark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension}
 *       to {@code argLine}.</li>
 *   <li>Appends the JVM arguments Spark requires on modular runtimes (Java 9+):
 *       the {@code --add-opens} set plus the Netty/reflect flags. Without these
 *       the Spark driver dies with {@code InaccessibleObjectException} before
 *       any test runs. Nothing is injected on Java 8 (the flags do not exist
 *       there); pass {@code injectJvmOpens=false} to opt out (e.g. fork JDK
 *       managed via toolchains).</li>
 *   <li>Adds the resolved interceptor JAR to {@code additionalClasspathElements}.</li>
 * </ul>
 */
public class SurefireConfigurator {

    public static final String EXTENSION_PROPERTY = "spark.sql.extensions";
    public static final String EXTENSION_CLASS = "io.github.wpunit13.mutator.MutatorSparkExtension";
    public static final String EXTENSION_ARG = "-D" + EXTENSION_PROPERTY + "=" + EXTENSION_CLASS;

    /**
     * JVM args Spark needs on modular runtimes (Java 9+), verbatim from the
     * root POM's {@code spark.test.jvm.args} minus {@code -Xmx2g} (which is
     * project-specific and must not be imposed downstream).
     * {@code -XX:+IgnoreUnrecognizedVMOptions} first keeps the set
     * forward-compatible across JDK releases.
     */
    static final List<String> SPARK_JVM_OPEN_ARGS = List.of(
            "-XX:+IgnoreUnrecognizedVMOptions",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
            "-Djdk.reflect.useDirectMethodHandle=false",
            "-Dio.netty.tryReflectionSetAccessible=true");

    private final boolean injectJvmOpens;

    /** Creates a configurator with JVM-opens injection enabled (the default). */
    public SurefireConfigurator() {
        this(true);
    }

    /**
     * @param injectJvmOpens when true, the mandatory modular-runtime JVM args
     *                       (see {@link #SPARK_JVM_OPEN_ARGS}) are merged into
     *                       every configured {@code argLine} on Java 9+.
     */
    public SurefireConfigurator(boolean injectJvmOpens) {
        this.injectJvmOpens = injectJvmOpens;
    }

    /**
     * True when the current JVM implements the module system (Java 9+), the
     * only runtimes that accept {@code --add-opens}. Reads
     * {@code java.specification.version}, handling both the legacy
     * {@code 1.8} format and the modern {@code 17}/{@code 21} format.
     * Package-private seam for tests.
     */
    static boolean isModularRuntime() {
        String spec = System.getProperty("java.specification.version", "1.8");
        if (spec.indexOf('.') > 0) {
            return false; // legacy 1.x format => pre-9
        }
        try {
            return Integer.parseInt(spec) >= 9;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    public static final String SUREFIRE_GROUP_ID = "org.apache.maven.plugins";
    public static final String SUREFIRE_ARTIFACT_ID = "maven-surefire-plugin";
    public static final String SUREFIRE_PLUGIN_KEY = SUREFIRE_GROUP_ID + ":" + SUREFIRE_ARTIFACT_ID;

    public static final String FAILSAFE_GROUP_ID = "org.apache.maven.plugins";
    public static final String FAILSAFE_ARTIFACT_ID = "maven-failsafe-plugin";
    public static final String FAILSAFE_PLUGIN_KEY = FAILSAFE_GROUP_ID + ":" + FAILSAFE_ARTIFACT_ID;

    private static final String ARG_LINE = "argLine";

    /**
     * Configures Surefire for the given project using the resolved interceptor JAR file.
     */
    public SurefireConfigResult configure(MavenProject project, File interceptorJar) {
        Objects.requireNonNull(interceptorJar, "interceptorJar must not be null");
        return configure(project, interceptorJar.getAbsolutePath());
    }

    /**
     * Configures Surefire for the given project using the resolved interceptor JAR path.
     */
    public SurefireConfigResult configure(MavenProject project, String interceptorJarPath) {
        Objects.requireNonNull(interceptorJarPath, "interceptorJarPath must not be null");
        return configure(project, Collections.singletonList(interceptorJarPath));
    }

    /**
     * Configures Surefire for the given project using a list of resolved JAR paths.
     */
    public SurefireConfigResult configure(MavenProject project, List<String> interceptorJarPaths) {
        if (project == null) {
            throw new IllegalArgumentException("MavenProject must not be null");
        }
        if (interceptorJarPaths == null || interceptorJarPaths.isEmpty()) {
            throw new IllegalArgumentException("interceptorJarPaths must not be null or empty");
        }

        // Configure Surefire
        Plugin surefirePlugin = findOrCreatePlugin(project, SUREFIRE_GROUP_ID, SUREFIRE_ARTIFACT_ID);
        Xpp3Dom surefireConfig = getOrCreateConfiguration(surefirePlugin);
        String configuredArgLine = updateArgLine(surefireConfig, project.getProperties());
        List<String> configuredClasspath = updateAdditionalClasspathElements(surefireConfig, interceptorJarPaths);

        // Update any executions defined on the Surefire plugin
        configurePluginExecutions(surefirePlugin, interceptorJarPaths);

        // Also configure Failsafe if present
        configureFailsafeIfPresent(project, interceptorJarPaths);

        // Propagate property to project properties if argLine exists there
        propagateExtensionProperty(project.getProperties());

        return new SurefireConfigResult(configuredArgLine, configuredClasspath);
    }

    private void configurePluginExecutions(Plugin plugin, List<String> interceptorJarPaths) {
        if (plugin.getExecutions() == null) {
            return;
        }
        for (PluginExecution execution : plugin.getExecutions()) {
            if (execution.getConfiguration() instanceof Xpp3Dom executionConfig) {
                updateArgLine(executionConfig, null);
                updateAdditionalClasspathElements(executionConfig, interceptorJarPaths);
            }
        }
    }

    private void configureFailsafeIfPresent(MavenProject project, List<String> interceptorJarPaths) {
        Plugin failsafePlugin = project.getPlugin(FAILSAFE_PLUGIN_KEY);
        if (failsafePlugin == null) {
            return;
        }
        Xpp3Dom failsafeConfig = getOrCreateConfiguration(failsafePlugin);
        updateArgLine(failsafeConfig, null);
        updateAdditionalClasspathElements(failsafeConfig, interceptorJarPaths);
        configurePluginExecutions(failsafePlugin, interceptorJarPaths);
    }

    private void propagateExtensionProperty(Properties properties) {
        if (properties == null) {
            return;
        }
        properties.setProperty(EXTENSION_PROPERTY, EXTENSION_CLASS);
        if (properties.containsKey(ARG_LINE)) {
            String existing = properties.getProperty(ARG_LINE);
            properties.setProperty(ARG_LINE, mergeArgs(existing));
        }
    }

    private Plugin findOrCreatePlugin(MavenProject project, String groupId, String artifactId) {
        String key = groupId + ":" + artifactId;
        Plugin plugin = project.getPlugin(key);
        if (plugin == null) {
            plugin = new Plugin();
            plugin.setGroupId(groupId);
            plugin.setArtifactId(artifactId);
            if (project.getBuild() == null) {
                project.setBuild(new Build());
            }
            project.getBuild().addPlugin(plugin);
            project.getBuild().flushPluginMap();
        }
        return plugin;
    }

    private Xpp3Dom getOrCreateConfiguration(Plugin plugin) {
        if (plugin.getConfiguration() instanceof Xpp3Dom existingConfig) {
            return existingConfig;
        }
        Xpp3Dom config = new Xpp3Dom("configuration");
        plugin.setConfiguration(config);
        return config;
    }

    private String updateArgLine(Xpp3Dom configuration, Properties properties) {
        Xpp3Dom argLineNode = configuration.getChild(ARG_LINE);
        if (argLineNode == null) {
            String base = (properties != null && properties.containsKey(ARG_LINE))
                    ? properties.getProperty(ARG_LINE)
                    : null;
            String finalValue = mergeArgs(base);
            argLineNode = new Xpp3Dom(ARG_LINE);
            argLineNode.setValue(finalValue);
            configuration.addChild(argLineNode);
            return finalValue;
        }
        String finalValue = mergeArgs(argLineNode.getValue());
        argLineNode.setValue(finalValue);
        return finalValue;
    }

    /**
     * Merges the extension arg (and, when enabled and on a modular runtime,
     * the mandatory Spark JVM opens) into an argLine value. Blank values are
     * replaced outright; each token is appended only if not already present,
     * so repeated configuration is idempotent.
     */
    private String mergeArgs(String existing) {
        String merged = mergeToken(existing, EXTENSION_ARG);
        if (injectJvmOpens && isModularRuntime()) {
            for (String arg : SPARK_JVM_OPEN_ARGS) {
                merged = mergeToken(merged, arg);
            }
        }
        return merged;
    }

    private static String mergeToken(String existing, String token) {
        if (existing == null || existing.isBlank()) {
            return token;
        }
        if (existing.contains(token)) {
            return existing;
        }
        return existing.trim() + " " + token;
    }

    private List<String> updateAdditionalClasspathElements(Xpp3Dom configuration, List<String> jarPaths) {
        Xpp3Dom elementsNode = configuration.getChild("additionalClasspathElements");
        if (elementsNode == null) {
            elementsNode = new Xpp3Dom("additionalClasspathElements");
            configuration.addChild(elementsNode);
        }

        Set<String> existingElements = new LinkedHashSet<>();
        Xpp3Dom[] children = elementsNode.getChildren("additionalClasspathElement");
        if (children != null) {
            for (Xpp3Dom child : children) {
                if (child.getValue() != null) {
                    existingElements.add(child.getValue().trim());
                }
            }
        }

        for (String path : jarPaths) {
            if (path != null && !path.isBlank() && !existingElements.contains(path.trim())) {
                Xpp3Dom elementNode = new Xpp3Dom("additionalClasspathElement");
                elementNode.setValue(path.trim());
                elementsNode.addChild(elementNode);
                existingElements.add(path.trim());
            }
        }

        return new ArrayList<>(existingElements);
    }

    /**
     * Value object describing the parameters configured on Surefire.
     */
    public static class SurefireConfigResult {
        private final String argLine;
        private final List<String> additionalClasspathElements;

        public SurefireConfigResult(String argLine, List<String> additionalClasspathElements) {
            this.argLine = argLine;
            this.additionalClasspathElements =
                    Collections.unmodifiableList(new ArrayList<>(additionalClasspathElements));
        }

        public String getArgLine() {
            return argLine;
        }

        public List<String> getAdditionalClasspathElements() {
            return additionalClasspathElements;
        }

        @Override
        public String toString() {
            return "SurefireConfigResult{" +
                    "argLine='" + argLine + '\'' +
                    ", additionalClasspathElements=" + additionalClasspathElements +
                    '}';
        }
    }
}
