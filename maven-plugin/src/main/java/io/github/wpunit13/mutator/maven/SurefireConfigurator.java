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
 *   <li>Adds the resolved interceptor JAR to {@code additionalClasspathElements}.</li>
 * </ul>
 */
public class SurefireConfigurator {

    public static final String EXTENSION_PROPERTY = "spark.sql.extensions";
    public static final String EXTENSION_CLASS = "io.github.wpunit13.mutator.MutatorSparkExtension";
    public static final String EXTENSION_ARG = "-D" + EXTENSION_PROPERTY + "=" + EXTENSION_CLASS;

    public static final String SUREFIRE_GROUP_ID = "org.apache.maven.plugins";
    public static final String SUREFIRE_ARTIFACT_ID = "maven-surefire-plugin";
    public static final String SUREFIRE_PLUGIN_KEY = SUREFIRE_GROUP_ID + ":" + SUREFIRE_ARTIFACT_ID;

    public static final String FAILSAFE_GROUP_ID = "org.apache.maven.plugins";
    public static final String FAILSAFE_ARTIFACT_ID = "maven-failsafe-plugin";
    public static final String FAILSAFE_PLUGIN_KEY = FAILSAFE_GROUP_ID + ":" + FAILSAFE_ARTIFACT_ID;

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
        if (surefirePlugin.getExecutions() != null) {
            for (PluginExecution execution : surefirePlugin.getExecutions()) {
                if (execution.getConfiguration() instanceof Xpp3Dom) {
                    updateArgLine((Xpp3Dom) execution.getConfiguration(), null);
                    updateAdditionalClasspathElements((Xpp3Dom) execution.getConfiguration(), interceptorJarPaths);
                }
            }
        }

        // Also configure Failsafe if present
        Plugin failsafePlugin = project.getPlugin(FAILSAFE_PLUGIN_KEY);
        if (failsafePlugin != null) {
            Xpp3Dom failsafeConfig = getOrCreateConfiguration(failsafePlugin);
            updateArgLine(failsafeConfig, null);
            updateAdditionalClasspathElements(failsafeConfig, interceptorJarPaths);
            if (failsafePlugin.getExecutions() != null) {
                for (PluginExecution execution : failsafePlugin.getExecutions()) {
                    if (execution.getConfiguration() instanceof Xpp3Dom) {
                        updateArgLine((Xpp3Dom) execution.getConfiguration(), null);
                        updateAdditionalClasspathElements((Xpp3Dom) execution.getConfiguration(), interceptorJarPaths);
                    }
                }
            }
        }

        // Propagate property to project properties if argLine exists there
        Properties props = project.getProperties();
        if (props != null) {
            props.setProperty(EXTENSION_PROPERTY, EXTENSION_CLASS);
            if (props.containsKey("argLine")) {
                String existing = props.getProperty("argLine");
                if (existing == null || existing.isBlank()) {
                    props.setProperty("argLine", EXTENSION_ARG);
                } else if (!existing.contains(EXTENSION_ARG)) {
                    props.setProperty("argLine", existing.trim() + " " + EXTENSION_ARG);
                }
            }
        }

        return new SurefireConfigResult(configuredArgLine, configuredClasspath);
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
        Object configObj = plugin.getConfiguration();
        if (configObj instanceof Xpp3Dom) {
            return (Xpp3Dom) configObj;
        }
        Xpp3Dom config = new Xpp3Dom("configuration");
        plugin.setConfiguration(config);
        return config;
    }

    private String updateArgLine(Xpp3Dom configuration, Properties properties) {
        Xpp3Dom argLineNode = configuration.getChild("argLine");
        String finalValue;
        if (argLineNode == null) {
            argLineNode = new Xpp3Dom("argLine");
            String base = (properties != null && properties.containsKey("argLine"))
                    ? properties.getProperty("argLine")
                    : null;
            if (base != null && !base.isBlank()) {
                finalValue = base.contains(EXTENSION_ARG) ? base : base.trim() + " " + EXTENSION_ARG;
            } else {
                finalValue = EXTENSION_ARG;
            }
            argLineNode.setValue(finalValue);
            configuration.addChild(argLineNode);
        } else {
            String current = argLineNode.getValue();
            if (current == null || current.isBlank()) {
                finalValue = EXTENSION_ARG;
            } else if (!current.contains(EXTENSION_ARG)) {
                finalValue = current.trim() + " " + EXTENSION_ARG;
            } else {
                finalValue = current;
            }
            argLineNode.setValue(finalValue);
        }
        return finalValue;
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
