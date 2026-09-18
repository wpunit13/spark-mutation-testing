package io.github.wpunit13.mutator.maven;

import org.apache.maven.model.Build;
import org.apache.maven.model.Plugin;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the argLine contract of {@link SurefireConfigurator}: the Spark
 * extension property is always merged in, the mandatory modular-runtime JVM
 * opens are merged in when enabled (and only on Java 9+), injection is
 * idempotent, and pre-existing argLines (e.g. JaCoCo's {@code @{argLine}}
 * placeholder) are preserved, not replaced.
 */
class SurefireConfiguratorTest {

    private MavenProject newProject() {
        MavenProject project = new MavenProject();
        project.setBuild(new Build());
        project.getModel().setProperties(new Properties());
        return project;
    }

    private String argLineOf(MavenProject project) {
        Plugin surefire = project.getPlugin(SurefireConfigurator.SUREFIRE_PLUGIN_KEY);
        Xpp3Dom config = (Xpp3Dom) surefire.getConfiguration();
        Xpp3Dom argLine = config.getChild("argLine");
        return argLine != null ? argLine.getValue() : null;
    }

    @Test
    void injectsExtensionAndJvmOpensOnModularRuntime() {
        MavenProject project = newProject();

        SurefireConfigurator.SurefireConfigResult result =
                new SurefireConfigurator(true).configure(project, "/tmp/interceptor.jar");

        assertTrue(result.getArgLine().contains(SurefireConfigurator.EXTENSION_ARG),
                "the Spark extension must always be in argLine");
        if (SurefireConfigurator.isModularRuntime()) {
            for (String arg : SurefireConfigurator.SPARK_JVM_OPEN_ARGS) {
                assertTrue(result.getArgLine().contains(arg),
                        "modular runtime argLine must carry " + arg);
            }
        } else {
            assertFalse(result.getArgLine().contains("--add-opens"),
                    "pre-9 runtimes must never receive --add-opens");
        }
    }

    @Test
    void optOutSkipsJvmOpensButKeepsExtension() {
        MavenProject project = newProject();

        SurefireConfigurator.SurefireConfigResult result =
                new SurefireConfigurator(false).configure(project, "/tmp/interceptor.jar");

        assertTrue(result.getArgLine().contains(SurefireConfigurator.EXTENSION_ARG));
        assertFalse(result.getArgLine().contains("--add-opens"),
                "injectJvmOpens=false must not append any --add-opens");
        assertFalse(result.getArgLine().contains("IgnoreUnrecognizedVMOptions"),
                "injectJvmOpens=false must not append any JVM opens");
    }

    @Test
    void injectionIsIdempotent() {
        MavenProject project = newProject();
        SurefireConfigurator configurator = new SurefireConfigurator(true);

        configurator.configure(project, "/tmp/interceptor.jar");
        String first = argLineOf(project);
        configurator.configure(project, "/tmp/interceptor.jar");
        String second = argLineOf(project);

        assertEquals(first, second, "re-configuration must not duplicate argLine tokens");
    }

    @Test
    void existingArgLineIsMergedNotReplaced() {
        MavenProject project = newProject();
        String jacoco = "@{argLine} -Xmx2g";
        project.getProperties().setProperty("argLine", jacoco);

        new SurefireConfigurator(true).configure(project, "/tmp/interceptor.jar");

        String merged = argLineOf(project);
        assertTrue(merged.startsWith(jacoco), "the user's argLine must be preserved verbatim");
        assertTrue(merged.contains(SurefireConfigurator.EXTENSION_ARG));
    }
}
