package io.github.wpunit13.mutator.maven;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.model.Build;
import org.apache.maven.model.Plugin;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.util.xml.Xpp3Dom;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResult;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SparkVersionDetector}, {@link SurefireConfigurator}, and {@link MutateMojo}.
 */
class SparkVersionDetectorTest {

    private Artifact createArtifact(String groupId, String artifactId, String version) {
        return new DefaultArtifact(
                groupId,
                artifactId,
                version,
                "test",
                "jar",
                null,
                new DefaultArtifactHandler("jar")
        );
    }

    @Test
    void testSpark35Scala213SupportedAndMapped() throws MojoExecutionException {
        SparkVersionDetector detector = new SparkVersionDetector();
        Set<Artifact> artifacts = new HashSet<>();
        artifacts.add(createArtifact("org.apache.spark", "spark-sql_2.13", "3.5.3"));
        artifacts.add(createArtifact("org.apache.spark", "spark-catalyst_2.13", "3.5.3"));

        SparkVersionDetector.InterceptorCoordinate coordinate = detector.detect(artifacts);

        assertNotNull(coordinate);
        assertEquals("io.github.wpunit13", coordinate.getGroupId());
        assertEquals("interceptor-spark-3.5_2.13", coordinate.getArtifactId());
        assertEquals("1.0.0-SNAPSHOT", coordinate.getVersion());
        assertEquals("3.5.3", coordinate.getSparkVersion());
        assertEquals("3.5", coordinate.getSparkMinor());
        assertEquals("2.13", coordinate.getScalaVersion());
        assertEquals("3.5_2.13", coordinate.getKey());
        assertEquals("io.github.wpunit13:interceptor-spark-3.5_2.13:1.0.0-SNAPSHOT", coordinate.getCoordinate());
        assertEquals("io.github.wpunit13:interceptor-spark-3.5_2.13:1.0.0-SNAPSHOT", coordinate.toString());
        assertEquals("io.github.wpunit13:interceptor-spark-3.5_2.13:1.0.0-SNAPSHOT", detector.detectCoordinate(artifacts));
    }

    @Test
    void testSpark35Scala212SupportedAndMapped() throws MojoExecutionException {
        SparkVersionDetector detector = new SparkVersionDetector();
        Set<Artifact> artifacts = Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.12", "3.5.1")
        );

        SparkVersionDetector.InterceptorCoordinate coordinate = detector.detect(artifacts);

        assertNotNull(coordinate);
        assertEquals("interceptor-spark-3.5_2.12", coordinate.getArtifactId());
        assertEquals("io.github.wpunit13:interceptor-spark-3.5_2.12:1.0.0-SNAPSHOT", coordinate.getCoordinate());
    }

    @Test
    void testUnsupportedSparkVersion24ThrowsMojoExecutionException() {
        SparkVersionDetector detector = new SparkVersionDetector();
        Set<Artifact> artifacts = Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.12", "2.4.8")
        );

        MojoExecutionException ex = assertThrows(
                MojoExecutionException.class,
                () -> detector.detect(artifacts)
        );

        assertTrue(ex.getMessage().contains("Unsupported Spark/Scala combination '2.4_2.12'"),
                "Message should state unsupported combination: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("Supported versions:"),
                "Message should state supported versions: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("3.5_2.12") && ex.getMessage().contains("3.5_2.13"),
                "Message should list all supported versions: " + ex.getMessage());
    }

    @Test
    void testUnsupportedSparkVersion34ThrowsMojoExecutionException() {
        SparkVersionDetector detector = new SparkVersionDetector();
        Set<Artifact> artifacts = Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.12", "3.4.1")
        );

        MojoExecutionException ex = assertThrows(
                MojoExecutionException.class,
                () -> detector.detect(artifacts)
        );

        assertTrue(ex.getMessage().contains("Unsupported Spark/Scala combination '3.4_2.12'"));
    }

    @Test
    void testUnsupportedFutureSparkVersionThrowsMojoExecutionException() {
        SparkVersionDetector detector = new SparkVersionDetector();
        Set<Artifact> artifacts = Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.13", "4.0.0")
        );

        MojoExecutionException ex = assertThrows(
                MojoExecutionException.class,
                () -> detector.detect(artifacts)
        );

        assertTrue(ex.getMessage().contains("Unsupported Spark/Scala combination '4.0_2.13'"));
    }

    @Test
    void testMissingSparkSqlDependencyThrowsMojoExecutionException() {
        SparkVersionDetector detector = new SparkVersionDetector();
        Set<Artifact> artifacts = Collections.singleton(
                createArtifact("org.apache.spark", "spark-core_2.13", "3.5.3")
        );

        MojoExecutionException ex = assertThrows(
                MojoExecutionException.class,
                () -> detector.detect(artifacts)
        );

        assertTrue(ex.getMessage().contains("No org.apache.spark:spark-sql_<scala_version> dependency found"));
    }

    @Test
    void testEmptyArtifactsThrowsMojoExecutionException() {
        SparkVersionDetector detector = new SparkVersionDetector();
        MojoExecutionException ex = assertThrows(
                MojoExecutionException.class,
                () -> detector.detect(Collections.emptySet())
        );
        assertTrue(ex.getMessage().contains("No artifacts found on test classpath"));
    }

    @Test
    void testNullArtifactsThrowsMojoExecutionException() {
        SparkVersionDetector detector = new SparkVersionDetector();
        assertThrows(MojoExecutionException.class, () -> detector.detect((Set<Artifact>) null));
    }

    @Test
    void testMalformedSparkVersionThrowsMojoExecutionException() {
        SparkVersionDetector detector = new SparkVersionDetector();
        Set<Artifact> artifacts = Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.13", "invalid")
        );

        MojoExecutionException ex = assertThrows(
                MojoExecutionException.class,
                () -> detector.detect(artifacts)
        );
        assertTrue(ex.getMessage().contains("Could not parse Spark version string"));
    }

    @Test
    void testCustomPluginVersionMapping() throws MojoExecutionException {
        SparkVersionDetector detector = new SparkVersionDetector("2.1.0");
        Set<Artifact> artifacts = Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.13", "3.5.3")
        );

        SparkVersionDetector.InterceptorCoordinate coordinate = detector.detect(artifacts);
        assertEquals("io.github.wpunit13:interceptor-spark-3.5_2.13:2.1.0", coordinate.getCoordinate());
    }

    @Test
    void testDetectWithMavenProject() throws MojoExecutionException {
        SparkVersionDetector detector = new SparkVersionDetector();
        MavenProject project = new MavenProject();
        project.setArtifacts(Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.13", "3.5.3")
        ));

        SparkVersionDetector.InterceptorCoordinate coordinate = detector.detect(project);
        assertEquals("io.github.wpunit13:interceptor-spark-3.5_2.13:1.0.0-SNAPSHOT", coordinate.getCoordinate());
    }

    @Test
    void testSurefireConfiguratorAddsExtensionAndClasspath() throws IOException {
        SurefireConfigurator configurator = new SurefireConfigurator();
        MavenProject project = new MavenProject();
        Build build = new Build();
        project.setBuild(build);

        Path tempJar = Files.createTempFile("interceptor-spark-3.5_2.13-", ".jar");
        try {
            File jarFile = tempJar.toFile();

            SurefireConfigurator.SurefireConfigResult result = configurator.configure(project, jarFile);

            assertNotNull(result);
            assertTrue(result.getArgLine().contains("-Dspark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension"));
            assertTrue(result.getAdditionalClasspathElements().contains(jarFile.getAbsolutePath()));

            Plugin surefire = project.getPlugin("org.apache.maven.plugins:maven-surefire-plugin");
            assertNotNull(surefire);
            Xpp3Dom config = (Xpp3Dom) surefire.getConfiguration();
            assertNotNull(config);

            Xpp3Dom argLine = config.getChild("argLine");
            assertNotNull(argLine);
            assertTrue(argLine.getValue().contains("-Dspark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension"));

            Xpp3Dom cpElements = config.getChild("additionalClasspathElements");
            assertNotNull(cpElements);
            assertEquals(1, cpElements.getChildren().length);
            assertEquals(jarFile.getAbsolutePath(), cpElements.getChildren()[0].getValue());

            // Test idempotency: configuring again should not duplicate argLine or classpath
            SurefireConfigurator.SurefireConfigResult result2 = configurator.configure(project, jarFile);
            assertEquals(result.getArgLine(), result2.getArgLine());
            assertEquals(1, result2.getAdditionalClasspathElements().size());
            assertEquals(1, config.getChild("additionalClasspathElements").getChildren().length);
        } finally {
            Files.deleteIfExists(tempJar);
        }
    }

    @Test
    void testSurefireConfiguratorAppendsToExistingArgLine() throws IOException {
        SurefireConfigurator configurator = new SurefireConfigurator();
        MavenProject project = new MavenProject();
        Build build = new Build();
        project.setBuild(build);

        Plugin surefire = new Plugin();
        surefire.setGroupId("org.apache.maven.plugins");
        surefire.setArtifactId("maven-surefire-plugin");
        Xpp3Dom config = new Xpp3Dom("configuration");
        Xpp3Dom existingArgLine = new Xpp3Dom("argLine");
        existingArgLine.setValue("-Xmx2g -ea");
        config.addChild(existingArgLine);
        surefire.setConfiguration(config);
        build.addPlugin(surefire);

        Path tempJar = Files.createTempFile("interceptor-spark-3.5_2.13-", ".jar");
        try {
            File jarFile = tempJar.toFile();
            SurefireConfigurator.SurefireConfigResult result = configurator.configure(project, jarFile);

            assertEquals("-Xmx2g -ea -Dspark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension",
                    result.getArgLine());
        } finally {
            Files.deleteIfExists(tempJar);
        }
    }

    @Test
    void testMutateMojoExecutionFailsOnUnsupportedSpark() {
        MutateMojo mojo = new MutateMojo();
        MavenProject project = new MavenProject();
        project.setArtifacts(Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.12", "2.4.8")
        ));
        mojo.setProject(project);

        MojoExecutionException ex = assertThrows(MojoExecutionException.class, mojo::execute);
        assertTrue(ex.getMessage().contains("Unsupported Spark/Scala combination '2.4_2.12'"));
    }

    @Test
    void testMutateMojoSuccessfulExecution() throws Exception {
        MutateMojo mojo = new MutateMojo();
        MavenProject project = new MavenProject();
        project.setArtifacts(Collections.singleton(
                createArtifact("org.apache.spark", "spark-sql_2.13", "3.5.3")
        ));
        project.setBuild(new Build());
        mojo.setProject(project);

        Path tempJar = Files.createTempFile("interceptor-test-", ".jar");
        try {
            // Mock RepositorySystem and RepositorySystemSession via reflection proxy
            RepositorySystemSession session = (RepositorySystemSession) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[]{RepositorySystemSession.class},
                    (proxy, method, args) -> null
            );

            RepositorySystem repoSystem = (RepositorySystem) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[]{RepositorySystem.class},
                    (proxy, method, args) -> {
                        if ("resolveArtifact".equals(method.getName())) {
                            ArtifactRequest req = (ArtifactRequest) args[1];
                            ArtifactResult res = new ArtifactResult(req);
                            org.eclipse.aether.artifact.Artifact resolvedArtifact =
                                    req.getArtifact().setFile(tempJar.toFile());
                            res.setArtifact(resolvedArtifact);
                            return res;
                        }
                        return null;
                    }
            );

            mojo.setRepositorySystem(repoSystem);
            mojo.setRepositorySession(session);

            mojo.execute();

            Plugin surefire = project.getPlugin("org.apache.maven.plugins:maven-surefire-plugin");
            assertNotNull(surefire);
            Xpp3Dom config = (Xpp3Dom) surefire.getConfiguration();
            assertNotNull(config);
            assertTrue(config.getChild("argLine").getValue().contains("MutatorSparkExtension"));
            assertEquals(tempJar.toFile().getAbsolutePath(),
                    config.getChild("additionalClasspathElements").getChildren()[0].getValue());
        } finally {
            Files.deleteIfExists(tempJar);
        }
    }
}
