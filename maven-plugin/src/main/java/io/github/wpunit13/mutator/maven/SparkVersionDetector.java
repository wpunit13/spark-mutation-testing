package io.github.wpunit13.mutator.maven;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Inspects the target project's test classpath to determine the exact
 * {@code org.apache.spark:spark-sql_<scala_ver>:<version>} artifact coordinate present,
 * validates against supported Spark/Scala versions, and maps to the matching
 * {@code io.github.wpunit13:interceptor-spark-<major.minor>_<scala_ver>:<version>}
 * interceptor artifact coordinate.
 */
public class SparkVersionDetector {

    public static final String DEFAULT_GROUP_ID = "io.github.wpunit13";
    public static final String DEFAULT_INTERCEPTOR_VERSION = "1.0.0-SNAPSHOT";
    public static final String SPARK_GROUP_ID = "org.apache.spark";
    public static final String SPARK_SQL_PREFIX = "spark-sql_";

    public static final Set<String> SUPPORTED_VERSIONS =
            Collections.unmodifiableSet(new TreeSet<>(Set.of("3.5_2.12", "3.5_2.13", "4.2_2.13")));

    private final String interceptorVersion;

    public SparkVersionDetector() {
        this(DEFAULT_INTERCEPTOR_VERSION);
    }

    public SparkVersionDetector(String interceptorVersion) {
        this.interceptorVersion = (interceptorVersion != null && !interceptorVersion.isBlank())
                ? interceptorVersion
                : DEFAULT_INTERCEPTOR_VERSION;
    }

    public static Set<String> getSupportedVersions() {
        return SUPPORTED_VERSIONS;
    }

    /**
     * Detects the Spark version and maps to the corresponding interceptor coordinate
     * from a {@link MavenProject}.
     */
    public InterceptorCoordinate detect(MavenProject project) throws MojoExecutionException {
        if (project == null) {
            throw new MojoExecutionException("MavenProject cannot be null");
        }
        Set<Artifact> artifacts = project.getArtifacts();
        if (artifacts == null) {
            throw new MojoExecutionException(
                    "Project artifacts are null; ensure test dependency resolution is enabled");
        }
        return detect(artifacts);
    }

    /**
     * Detects the Spark version and maps to the corresponding interceptor coordinate
     * from a set of classpath {@link Artifact}s.
     */
    public InterceptorCoordinate detect(Set<Artifact> artifacts) throws MojoExecutionException {
        if (artifacts == null || artifacts.isEmpty()) {
            throw new MojoExecutionException(
                    "No artifacts found on test classpath. Ensure test dependencies are resolved.");
        }

        Artifact sparkSqlArtifact = null;
        for (Artifact artifact : artifacts) {
            if (SPARK_GROUP_ID.equals(artifact.getGroupId())
                    && artifact.getArtifactId() != null
                    && artifact.getArtifactId().startsWith(SPARK_SQL_PREFIX)) {
                sparkSqlArtifact = artifact;
                break;
            }
        }

        if (sparkSqlArtifact == null) {
            throw new MojoExecutionException(
                    "No org.apache.spark:spark-sql_<scala_version> dependency found on test classpath");
        }

        String artifactId = sparkSqlArtifact.getArtifactId();
        String scalaVersion = artifactId.substring(SPARK_SQL_PREFIX.length());
        if (scalaVersion.isBlank()) {
            throw new MojoExecutionException(
                    "Could not extract Scala binary version from artifactId '" + artifactId + "'");
        }

        String sparkVersion = sparkSqlArtifact.getVersion();
        if (sparkVersion == null || sparkVersion.isBlank()) {
            throw new MojoExecutionException(
                    "Spark artifact '" + sparkSqlArtifact + "' has null or empty version");
        }

        String[] parts = sparkVersion.split("\\.");
        if (parts.length < 2) {
            throw new MojoExecutionException(
                    "Could not parse Spark version string '" + sparkVersion + "': expected at least '<major>.<minor>'");
        }

        String sparkMinor = parts[0] + "." + parts[1];
        String key = sparkMinor + "_" + scalaVersion;

        if (!SUPPORTED_VERSIONS.contains(key)) {
            throw new MojoExecutionException(
                    "Unsupported Spark/Scala combination '" + key + "'. "
                            + "Supported versions: " + new TreeSet<>(SUPPORTED_VERSIONS));
        }

        // Resolve the PUBLISHED bundle (shaded fat jar), not the thin shim —
        // WP-22 excludes the thin shims from Central; only the bundles are on
        // the public surface. Resolving the thin shim made the released
        // plugin's `mutate` goal fail on consumer machines (v1.0.0 defect).
        String interceptorArtifactId = "interceptor-bundle-spark-" + key;

        return new InterceptorCoordinate(
                DEFAULT_GROUP_ID,
                interceptorArtifactId,
                this.interceptorVersion,
                sparkVersion,
                sparkMinor,
                scalaVersion,
                key);
    }

    public String detectCoordinate(MavenProject project) throws MojoExecutionException {
        return detect(project).getCoordinate();
    }

    public String detectCoordinate(Set<Artifact> artifacts) throws MojoExecutionException {
        return detect(artifacts).getCoordinate();
    }

    /**
     * Value object representing the mapped interceptor artifact coordinate and metadata.
     */
    public static class InterceptorCoordinate {
        private final String groupId;
        private final String artifactId;
        private final String version;
        private final String sparkVersion;
        private final String sparkMinor;
        private final String scalaVersion;
        private final String key;

        public InterceptorCoordinate(
                String groupId,
                String artifactId,
                String version,
                String sparkVersion,
                String sparkMinor,
                String scalaVersion,
                String key) {
            this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
            this.artifactId = Objects.requireNonNull(artifactId, "artifactId must not be null");
            this.version = Objects.requireNonNull(version, "version must not be null");
            this.sparkVersion = sparkVersion;
            this.sparkMinor = sparkMinor;
            this.scalaVersion = scalaVersion;
            this.key = key;
        }

        public String getGroupId() {
            return groupId;
        }

        public String getArtifactId() {
            return artifactId;
        }

        public String getVersion() {
            return version;
        }

        public String getSparkVersion() {
            return sparkVersion;
        }

        public String getSparkMinor() {
            return sparkMinor;
        }

        public String getScalaVersion() {
            return scalaVersion;
        }

        public String getKey() {
            return key;
        }

        public String getCoordinate() {
            return groupId + ":" + artifactId + ":" + version;
        }

        @Override
        public String toString() {
            return getCoordinate();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InterceptorCoordinate that = (InterceptorCoordinate) o;
            return Objects.equals(groupId, that.groupId)
                    && Objects.equals(artifactId, that.artifactId)
                    && Objects.equals(version, that.version)
                    && Objects.equals(sparkVersion, that.sparkVersion)
                    && Objects.equals(sparkMinor, that.sparkMinor)
                    && Objects.equals(scalaVersion, that.scalaVersion)
                    && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupId, artifactId, version, sparkVersion, sparkMinor, scalaVersion, key);
        }
    }
}
