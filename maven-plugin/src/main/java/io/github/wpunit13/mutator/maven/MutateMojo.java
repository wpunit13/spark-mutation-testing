package io.github.wpunit13.mutator.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.ResolutionScope;

/**
 * Entry point for mutation testing of Java and Scala Spark pipelines:
 * {@code mvn spark-mutator:mutate}.
 *
 * <h2>Status: placeholder</h2>
 *
 * <p>This Mojo is intentionally non-functional and always fails. The
 * Java/Scala orchestration described in {@code docs/ARCHITECTURE.md} section
 * 5.4 is not implemented. That work comprises:
 *
 * <ul>
 *   <li>introspecting the target project's test classpath to determine the
 *       exact {@code org.apache.spark:spark-sql_<scala>} version in use;</li>
 *   <li>resolving the matching {@code interceptor-bundle-spark-*} artifact
 *       through the Maven resolver at plugin-execution time, rather than
 *       requiring a declared dependency in the user's POM;</li>
 *   <li>injecting that artifact plus
 *       {@code -Dspark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension}
 *       into the Surefire/Failsafe {@code argLine};</li>
 *   <li>driving the baseline, discovery, and fail-fast mutation loop, which is
 *       the JVM-side counterpart of the pytest plugin's
 *       {@code pytest_runtest_protocol} hook.</li>
 * </ul>
 *
 * <p>The module exists today so the full Maven reactor builds and installs
 * cleanly and so the artifact coordinate
 * {@code io.github.wpunit13:spark-mutator-maven-plugin} resolves for anyone
 * following the project README.
 *
 * <p>The PySpark track is complete; use the {@code pytest-spark-mutator}
 * Python plugin with {@code pytest --spark-mutate}.
 *
 * <p><strong>Note for implementers:</strong> {@code requiresDependencyResolution}
 * is already set to {@link ResolutionScope#TEST} below, because classpath
 * introspection cannot work without it. Do not remove it.
 */
@Mojo(
        name = "mutate",
        defaultPhase = LifecyclePhase.VERIFY,
        requiresDependencyResolution = ResolutionScope.TEST,
        threadSafe = false)
public class MutateMojo extends AbstractMojo {

    static final String NOT_IMPLEMENTED_MESSAGE =
            "spark-mutator-maven-plugin orchestration is not yet implemented; "
                    + "use the pytest-spark-mutator plugin for PySpark pipelines";

    @Override
    public void execute() throws MojoExecutionException {
        throw new MojoExecutionException(NOT_IMPLEMENTED_MESSAGE);
    }
}
