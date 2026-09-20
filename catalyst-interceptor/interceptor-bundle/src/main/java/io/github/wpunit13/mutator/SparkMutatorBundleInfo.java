package io.github.wpunit13.mutator;

/**
 * Marker for the Spark 3.5.x / Scala 2.13 shade bundle.
 *
 * <p>This module compiles no engine code: the maven-shade-plugin merges
 * mutator-core, interceptor-api, interceptor-dispatch, interceptor-runtime
 * and the Spark 3.5_2.13 shim into one self-contained fat jar that the
 * Python wheel mounts via {@code --jars} and the Maven plugin resolves via
 * Aether. First-party packages are intentionally NOT relocated — the Py4J
 * bridge calls {@code io.github.wpunit13.mutator.*} by exact FQCN.</p>
 *
 * <p>This class exists so the sources/javadoc jars Maven Central requires
 * have real content; the module's own POM documents the shading design.</p>
 */
public final class SparkMutatorBundleInfo {

    private SparkMutatorBundleInfo() {
        // marker only — never instantiated
    }
}
