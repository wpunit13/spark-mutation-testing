/**
 * Shade-only aggregation bundle for Spark 3.5.x / Scala 2.13.
 *
 * This module has no code of its own: the maven-shade-plugin merges
 * mutator-core, interceptor-api, interceptor-dispatch, interceptor-runtime
 * and the Spark 3.5_2.13 shim into one self-contained fat jar that the
 * Python wheel mounts via {@code --jars} and the Maven plugin resolves via
 * Aether. First-party packages are intentionally NOT relocated — the Py4J
 * bridge calls {@code io.github.wpunit13.mutator.*} by exact FQCN.
 *
 * Sources/javadoc jars exist (empty-package doc only) to satisfy Maven
 * Central's artifact-validation requirements; the module's own POM
 * documents the shading design in full.
 */
package io.github.wpunit13.mutator;
