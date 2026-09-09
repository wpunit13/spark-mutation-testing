package io.github.wpunit13.mutator.dispatch

import io.github.wpunit13.mutator.api.PlanMutatorShim

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

/**
 * Resolves exactly one [[PlanMutatorShim]] implementation at runtime via
 * java.util.ServiceLoader and caches it for the JVM's lifetime.
 *
 * ARCHITECTURAL INVARIANT: this module must never declare a dependency on a
 * concrete interceptor-spark-* module; the shim is discovered reflectively
 * from META-INF/services. Finding zero or more than one shim is a packaging
 * error and fails loudly.
 */
object ShimDispatcher {

  /**
   * The single resolved shim. Lazily initialized: discovery runs exactly
   * once and the result is cached for the JVM's lifetime. Do not re-run
   * discovery per access.
   */
  lazy val activeShim: PlanMutatorShim = {
    // Spark's driver classloader arrangement makes the context classloader
    // the more reliable choice for jars mounted via --jars.
    val classLoader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(classOf[PlanMutatorShim].getClassLoader)
    resolve(discover(classLoader))
  }

  /**
   * Materializes every PlanMutatorShim discovered through `classLoader`.
   * All implementations are collected into a List first — never just take
   * the head — so accidental multi-shim classpaths are detected.
   * Narrow test seam: package-private so ShimDispatcherSpec can exercise
   * discovery with controlled classloaders.
   */
  private[dispatch] def discover(classLoader: ClassLoader): List[PlanMutatorShim] = {
    val loader = ServiceLoader.load(classOf[PlanMutatorShim], classLoader)
    loader.iterator().asScala.toList
  }

  /**
   * Validation over an already-materialized shim list. Narrow test seam:
   * package-private so the zero- and multiple-implementation error paths
   * can be tested with synthetic lists.
   */
  private[dispatch] def resolve(shims: List[PlanMutatorShim]): PlanMutatorShim = shims match {
    case Nil =>
      throw new IllegalStateException("no PlanMutatorShim implementation found on classpath")
    case single :: Nil =>
      single
    case multiple =>
      // Multiple shim jars (e.g. 2.12 and 2.13 builds) coexisting on one
      // classpath cause class-loading collisions. Runtime selection mounts
      // exactly one shim jar per session; accidental co-presence is a
      // packaging error and must fail loudly, not silently pick one.
      throw new IllegalStateException(
        "expected exactly one PlanMutatorShim implementation on the classpath but found "
          + multiple.size + ": "
          + multiple.map(s => s.supportedVersion.toString).mkString(", "))
  }
}
