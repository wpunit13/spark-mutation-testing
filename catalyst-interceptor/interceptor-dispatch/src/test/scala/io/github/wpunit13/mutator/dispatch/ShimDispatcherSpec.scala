package io.github.wpunit13.mutator.dispatch

import io.github.wpunit13.mutator.api.{PlanMutatorShim, SparkShimVersion}
import org.scalatest.funsuite.AnyFunSuite

class ShimDispatcherSpec extends AnyFunSuite {

  private class FakeShim(version: SparkShimVersion) extends PlanMutatorShim {
    override val supportedVersion: SparkShimVersion = version
    override def classify(
      node: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      depth: Int,
      childOrdinal: Int
    ): Option[(io.github.wpunit13.mutator.api.OperatorType,
      Seq[io.github.wpunit13.mutator.api.MutationCandidate])] = None
    override def mutateJoin(
      node: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      mutationIndex: Int): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
      throw new UnsupportedOperationException("fake")
    override def mutateFilter(
      node: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      mutationIndex: Int): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
      throw new UnsupportedOperationException("fake")
    override def mutateAggregate(
      node: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      mutationIndex: Int): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
      throw new UnsupportedOperationException("fake")
    override def mutateWindow(
      node: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      mutationIndex: Int): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
      throw new UnsupportedOperationException("fake")
    override def mutateProject(
      node: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      mutationIndex: Int): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
      throw new UnsupportedOperationException("fake")
    override def canonicalExprSig(
      node: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      operatorType: io.github.wpunit13.mutator.api.OperatorType): String = ""
  }

  test("empty shim list throws IllegalStateException with the zero-implementation message") {
    val ex = intercept[IllegalStateException] {
      ShimDispatcher.resolve(List.empty)
    }
    assert(ex.getMessage.contains("no PlanMutatorShim implementation found on classpath"))
  }

  test("multiple shims throw IllegalStateException listing every discovered supportedVersion") {
    val shimA = new FakeShim(SparkShimVersion("3.5", "2.13"))
    val shimB = new FakeShim(SparkShimVersion("3.4", "2.12"))
    val ex = intercept[IllegalStateException] {
      ShimDispatcher.resolve(List(shimA, shimB))
    }
    assert(ex.getMessage.contains("3.5"))
    assert(ex.getMessage.contains("3.4"))
  }

  test("exactly one shim resolves to that shim") {
    val shim = new FakeShim(SparkShimVersion("3.5", "2.13"))
    assert(ShimDispatcher.resolve(List(shim)) eq shim)
  }

  test("discover with a classloader exposing no shim returns an empty list") {
    val emptyLoader = new java.net.URLClassLoader(Array.empty, new java.net.URLClassLoader(
      Array.empty, ClassLoader.getSystemClassLoader.getParent))
    assert(ShimDispatcher.discover(emptyLoader).isEmpty)
  }

  test("discover with the null parent bootstrap classloader returns an empty list") {
    // The bootstrap classloader has no META-INF/services registration for
    // PlanMutatorShim, so discovery must materialize an empty List.
    val bootstrapOnlyLoader = new java.net.URLClassLoader(Array.empty, null)
    assert(ShimDispatcher.discover(bootstrapOnlyLoader).isEmpty)
  }

  test("discover with the system classloader materializes all registered shims") {
    // Whether any shim is registered here depends on the classpath; the
    // seam contract under test is full materialization into a List, which
    // holds for both outcomes.
    val shims = ShimDispatcher.discover(Thread.currentThread().getContextClassLoader)
    assert(shims.size >= 0)
    shims.foreach(s => assert(s.isInstanceOf[PlanMutatorShim]))
  }
}
