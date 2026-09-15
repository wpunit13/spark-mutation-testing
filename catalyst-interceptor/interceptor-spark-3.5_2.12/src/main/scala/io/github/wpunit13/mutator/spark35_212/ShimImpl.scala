package io.github.wpunit13.mutator.spark35_212

import io.github.wpunit13.mutator.api.SparkShimVersion
import io.github.wpunit13.mutator.spark35.Spark35ShimBase

/**
 * PlanMutatorShim implementation for the Spark 3.5.x / Scala 2.12 combination.
 *
 * Instantiated via ServiceLoader, hence a plain class with a public no-arg
 * constructor (a Scala object would compile to a private-ctor singleton and
 * fail reflective loading). All mutation logic lives in [[Spark35ShimBase]];
 * this class only supplies the Scala-binary version.
 */
class ShimImpl extends Spark35ShimBase {
  override val supportedVersion: SparkShimVersion = SparkShimVersion("3.5", "2.12")
}
