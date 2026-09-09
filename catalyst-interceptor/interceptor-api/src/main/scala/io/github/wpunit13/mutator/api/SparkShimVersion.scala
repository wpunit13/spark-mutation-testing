package io.github.wpunit13.mutator.api

/** Immutable value identifying a shim's target Spark/Scala binary cell. */
final case class SparkShimVersion(sparkMinor: String, scalaBinary: String)
