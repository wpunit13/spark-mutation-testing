package io.github.wpunit13.mutator.api

/** Immutable value identifying a shim's target Spark/Scala combination. */
final case class SparkShimVersion(sparkMinor: String, scalaBinary: String)
