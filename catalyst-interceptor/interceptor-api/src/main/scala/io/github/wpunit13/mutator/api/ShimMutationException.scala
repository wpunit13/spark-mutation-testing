package io.github.wpunit13.mutator.api

/** Thrown when a shim is asked to mutate a node whose live structure no
  * longer matches what was catalogued at Discovery time. Always maps to ERRORED. */
final class ShimMutationException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
