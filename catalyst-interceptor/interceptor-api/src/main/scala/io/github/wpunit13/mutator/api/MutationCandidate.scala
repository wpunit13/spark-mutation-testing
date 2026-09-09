package io.github.wpunit13.mutator.api

/** One entry in the version-agnostic catalog of applicable mutations for a
  * single plan node, produced during Discovery. */
final case class MutationCandidate(
  coordinate: NodeCoordinate,
  operatorType: OperatorType,
  mutationIndex: Int,
  description: String // e.g. "INNER -> CROSS", human-readable, report-facing only
)
