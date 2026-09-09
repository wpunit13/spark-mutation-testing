package io.github.wpunit13.mutator.api

/** Fixed, version-independent operator classification. Never derived from a
  * concrete Catalyst class name. */
sealed trait OperatorType
object OperatorType {
  case object Join extends OperatorType
  case object Filter extends OperatorType
  case object Aggregate extends OperatorType
  case object Window extends OperatorType
  case object Project extends OperatorType
  case object Other extends OperatorType
}
