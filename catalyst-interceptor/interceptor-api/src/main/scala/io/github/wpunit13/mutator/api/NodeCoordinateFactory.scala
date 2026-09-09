package io.github.wpunit13.mutator.api

import io.github.wpunit13.mutator.hash.DeterministicHasher

/** The only place a NodeCoordinate is ever computed. Builds the canonical
  * coordinate string, hashes it via DeterministicHasher, and wraps the
  * unsigned 64-bit result in a NodeCoordinate. */
object NodeCoordinateFactory {

  def apply(depth: Int, operatorType: OperatorType, childOrdinal: Int, exprSig: String): NodeCoordinate = {
    val canonical = s"$depth|${operatorTypeTag(operatorType)}|$childOrdinal|$exprSig"
    val hex = DeterministicHasher.hashToHex(canonical)
    NodeCoordinate(java.lang.Long.parseUnsignedLong(hex, 16))
  }

  def operatorTypeTag(operatorType: OperatorType): String = operatorType match {
    case OperatorType.Join      => "JOIN"
    case OperatorType.Filter    => "FILTER"
    case OperatorType.Aggregate => "AGGREGATE"
    case OperatorType.Window    => "WINDOW"
    case OperatorType.Project   => "PROJECT"
    case OperatorType.Other     => "OTHER"
  }
}
