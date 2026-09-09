package io.github.wpunit13.mutator.api

/** Opaque 64-bit coordinate. Equality/hashCode are value-based.
  * toString renders the fixed 16-char lowercase hex form. */
final case class NodeCoordinate(value: Long) {
  def toHex: String = f"$value%016x"

  override def toString: String = toHex
}
