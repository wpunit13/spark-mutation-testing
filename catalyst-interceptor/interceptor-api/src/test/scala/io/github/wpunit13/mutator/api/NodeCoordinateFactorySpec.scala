package io.github.wpunit13.mutator.api

import io.github.wpunit13.mutator.hash.DeterministicHasher
import org.scalatest.funsuite.AnyFunSuite

class NodeCoordinateFactorySpec extends AnyFunSuite {

  private val hex16 = "^[0-9a-f]{16}$"

  test("golden value is pinned to the independently-computed SHA-256 truncation") {
    val coord = NodeCoordinateFactory(2, OperatorType.Join, 0, "Inner;(customer_id#0 = customer_id#1)")
    assert(coord.toHex == "058a342dfabb7500")
  }

  test("round trip is lossless across varied canonical strings, including high-bit-set hashes") {
    val inputs = (0 until 30).map(i => s"$i|FILTER|${i * 3}|col#$i = lit_$i") ++
      Seq(
        "0|JOIN|0|",
        "1|PROJECT|-1|a#1, b#2",
        "5|WINDOW|2|row_number()#3 OVER (PARTITION BY x#4 ORDER BY y#5)",
        "7|AGGREGATE|1|count(1)#6; x#7",
        "3|OTHER|0|nothing here"
      )
    val hexes = inputs.map { canonical =>
      val parts = canonical.split("\\|", 4)
      val coord = NodeCoordinateFactory(parts(0).toInt, tagToType(parts(1)), parts(2).toInt, parts(3))
      assert(coord.toHex == DeterministicHasher.hashToHex(canonical),
        s"round trip mismatch for canonical string: $canonical")
      assert(coord.toHex.matches(hex16), s"not 16 lowercase hex chars: ${coord.toHex}")
      coord.toHex
    }
    // At least one coordinate must have the high bit set (first hex digit 8-f)
    // to prove no sign-extension bug exists in the parse/render round trip.
    assert(hexes.exists(h => h.charAt(0) >= '8' && h.charAt(0) <= 'f'),
      s"no high-bit-set coordinate produced; got: ${hexes.mkString(",")}")
  }

  private def tagToType(tag: String): OperatorType = tag match {
    case "JOIN"     => OperatorType.Join
    case "FILTER"   => OperatorType.Filter
    case "AGGREGATE" => OperatorType.Aggregate
    case "WINDOW"   => OperatorType.Window
    case "PROJECT"  => OperatorType.Project
    case _          => OperatorType.Other
  }

  test("apply is deterministic for identical arguments") {
    val a = NodeCoordinateFactory(4, OperatorType.Filter, 1, "not(status#9 = 'COMPLETED')")
    val b = NodeCoordinateFactory(4, OperatorType.Filter, 1, "not(status#9 = 'COMPLETED')")
    assert(a == b)
    assert(a.toHex == b.toHex)
  }

  test("operatorTypeTag returns the exact uppercase tag for all six cases") {
    assert(NodeCoordinateFactory.operatorTypeTag(OperatorType.Join) == "JOIN")
    assert(NodeCoordinateFactory.operatorTypeTag(OperatorType.Filter) == "FILTER")
    assert(NodeCoordinateFactory.operatorTypeTag(OperatorType.Aggregate) == "AGGREGATE")
    assert(NodeCoordinateFactory.operatorTypeTag(OperatorType.Window) == "WINDOW")
    assert(NodeCoordinateFactory.operatorTypeTag(OperatorType.Project) == "PROJECT")
    assert(NodeCoordinateFactory.operatorTypeTag(OperatorType.Other) == "OTHER")
  }
}
