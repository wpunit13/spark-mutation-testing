package io.github.wpunit13.mutator.spark35

import io.github.wpunit13.mutator.api._
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, Literal, Not}
import org.apache.spark.sql.catalyst.plans.{Cross, JoinType, LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.types.BooleanType

/**
 * PlanMutatorShim implementation for the Spark 3.5.x / Scala 2.13 cell.
 *
 * Instantiated via ServiceLoader, hence a plain class with a public no-arg
 * constructor (a Scala object would compile to a private-ctor singleton and
 * fail reflective loading).
 */
class ShimImpl extends PlanMutatorShim {

  override val supportedVersion: SparkShimVersion = SparkShimVersion("3.5", "2.13")

  override def classify(
    node: LogicalPlan,
    depth: Int,
    childOrdinal: Int
  ): Option[(OperatorType, Seq[MutationCandidate])] = node match {
    case j: Join =>
      // Read-only: no new LogicalPlan/Expression is constructed here.
      val coord = NodeCoordinateFactory(depth, OperatorType.Join, childOrdinal,
        canonicalExprSig(node, OperatorType.Join))
      val targets: Seq[(Int, String, JoinType)] = Seq(
        (0, "INNER -> LEFT", LeftOuter),
        (1, "INNER -> CROSS", Cross),
        (2, "INNER -> ANTI", LeftAnti)
      )
      // Omit no-op mutants whose target equals the node's current join type.
      val candidates = targets
        .filterNot { case (_, _, target) => j.joinType == target }
        .map { case (index, description, _) =>
          MutationCandidate(coord, OperatorType.Join, index, description)
        }
      Some((OperatorType.Join, candidates))

    case f: Filter =>
      val coord = NodeCoordinateFactory(depth, OperatorType.Filter, childOrdinal,
        canonicalExprSig(node, OperatorType.Filter))
      val builder = Seq.newBuilder[MutationCandidate]
      // Conjunct-splitting candidates exist only for a top-level And condition.
      if (isTopLevelAnd(f.condition)) {
        builder += MutationCandidate(coord, OperatorType.Filter, 0, "FILTER -> keep left conjunct")
        builder += MutationCandidate(coord, OperatorType.Filter, 1, "FILTER -> keep right conjunct")
      }
      builder += MutationCandidate(coord, OperatorType.Filter, 2, "FILTER -> FALSE")
      builder += MutationCandidate(coord, OperatorType.Filter, 3, "FILTER -> NOT(predicate)")
      Some((OperatorType.Filter, builder.result()))

    case _ => None
  }

  private def isTopLevelAnd(condition: Expression): Boolean = condition match {
    case _: And => true
    case _      => false
  }

  override def mutateJoin(node: LogicalPlan, mutationIndex: Int): LogicalPlan = node match {
    case j: Join =>
      mutationIndex match {
        case 0 => j.copy(joinType = LeftOuter)
        case 1 =>
          // INNER -> CROSS relaxes the join to a true Cartesian product, so the
          // join predicate is dropped. Keeping it would make `CROSS JOIN ... ON
          // <equality>` semantically identical to the Inner join it replaced,
          // turning the mutation into a no-op with zero killing power for any
          // conditioned join.
          j.copy(joinType = Cross, condition = None)
        case 2 => j.copy(joinType = LeftAnti)
        case other => throw new ShimMutationException(
          s"Unknown join mutationIndex $other; expected 0, 1 or 2")
      }

    case other => throw new ShimMutationException(
      s"mutateJoin expects a Join node but found ${other.getClass.getName}")
  }

  override def mutateFilter(node: LogicalPlan, mutationIndex: Int): LogicalPlan = node match {
    case f: Filter =>
      mutationIndex match {
        case 0 =>
          f.condition match {
            case And(left, _) => f.copy(condition = left)
            case other => throw new ShimMutationException(
              s"mutationIndex 0 requires a top-level And condition, found: ${other.getClass.getName}")
          }
        case 1 =>
          f.condition match {
            case And(_, right) => f.copy(condition = right)
            case other => throw new ShimMutationException(
              s"mutationIndex 1 requires a top-level And condition, found: ${other.getClass.getName}")
          }
        case 2 => f.copy(condition = Literal(false, BooleanType))
        case 3 => f.copy(condition = Not(f.condition))
        case other => throw new ShimMutationException(
          s"Unknown filter mutationIndex $other; expected 0, 1, 2 or 3")
      }

    case other => throw new ShimMutationException(
      s"mutateFilter expects a Filter node but found ${other.getClass.getName}")
  }

  override def mutateAggregate(node: LogicalPlan, mutationIndex: Int): LogicalPlan =
    throw new UnsupportedOperationException("mutateAggregate is not implemented in this release")

  override def mutateWindow(node: LogicalPlan, mutationIndex: Int): LogicalPlan =
    throw new UnsupportedOperationException("mutateWindow is not implemented in this release")

  override def mutateProject(node: LogicalPlan, mutationIndex: Int): LogicalPlan =
    throw new UnsupportedOperationException("mutateProject is not implemented in this release")

  override def canonicalExprSig(node: LogicalPlan, operatorType: OperatorType): String =
    (node, operatorType) match {
      case (j: Join, OperatorType.Join) =>
        val conditionSig = j.condition.map(substituteExprIds(j, _)).getOrElse("")
        Seq(j.joinType.sql, conditionSig).mkString(";")

      case (f: Filter, OperatorType.Filter) =>
        Seq(substituteExprIds(f, f.condition)).mkString(";")

      case _ => ""
    }

  /**
   * Renders `expr` via Catalyst's own `.sql` and rewrites every attribute
   * reference into a positional placeholder `#<ordinal>`, where the ordinal
   * is the attribute's zero-based position in `node.output`. Attributes
   * absent from `node.output` (legitimate for join conditions referencing
   * child-only attributes) collapse to the single deterministic sentinel `#x`.
   *
   * Note on Spark 3.5.x: AttributeReference.sql renders `qualifier.name` and
   * carries NO `#<exprId>` suffix, so the §4.3 "replace the #<exprId> suffix"
   * mechanics degenerate to a no-op here and qualifiers/names would leak into
   * the signature. The spec's stated intent ("replaces every attribute
   * reference with a positional placeholder rather than its exprId") is
   * implemented literally instead: each attribute's whole rendered token is
   * replaced by `#<ordinal>`. This keeps the exprSig byte-identical across
   * independent builds (different exprIds, aliases and names) and across
   * Spark versions.
   *
   * Replacements run longest-token-first so a token that is a prefix of
   * another (e.g. "A.id" vs "A.id2") can never corrupt the longer one.
   */
  private def substituteExprIds(node: LogicalPlan, expr: Expression): String = {
    val ordinals: Map[Long, Int] =
      node.output.zipWithIndex.map { case (a, i) => a.exprId.id -> i }.toMap

    val attrs: Seq[AttributeReference] =
      expr.collect { case a: AttributeReference => a }.distinct

    if (attrs.isEmpty) {
      expr.sql
    } else {
      val rendered = expr.sql
      val tokens = attrs.map(a => a -> a.sql).sortBy { case (_, token) => (-token.length, token) }
      var result = rendered
      for ((a, token) <- tokens) {
        val placeholder = ordinals.get(a.exprId.id).map(o => "#" + o).getOrElse("#x")
        result = result.replace(token, placeholder)
      }
      result
    }
  }
}
