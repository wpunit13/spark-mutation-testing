package io.github.wpunit13.mutator.spark42_213

import io.github.wpunit13.mutator.api._
import org.apache.spark.sql.catalyst.expressions.{
  Alias, And, Ascending, Attribute, AttributeReference, Cast, Coalesce, Descending,
  Expression, IsNotNull, Literal, NamedExpression, Not, RowFrame, SortOrder,
  SpecifiedWindowFrame, UnaryMinus, UnboundedPreceding, WindowExpression,
  WindowSpecDefinition
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, JoinType, LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Window}
import org.apache.spark.sql.types.{BooleanType, DecimalType, DoubleType}

/**
 * PlanMutatorShim for the Spark 4.2.x / Scala 2.13 combination (WP-20).
 *
 * The mutation rules and the canonicalization grammar are byte-for-byte the
 * 3.5 engine's semantics: the golden cross-version test in this module pins
 * `canonicalExprSig` / `NodeCoordinate` / `MutantID` output to the values the
 * 3.5 shims produce for the same canonical queries, so a coordinate computed
 * under 3.5 addresses the same logical mutation under 4.2. Compatibility was
 * proven by compiling (VERSION_ADDITION_SOP step 2); the compiler named the
 * only Spark-4.x surface differences, and the formulas were never touched.
 *
 * Spark 4.x is Scala 2.13-only, so this minor line has a single Scala-binary
 * combination and the logic lives here directly (no per-minor shared base).
 */
class ShimImpl extends PlanMutatorShim {

  override val supportedVersion: SparkShimVersion = SparkShimVersion("4.2", "2.13")

  override def classify(
    node: LogicalPlan,
    depth: Int,
    childOrdinal: Int
  ): Option[(OperatorType, Seq[MutationCandidate])] = node match {
    case j: Join =>
      // Read-only: no new LogicalPlan/Expression is constructed here.
      val coord = NodeCoordinateFactory(depth, OperatorType.Join, childOrdinal,
        canonicalExprSig(node, OperatorType.Join))
      val targets: Seq[(Int, JoinType)] = Seq(
        (0, LeftOuter),
        (1, Cross),
        (2, LeftAnti)
      )
      // Omit no-op mutants whose target equals the node's current join type.
      // The description renders the node's ACTUAL source type, so a LEFT/CROSS/
      // ANTI join reports truthfully ("LEFT -> CROSS"); an INNER source stays
      // byte-identical to the original static strings ("INNER -> LEFT", ...).
      val candidates = targets
        .filterNot { case (_, target) => j.joinType == target }
        .map { case (index, target) =>
          MutationCandidate(coord, OperatorType.Join, index,
            s"${joinShortForm(j.joinType)} -> ${joinShortForm(target)}")
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

    case a: Aggregate =>
      val coord = NodeCoordinateFactory(depth, OperatorType.Aggregate, childOrdinal,
        canonicalExprSig(node, OperatorType.Aggregate))
      val builder = Seq.newBuilder[MutationCandidate]
      builder += MutationCandidate(coord, OperatorType.Aggregate, 0, "AGGREGATE -> swap function")
      if (a.groupingExpressions.length > 1) {
        builder += MutationCandidate(coord, OperatorType.Aggregate, 1, "AGGREGATE -> drop grouping key")
      }
      builder += MutationCandidate(coord, OperatorType.Aggregate, 2, "AGGREGATE -> zero aggregate")
      Some((OperatorType.Aggregate, builder.result()))

    case w: Window =>
      val coord = NodeCoordinateFactory(depth, OperatorType.Window, childOrdinal,
        canonicalExprSig(node, OperatorType.Window))
      val builder = Seq.newBuilder[MutationCandidate]
      if (w.orderSpec.nonEmpty) {
        builder += MutationCandidate(coord, OperatorType.Window, 0, "INVERT_WINDOW_ORDER")
      }
      if (hasTruncatableFrame(w)) {
        builder += MutationCandidate(coord, OperatorType.Window, 1, "TRUNCATE_WINDOW_FRAME")
      }
      val candidates = builder.result()
      if (candidates.nonEmpty) {
        Some((OperatorType.Window, candidates))
      } else {
        None
      }

    case p: Project =>
      val coord = NodeCoordinateFactory(depth, OperatorType.Project, childOrdinal,
        canonicalExprSig(node, OperatorType.Project))
      val builder = Seq.newBuilder[MutationCandidate]
      if (p.projectList.exists(containsCoalesce)) {
        builder += MutationCandidate(coord, OperatorType.Project, 0, "COALESCE_BYPASS")
      }
      if (p.projectList.nonEmpty) {
        builder += MutationCandidate(coord, OperatorType.Project, 1, "INJECT_NULL")
      }
      if (p.projectList.exists(_.dataType.isInstanceOf[DecimalType])) {
        builder += MutationCandidate(coord, OperatorType.Project, 2, "DECIMAL_TO_DOUBLE")
      }
      val candidates = builder.result()
      if (candidates.nonEmpty) {
        Some((OperatorType.Project, candidates))
      } else {
        None
      }

    case _ => None
  }

  private def isTopLevelAnd(condition: Expression): Boolean = condition match {
    case _: And => true
    case _      => false
  }

  /**
   * TRUNCATE_WINDOW_FRAME is only offered for frames the executor actually
   * honors. Two shapes make the truncation a guaranteed no-op — a mutant that
   * applies, changes nothing, and is reported SURVIVED (a false blind spot):
   *   - ranking window functions (row_number, rank, dense_rank, ntile, ...)
   *     ignore the frame entirely — only aggregate window functions are
   *     frame-sensitive;
   *   - a RangeFrame offset with an empty ORDER BY: every row in the
   *     partition is a peer, so any offset bound selects the same peer group
   *     as the unbounded bound it replaced.
   */
  private def hasTruncatableFrame(w: Window): Boolean =
    w.windowExpressions.exists { ne =>
      ne.exists {
        case we @ WindowExpression(_, _) =>
          we.windowSpec.frameSpecification match {
            case f: SpecifiedWindowFrame =>
              f.lower == UnboundedPreceding && isFrameSensitive(we.windowFunction) &&
                (f.frameType == RowFrame || w.orderSpec.nonEmpty)
            case _ => false
          }
        case _ => false
      }
    }

  /** Only aggregate window functions honor the frame; ranking functions
    * (row_number, rank, dense_rank, ntile, ...) ignore it entirely. */
  private def isFrameSensitive(fn: Expression): Boolean =
    fn.exists { case _: AggregateExpression => true; case _ => false }

  /** Report-facing short form. INNER/LEFT/CROSS/ANTI match the original static
    * descriptions byte-for-byte; anything else falls back to JoinType.sql. */
  private def joinShortForm(jt: JoinType): String = jt match {
    case Inner     => "INNER"
    case LeftOuter => "LEFT"
    case Cross     => "CROSS"
    case LeftAnti  => "ANTI"
    case other     => other.sql
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

  override def mutateAggregate(node: LogicalPlan, mutationIndex: Int): LogicalPlan = node match {
    case a: Aggregate =>
      mutationIndex match {
        case 0 =>
          val newAggExprs = a.aggregateExpressions.map { namedExpr =>
            namedExpr.transformDown {
              case aggExpr: AggregateExpression =>
                aggExpr.aggregateFunction match {
                  case s: Sum   => aggExpr.copy(new Max(s.child), aggExpr.mode, aggExpr.isDistinct, aggExpr.filter, aggExpr.resultId)
                  case m: Max   => aggExpr.copy(new Min(m.child), aggExpr.mode, aggExpr.isDistinct, aggExpr.filter, aggExpr.resultId)
                  case m: Min   => aggExpr.copy(new Sum(m.child), aggExpr.mode, aggExpr.isDistinct, aggExpr.filter, aggExpr.resultId)
                  // Count returns LongType; use a Long literal so the mutated
                  // output column keeps its data type (an Int literal would flip
                  // the column Long -> Int and risk a re-analysis type error).
                  case _: Count => Literal(1L)
                  case _        => aggExpr
                }
            }.asInstanceOf[NamedExpression]
          }
          a.copy(aggregateExpressions = newAggExprs)

        case 1 =>
          if (a.groupingExpressions.length <= 1) {
            throw new ShimMutationException(
              s"mutationIndex 1 requires more than 1 grouping expression, found: ${a.groupingExpressions.length}")
          }
          val lastGrouping = a.groupingExpressions.last
          val newGrouping = a.groupingExpressions.dropRight(1)
          val newAggExprs = a.aggregateExpressions.filterNot(ne => isMatchingGrouping(ne, lastGrouping))
          a.copy(groupingExpressions = newGrouping, aggregateExpressions = newAggExprs)

        case 2 =>
          if (a.aggregateExpressions.isEmpty) {
            throw new ShimMutationException("mutationIndex 2 requires at least 1 aggregate expression")
          }
          val targetIdx = a.aggregateExpressions.indexWhere(expr =>
            containsAggregate(expr) || !isMatchingAnyGrouping(expr, a.groupingExpressions))
          val idx = if (targetIdx >= 0) targetIdx else 0
          val targetExpr = a.aggregateExpressions(idx)
          val zeroedExpr: NamedExpression = targetExpr match {
            case alias: Alias =>
              alias.copy(Literal.default(alias.child.dataType), alias.name)(
                alias.exprId, alias.qualifier, alias.explicitMetadata, alias.nonInheritableMetadataKeys)
            case other =>
              Alias(Literal.default(other.dataType), other.name)(
                other.exprId, other.qualifier, None, Nil)
          }
          a.copy(aggregateExpressions = a.aggregateExpressions.updated(idx, zeroedExpr))

        case other => throw new ShimMutationException(
          s"Unknown aggregate mutationIndex $other; expected 0, 1 or 2")
      }

    case other => throw new ShimMutationException(
      s"mutateAggregate expects an Aggregate node but found ${other.getClass.getName}")
  }

  override def mutateWindow(node: LogicalPlan, mutationIndex: Int): LogicalPlan = node match {
    case w: Window =>
      mutationIndex match {
        case 0 =>
          if (w.orderSpec.isEmpty) {
            throw new ShimMutationException(
              "mutationIndex 0 requires non-empty orderSpec")
          }
          val newOrderSpec = w.orderSpec.map(invertSortOrder)
          val newWindowExprs = w.windowExpressions.map { ne =>
            ne.transformDown {
              case so: SortOrder =>
                invertSortOrder(so)
            }.asInstanceOf[NamedExpression]
          }
          w.copy(windowExpressions = newWindowExprs, orderSpec = newOrderSpec)

        case 1 =>
          val hasUnbounded = w.windowExpressions.exists(_.exists {
            case f: SpecifiedWindowFrame => f.lower == UnboundedPreceding
            case _                       => false
          })
          if (!hasUnbounded) {
            throw new ShimMutationException(
              "mutationIndex 1 requires a SpecifiedWindowFrame starting with UnboundedPreceding")
          }
          val newWindowExprs = w.windowExpressions.map { namedExpr =>
            namedExpr.transformDown {
              case f: SpecifiedWindowFrame if f.lower == UnboundedPreceding =>
                f.copy(lower = UnaryMinus(Literal(1)))
            }.asInstanceOf[NamedExpression]
          }
          w.copy(windowExpressions = newWindowExprs)

        case other => throw new ShimMutationException(
          s"Unknown window mutationIndex $other; expected 0 or 1")
      }

    case other => throw new ShimMutationException(
      s"mutateWindow expects a Window node but found ${other.getClass.getName}")
  }

  private def invertSortOrder(so: SortOrder): SortOrder = {
    val newDirection = if (so.direction == Ascending) Descending else Ascending
    so.copy(direction = newDirection)
  }

  override def mutateProject(node: LogicalPlan, mutationIndex: Int): LogicalPlan = node match {
    case p: Project =>
      mutationIndex match {
        case 0 =>
          if (!p.projectList.exists(containsCoalesce)) {
            throw new ShimMutationException(
              "mutationIndex 0 requires at least one expression containing Coalesce")
          }
          val newProjectList = p.projectList.map { namedExpr =>
            namedExpr.transformDown {
              case c: Coalesce if c.children.nonEmpty =>
                c.children.head
            }.asInstanceOf[NamedExpression]
          }
          p.copy(projectList = newProjectList)

        case 1 =>
          if (p.projectList.isEmpty) {
            throw new ShimMutationException(
              "mutationIndex 1 requires at least 1 project expression")
          }
          val targetIdx = p.projectList.indexWhere(!_.isInstanceOf[Alias])
          val idx = if (targetIdx >= 0) targetIdx else 0
          val targetExpr = p.projectList(idx)
          val nullExpr: NamedExpression = targetExpr match {
            case alias: Alias =>
              alias.copy(child = Literal.create(null, alias.child.dataType), name = alias.name)(
                alias.exprId, alias.qualifier, alias.explicitMetadata, alias.nonInheritableMetadataKeys)
            case other =>
              Alias(Literal.create(null, other.dataType), other.name)(
                other.exprId, other.qualifier, None, Nil)
          }
          p.copy(projectList = p.projectList.updated(idx, nullExpr))

        case 2 =>
          val targetIdx = p.projectList.indexWhere(_.dataType.isInstanceOf[DecimalType])
          if (targetIdx < 0) {
            throw new ShimMutationException(
              "mutationIndex 2 requires at least one Decimal-typed project expression")
          }
          val targetExpr = p.projectList(targetIdx)
          val castExpr: NamedExpression = targetExpr match {
            case alias: Alias =>
              alias.copy(child = Cast(alias.child, DoubleType), name = alias.name)(
                alias.exprId, alias.qualifier, alias.explicitMetadata, alias.nonInheritableMetadataKeys)
            case other =>
              Alias(Cast(other, DoubleType), other.name)(
                other.exprId, other.qualifier, None, Nil)
          }
          p.copy(projectList = p.projectList.updated(targetIdx, castExpr))

        case other => throw new ShimMutationException(
          s"Unknown project mutationIndex $other; expected 0, 1 or 2")
      }

    case other => throw new ShimMutationException(
      s"mutateProject expects a Project node but found ${other.getClass.getName}")
  }

  private def containsCoalesce(expr: Expression): Boolean =
    expr.exists {
      case c: Coalesce => c.children.nonEmpty
      case _           => false
    }

  override def canonicalExprSig(node: LogicalPlan, operatorType: OperatorType): String = {
    val ordinals = fingerprintOrdinals(node, operatorType)
    (node, operatorType) match {
      case (j: Join, OperatorType.Join) =>
        val conditionSig = j.condition
          .map(normalizeCondition)
          .map(substituteAttrTokens(_, ordinals))
          .getOrElse("")
        Seq(j.joinType.sql, conditionSig).mkString(";")

      case (f: Filter, OperatorType.Filter) =>
        Seq(substituteAttrTokens(normalizeCondition(f.condition), ordinals)).mkString(";")

      case (a: Aggregate, OperatorType.Aggregate) =>
        val aggSig = a.aggregateExpressions.map(substituteAttrTokens(_, ordinals))
        val groupingSig = a.groupingExpressions.map(substituteAttrTokens(_, ordinals))
        (aggSig ++ groupingSig).mkString(";")

      case (w: Window, OperatorType.Window) =>
        val winSig = w.windowExpressions.map(substituteAttrTokens(_, ordinals))
        val partSig = w.partitionSpec.map(substituteAttrTokens(_, ordinals))
        val orderSig = w.orderSpec.map(substituteAttrTokens(_, ordinals))
        (winSig ++ partSig ++ orderSig).mkString(";")

      case (p: Project, OperatorType.Project) =>
        p.projectList.map(substituteAttrTokens(_, ordinals)).mkString(";")

      case _ => ""
    }
  }

  private def isMatchingGrouping(namedExpr: NamedExpression, groupingExpr: Expression): Boolean = {
    namedExpr.semanticEquals(groupingExpr) ||
    namedExpr == groupingExpr ||
    (namedExpr match {
      case a: Attribute =>
        groupingExpr match {
          case ga: Attribute => a.exprId == ga.exprId
          case alias: Alias =>
            alias.child.semanticEquals(a) ||
            (alias.child match {
              case gca: Attribute => a.exprId == gca.exprId
              case _ => false
            })
          case _ => false
        }
      case alias: Alias =>
        alias.child.semanticEquals(groupingExpr) ||
        alias.child == groupingExpr ||
        (groupingExpr match {
          case ga: Attribute =>
            alias.child match {
              case ca: Attribute => ca.exprId == ga.exprId
              case _ => false
            }
          case ga: Alias =>
            alias.child.semanticEquals(ga.child) ||
            ((alias.child, ga.child) match {
              case (ca: Attribute, gca: Attribute) => ca.exprId == gca.exprId
              case _ => false
            })
          case _ => false
        })
      case _ => false
    })
  }

  private def isMatchingAnyGrouping(namedExpr: NamedExpression, groupingExpressions: Seq[Expression]): Boolean =
    groupingExpressions.exists(g => isMatchingGrouping(namedExpr, g))

  private def containsAggregate(expr: Expression): Boolean =
    expr.exists {
      case _: AggregateFunction => true
      case _                    => false
    }

  /**
   * Renders `expr` via Catalyst's own `.sql` and rewrites every attribute
   * reference into a positional placeholder `#<ordinal>`, where the ordinal
   * is the attribute's zero-based position in `node.output`. Attributes
   * absent from `node.output` (legitimate for join conditions referencing
   * child-only attributes) collapse to the single deterministic sentinel `#x`.
   * Identical to the 3.5 shims' grammar — the golden cross-version test pins
   * byte-identity across versions.
   *
   * Replacements run longest-token-first so a token that is a prefix of
   * another (e.g. "A.id" vs "A.id2") can never corrupt the longer one.
   */
  /** The expression sequence the fingerprint renders, NORMALIZED (Filter and
    * Join conditions lose optimizer-inserted IsNotNull conjuncts and are
    * canonically ordered). Ordinals are assigned by first appearance across
    * this sequence — a property of the expression content alone, so column
    * pruning and pushdown cannot shift them between discovery and matching. */
  private def fingerprintOrdinals(node: LogicalPlan, operatorType: OperatorType): Map[Long, Int] = {
    val exprs: collection.Seq[Expression] = (node, operatorType) match {
      case (j: Join, OperatorType.Join)           => j.condition.toSeq.map(normalizeCondition)
      case (f: Filter, OperatorType.Filter)       => Seq(normalizeCondition(f.condition))
      case (a: Aggregate, OperatorType.Aggregate) => a.aggregateExpressions ++ a.groupingExpressions
      case (w: Window, OperatorType.Window)       => w.windowExpressions ++ w.partitionSpec ++ w.orderSpec
      case (p: Project, OperatorType.Project)     => p.projectList
      case _                                      => Seq.empty
    }
    exprs
      .flatMap(_.collect { case a: AttributeReference => a })
      .distinct
      .zipWithIndex
      .map { case (a, i) => a.exprId.id -> i }
      .toMap
  }

  /** Optimizer-inserted null guards (isnotnull(col)) are fingerprint noise:
    * pushdown prepends them to analyzed conditions, so the same site's
    * condition text differs between discovery and matching. Literal casts
    * (CAST(100 AS BIGINT), inserted by the analyzer to align types across
    * differently-typed sources) are folded away by SimplifyCasts at
    * optimization — same drift. Strip both: top-level IsNotNull conjuncts
    * (kept untouched when every conjunct is a guard — a pure null-guard
    * filter is a real site) and literal-cast wrappers, then canonically
    * order the remaining conjuncts. */
  private def normalizeCondition(condition: Expression): Expression = {
    val conjuncts = splitConjuncts(stripLiteralCasts(condition))
    val kept = conjuncts.filterNot(_.isInstanceOf[IsNotNull])
    if (kept.isEmpty || kept.length == conjuncts.length) {
      stripLiteralCasts(condition)
    } else {
      kept.sortBy(_.sql).reduce(And)
    }
  }

  /** Folds literal casts (CAST(literal AS type) -> literal): the analyzer
    * inserts them to align types across differently-typed sources, and
    * SimplifyCasts folds them away at optimization — the same site's
    * condition text differs between discovery and matching otherwise. */
  private def stripLiteralCasts(e: Expression): Expression =
    e.transform { case c: Cast if c.child.isInstanceOf[Literal] => c.child }

  private def splitConjuncts(e: Expression): Seq[Expression] = e match {
    case And(left, right) => splitConjuncts(left) ++ splitConjuncts(right)
    case other            => Seq(other)
  }

  /** The set of expression fingerprints that make up this node's site:
    * per-conjunct sigs for Filter/Join conditions, per-expression sigs for
    * Aggregate/Window/Project. The optimizer can only REMOVE expressions
    * (pruning, conversion) or ADD condition conjuncts (pushdown), so a
    * surviving site's set always INTERSECTS the discovery-time set — while a
    * different site built over a differently-typed source (e.g. an implicit
    * CAST inserted for a JSON-inferred BIGINT vs an INT LocalRelation)
    * produces a disjoint set. The viability filter and the identity fallback
    * use this intersection to tell "the same site, reshaped" from "a
    * different site that merely looks similar". */
  override def exprSigSet(node: LogicalPlan, operatorType: OperatorType): Set[String] = {
    val exprs: collection.Seq[Expression] = (node, operatorType) match {
      case (j: Join, OperatorType.Join)           => j.condition.toSeq.map(normalizeCondition)
      case (f: Filter, OperatorType.Filter)       => Seq(normalizeCondition(f.condition))
      case (a: Aggregate, OperatorType.Aggregate) => a.aggregateExpressions ++ a.groupingExpressions
      case (w: Window, OperatorType.Window)       => w.windowExpressions ++ w.partitionSpec ++ w.orderSpec
      case (p: Project, OperatorType.Project)     => p.projectList
      case _                                      => Seq.empty
    }
    val ordinals: Map[Long, Int] = exprs
      .flatMap(_.collect { case a: AttributeReference => a })
      .distinct
      .zipWithIndex
      .map { case (a, i) => a.exprId.id -> i }
      .toMap
    exprs.map(substituteAttrTokens(_, ordinals)).toSet
  }

  /** Renders `expr` via Catalyst's own `.sql` and rewrites every attribute
    * reference into a positional placeholder `#<ordinal>`, where the ordinal
    * is the attribute's first-appearance index across the node's fingerprint
    * expression sequence (see [[fingerprintOrdinals]]) — NOT its position in
    * `node.output`. Output positions shift whenever the optimizer prunes or
    * reshapes the schema; expression-relative ordinals do not, so the same
    * site's fingerprint survives optimization.
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
  private def substituteAttrTokens(expr: Expression, ordinals: Map[Long, Int]): String = {
    // `collection.Seq` (not the predef `Seq`) mirrors the 3.5 grammar exactly:
    // `expr.collect` returns `scala.collection.Seq`, and the fully-qualified
    // form compiles under both Scala binaries.
    val attrs: collection.Seq[AttributeReference] =
      expr.collect { case a: AttributeReference => a }.distinct

    if (attrs.isEmpty) {
      expr.sql
    } else {
      val rendered = expr.sql
      val tokens = attrs.map(a => a -> a.sql).sortBy { case (_, token) => (-token.length, token) }
      var result = rendered
      for ((a, token) <- tokens) {
        result = result.replace(token, "#" + ordinals(a.exprId.id))
      }
      // Literal type markers (100L, 1.0D, 100BD) leak the source column's
      // type into the fingerprint: the analyzer inserts CAST(literal AS type)
      // for differently-typed sources and SimplifyCasts folds them into
      // differently-typed literals (100 vs 100L for the same comparison).
      // Canonicalize to the bare value.
      result = result
        .replaceAll("(\\d+)L\\b", "$1")
        .replaceAll("(\\d+)D\\b", "$1")
        .replaceAll("(\\d+\\.?\\d*)BD\\b", "$1")
      result
    }
  }
}
