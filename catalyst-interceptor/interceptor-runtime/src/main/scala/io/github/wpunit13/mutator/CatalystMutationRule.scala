package io.github.wpunit13.mutator

import io.github.wpunit13.mutator.api.{NodeCoordinateFactory, OperatorType, PlanMutatorShim}
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess
import io.github.wpunit13.mutator.hash.DeterministicHasher
import io.github.wpunit13.mutator.model.OperatorTypeDto
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import java.util.concurrent.atomic.AtomicReference

/**
 * One Rule[LogicalPlan] implementation registered TWICE by
 * MutatorSparkExtension, branching on [[Phase]] and MutantRegistry state:
 *
 *  - '''PostHoc''' (analyzer "Post-Hoc Resolution" batch, Once strategy —
 *    exactly one invocation per analysis, on the fully resolved analyzed plan):
 *      - IDLE        -> Discovery: classify-and-catalogue; plan unchanged.
 *      - ACTIVE(id)  -> match the active mutant's NodeCoordinate (the exact
 *                       coordinate space Discovery used) and record the matched
 *                       node's SHAPE-FREE key — the coordinate the node yields
 *                       when classified as the plan root, i.e. a pure function
 *                       of (operatorType, expression signature) — in
 *                       [[CatalystMutationRule.pendingRewrite]]. The plan is
 *                       returned UNCHANGED; the rewrite is deferred to the
 *                       Optimizer phase because the analyzer's Finish Analysis
 *                       check runs after the Post-Hoc batch and would reject
 *                       schema-breaking rewrites before the optimizer — the
 *                       only component that can repair them — executes (e.g.
 *                       INNER -> ANTI under a USING join, where the analyzer
 *                       inserts a dedup Project referencing right-side columns
 *                       above the join; the optimizer's ColumnPruning /
 *                       CollapseProject merge that Project away, making the
 *                       ANTI plan valid).
 *  - '''Optimizer''' (extended operator-optimization rules, which run after
 *    analysis and its check, and before physical planning, so a mutated Join
 *    type is still subject to join-strategy selection afterward):
 *      - IDLE        -> no-op (Discovery happens only at PostHoc).
 *      - ACTIVE(id)  -> re-classify nodes SHAPE-FREE (as plan roots) and apply
 *                       exactly one rewrite at the first node whose shape-free
 *                       key matches the pending entry; tag it
 *                       AlreadyMutatedTag and record the applied fact in
 *                       AppliedMutantTracker.
 *
 * Why shape-free matching at the Optimizer phase: QueryExecution clones the
 * analyzed plan before optimization, so in-memory node tags set during
 * analysis do not reach the optimizer; and the optimizer's own rewrites
 * (predicate push-down) move nodes between batches, so positional coordinates
 * do not survive either. The shape-free key — the node's own coordinate
 * computed at a fixed synthetic position (depth 0, ordinal -1) — is invariant
 * across the clone, across optimizer rebuilds (the node's expressions are
 * preserved or recomputed identically), and across push-down relocation, while
 * remaining discriminative for distinct logical nodes (distinct operator type
 * or expression signature). The catalogued NodeCoordinate/MutantID formulas
 * are untouched: the catalog still addresses mutants by their fully
 * positional post-hoc coordinate.
 *
 * MUST be idempotent per invocation of `apply` (Catalyst may invoke a rule
 * more than once per query, e.g. under batch re-execution) — the rewritten
 * node is tagged AlreadyMutatedTag and the pending entry is consumed on
 * rewrite, so repeat visits are no-ops, never double mutations.
 *
 * Honesty contract: AppliedMutantTracker is recorded ONLY by the Optimizer
 * phase after a rewrite is actually applied — never on a post-hoc match
 * without a rewrite, and never on a no-match. A mutant whose coordinate
 * matched no plan node, or whose tagged plan never reached the optimizer
 * (discarded intermediate DataFrame), therefore reports "not applied" at the
 * harness, which classifies it ERRORED instead of a fake SURVIVED.
 *
 * @throws io.github.wpunit13.mutator.api.ShimMutationException propagated
 *         unchanged from the underlying shim call; caught by the harness and
 *         classified ERRORED.
 */
class CatalystMutationRule(shim: PlanMutatorShim, phase: CatalystMutationRule.Phase)
  extends Rule[LogicalPlan] {

  import CatalystMutationRule.{AlreadyMutatedTag, consumePendingRewrite, peekPendingRewrite, recordPendingRewrite}

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Read the registry state exactly once per apply call: it can change
    // concurrently and we need a consistent view for the whole invocation.
    val activeMutantId = MutantRegistry.getInstance().getActiveMutantOrNull()
    if (activeMutantId == null) {
      phase match {
        case CatalystMutationRule.PostHoc   => discovery(plan)
        case CatalystMutationRule.Optimizer => plan
      }
    } else {
      phase match {
        case CatalystMutationRule.PostHoc   => matchAndRecord(plan, activeMutantId)
        case CatalystMutationRule.Optimizer => rewriteMatched(plan, activeMutantId)
      }
    }
  }

  /**
   * Branch A — Discovery mode (registry IDLE, PostHoc phase). Classifies every
   * node pre-order and reports candidates to the catalog builder. Returns the
   * plan completely unchanged; no new nodes are constructed here.
   */
  private def discovery(plan: LogicalPlan): LogicalPlan = {
    def walk(node: LogicalPlan, depth: Int, childOrdinal: Int): Unit = {
      shim.classify(node, depth, childOrdinal).foreach { case (operatorType, candidates) =>
        val operatorTag = NodeCoordinateFactory.operatorTypeTag(operatorType)
        val dto = toDto(operatorType)
        candidates.foreach { candidate =>
          val coordinateHex = candidate.coordinate.toHex
          val mutantId = DeterministicHasher.computeMutantId(
            CatalystMutationRule.currentFilePathHint,
            coordinateHex,
            operatorTag,
            candidate.mutationIndex)
          // -1 for lineNumber: nothing computes a real source line yet and
          // the model documents -1 as "unknown".
          MutationCatalogAccess.sink().registerCandidate(
            CatalystMutationRule.currentFilePathHint,
            -1,
            dto,
            candidate.mutationIndex,
            candidate.description,
            coordinateHex,
            mutantId)
        }
      }
      node.children.zipWithIndex.foreach { case (child, ordinal) =>
        walk(child, depth + 1, ordinal)
      }
    }

    walk(plan, 0, -1)
    plan
  }

  /**
   * Branch B — Active mode (registry ACTIVE, PostHoc phase). Re-derives
   * coordinates via classify (identical coordinate space to Discovery) and,
   * on a match, records the matched node's shape-free key for the Optimizer
   * phase. The plan is returned unchanged.
   */
  private def matchAndRecord(plan: LogicalPlan, activeMutantId: String): LogicalPlan = {
    val meta = MutationCatalogAccess.findByIdOrNull(activeMutantId)
    if (meta == null) {
      // The harness activated a mutant this JVM never catalogued; that is a
      // harness scheduling bug, diagnosed at the harness level. Never crash
      // the rule for it.
      return plan
    }

    var matched: Option[LogicalPlan] = None

    def walk(node: LogicalPlan, depth: Int, childOrdinal: Int): Unit = {
      if (matched.isEmpty) {
        shim.classify(node, depth, childOrdinal).foreach { case (_, candidates) =>
          if (candidates.exists { candidate =>
                candidate.coordinate.toHex == meta.getCoordinateHex &&
                candidate.mutationIndex == meta.getMutationIndex
              }) {
            matched = Some(node)
          }
        }
        if (matched.isEmpty) {
          node.children.zipWithIndex.foreach { case (child, ordinal) =>
            walk(child, depth + 1, ordinal)
          }
        }
      }
    }

    walk(plan, 0, -1)

    matched match {
      case None =>
        // No node matches after a full traversal: no-op, do not throw. The
        // honesty guard turns the residual no-match into a loud ERRORED at
        // the harness level (no rewrite -> no tracker record).
        plan
      case Some(node) =>
        // Record the node's shape-free key: the coordinate it yields when
        // classified as the plan root. Invariant across the QueryExecution
        // plan clone, optimizer rebuilds, and predicate push-down.
        shim.classify(node, 0, -1).foreach { case (_, candidates) =>
          candidates.find(_.mutationIndex == meta.getMutationIndex).foreach { candidate =>
            recordPendingRewrite(activeMutantId, candidate.coordinate.toHex)
          }
        }
        plan
    }
  }

  /**
   * Branch C — Active mode (registry ACTIVE, Optimizer phase). Finds the first
   * node whose shape-free classification matches the pending entry and applies
   * exactly one rewrite. Runs after the analyzer's Finish Analysis check, so
   * schema-breaking rewrites reach the optimizer, whose own rules repair the
   * plan (e.g. ColumnPruning dropping the USING-join dedup Project's dead
   * right-side columns before an ANTI join drops them).
   */
  private def rewriteMatched(plan: LogicalPlan, activeMutantId: String): LogicalPlan = {
    val pending = peekPendingRewrite()
    if (pending == null) {
      // The post-hoc phase never matched this mutant (coordinate matched no
      // plan node), or the rewrite already happened. No-op here: the honesty
      // guard classifies the fork as not-applied (ERRORED) because no rewrite
      // was recorded.
      return plan
    }
    val (pendingMutantId, shapeFreeKey) = pending
    if (pendingMutantId != activeMutantId) {
      // Stale entry from a previous mutant; ignore it (never rewrite across
      // mutant boundaries).
      return plan
    }

    val meta = MutationCatalogAccess.findByIdOrNull(activeMutantId)
    if (meta == null) {
      // The harness activated a mutant this JVM never catalogued; harness
      // scheduling bug, diagnosed at the harness level. Never crash the rule.
      return plan
    }

    var matched: Option[LogicalPlan] = None

    def walk(node: LogicalPlan): Unit = {
      if (matched.isEmpty) {
        if (node.getTagValue(AlreadyMutatedTag).contains(true)) {
          // Already rewritten for this mutant (batch re-execution): skip.
        } else {
          shim.classify(node, 0, -1).foreach { case (_, candidates) =>
            if (candidates.exists { candidate =>
                  candidate.coordinate.toHex == shapeFreeKey &&
                  candidate.mutationIndex == meta.getMutationIndex
                }) {
              matched = Some(node)
            }
          }
          if (matched.isEmpty) {
            node.children.foreach(walk)
          }
        }
      }
    }

    walk(plan)

    matched match {
      case None =>
        // The matched logical node never reached the optimizer (discarded
        // intermediate DataFrame) or was eliminated by earlier optimizer
        // rules. Keep the pending entry so later batches can still match;
        // if no batch ever matches, the honesty guard classifies the fork as
        // not-applied (tracker stays empty).
        plan
      case Some(node) =>
        val rewritten = meta.getOperatorType match {
          case OperatorTypeDto.JOIN      => shim.mutateJoin(node, meta.getMutationIndex)
          case OperatorTypeDto.FILTER    => shim.mutateFilter(node, meta.getMutationIndex)
          case OperatorTypeDto.AGGREGATE => shim.mutateAggregate(node, meta.getMutationIndex)
          case OperatorTypeDto.WINDOW    => shim.mutateWindow(node, meta.getMutationIndex)
          case OperatorTypeDto.PROJECT   => shim.mutateProject(node, meta.getMutationIndex)
          case OperatorTypeDto.OTHER     => node
        }
        // setTagValue returns Unit in Spark 3.5.x (it returned this.type
        // in older Spark lines), so the tag is set in place on `rewritten`.
        rewritten.setTagValue(AlreadyMutatedTag, true)
        // Honesty tracking: record the applied fact only after a rewrite
        // was actually applied — never on a no-match.
        AppliedMutantTracker.record(activeMutantId)
        // One-shot: consume the pending entry so later batches/iterations
        // cannot rewrite a second node for the same mutant.
        consumePendingRewrite()
        // Replace only the matched node; reference-equality guard ensures
        // exactly one node is spliced and every other node is preserved.
        plan.transformDown { case n if n eq node => rewritten }
    }
  }

  /** Explicit total mapping; no string reflection. */
  private def toDto(operatorType: OperatorType): OperatorTypeDto = operatorType match {
    case OperatorType.Join      => OperatorTypeDto.JOIN
    case OperatorType.Filter    => OperatorTypeDto.FILTER
    case OperatorType.Aggregate => OperatorTypeDto.AGGREGATE
    case OperatorType.Window    => OperatorTypeDto.WINDOW
    case OperatorType.Project   => OperatorTypeDto.PROJECT
    case OperatorType.Other     => OperatorTypeDto.OTHER
  }
}

object CatalystMutationRule {

  /** Which registration point this rule instance serves. */
  sealed trait Phase
  case object PostHoc extends Phase
  case object Optimizer extends Phase

  /** Tags a node that has already been rewritten for the active mutant. */
  private[mutator] val AlreadyMutatedTag = TreeNodeTag[Boolean]("spark-mutator.alreadyMutated")

  /**
   * Cross-phase handoff from PostHoc (match) to Optimizer (rewrite):
   * (mutantId, shapeFreeCoordinateHex). A JVM-global reference — deliberately
   * NOT a node tag — because QueryExecution clones the analyzed plan before
   * optimization and node tags do not survive the clone.
   */
  private val pendingRewrite = new AtomicReference[(String, String)](null)

  private def recordPendingRewrite(mutantId: String, shapeFreeKey: String): Unit =
    pendingRewrite.set((mutantId, shapeFreeKey))

  private def peekPendingRewrite(): (String, String) = pendingRewrite.get()

  /** One-shot consumption after a successful rewrite. */
  private def consumePendingRewrite(): Unit = pendingRewrite.set(null)

  private val filePathHint = new AtomicReference[String]("unknown")

  /** Wired in by the test harness; defaults to "unknown" until then. */
  def setCurrentFilePathHint(filePath: String): Unit = filePathHint.set(filePath)

  def currentFilePathHint: String = filePathHint.get()
}