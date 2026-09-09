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
 * A single Rule[LogicalPlan] instance, added once, reused for the
 * SparkSession's lifetime. Behavior branches entirely on
 * MutantRegistry.getInstance().getActiveMutantOrNull():
 *   - null (IDLE)       -> classify-and-catalogue mode (Discovery), returns
 *                          `plan` unchanged.
 *   - non-null (ACTIVE) -> mutate mode: locates the node whose NodeCoordinate
 *                          matches the active mutant's target coordinate and
 *                          applies exactly one rewrite.
 *
 * MUST be idempotent per invocation of `apply` (Catalyst may invoke a rule
 * more than once per query, e.g. under batch re-execution) -- re-applying to
 * an already-mutated plan for the same active mutant id must be a no-op, not
 * a double mutation. Satisfied by tagging the rewritten node with
 * AlreadyMutatedTag so repeat visits skip it.
 *
 * @throws io.github.wpunit13.mutator.api.ShimMutationException propagated
 *         unchanged from the underlying shim call; caught by the harness and
 *         classified ERRORED.
 */
class CatalystMutationRule(shim: PlanMutatorShim) extends Rule[LogicalPlan] {

  import CatalystMutationRule.AlreadyMutatedTag

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Read the registry state exactly once per apply call: it can change
    // concurrently and we need a consistent view for the whole invocation.
    val activeMutantId = MutantRegistry.getInstance().getActiveMutantOrNull()
    if (activeMutantId == null) {
      discovery(plan)
    } else {
      mutate(plan, activeMutantId)
    }
  }

  /**
   * Branch A — Discovery mode (registry IDLE). Classifies every node
   * pre-order and reports candidates to the catalog builder. Returns the
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
   * Branch B — Active mode (registry ACTIVE). Re-derives coordinates via
   * classify (identical coordinate space to Discovery) and applies exactly
   * one rewrite at the first matching node.
   */
  private def mutate(plan: LogicalPlan, activeMutantId: String): LogicalPlan = {
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
        // No node matches after a full traversal: no-op, do not throw.
        plan
      case Some(node) =>
        if (node.getTagValue(AlreadyMutatedTag).contains(true)) {
          // Idempotency guard: Catalyst may invoke the rule repeatedly;
          // an already-mutated node must never be mutated a second time.
          plan
        } else {
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
          // Replace only the matched node; reference-equality guard ensures
          // exactly one node is spliced and every other node is preserved.
          plan.transformDown { case n if n eq node => rewritten }
        }
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

  /** Tags a node that has already been rewritten for the active mutant. */
  private val AlreadyMutatedTag = TreeNodeTag[Boolean]("spark-mutator.alreadyMutated")

  private val filePathHint = new AtomicReference[String]("unknown")

  /** Wired in by the test harness; defaults to "unknown" until then. */
  def setCurrentFilePathHint(filePath: String): Unit = filePathHint.set(filePath)

  def currentFilePathHint: String = filePathHint.get()
}
