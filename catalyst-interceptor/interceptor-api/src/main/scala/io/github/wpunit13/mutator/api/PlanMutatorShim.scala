package io.github.wpunit13.mutator.api

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait PlanMutatorShim {

  /** Identifies the exact Spark + Scala binary version cell this shim targets,
    * e.g. SparkShimVersion("3.5", "2.13"). Used for diagnostic logging only. */
  def supportedVersion: SparkShimVersion

  /**
   * Read-only classification of a single plan node during Discovery.
   * MUST NOT mutate or construct any new LogicalPlan/Expression node.
   *
   * @param node a single node from the analyzed LogicalPlan tree (not the whole tree).
   * @param depth pre-order DFS depth of `node` from the tree root.
   * @param childOrdinal this node's index within its parent's `children`, or -1 for the root.
   * @return `None` if `node` is not a supported mutation target;
   *         otherwise `Some` of its classification plus every applicable
   *         MutationCandidate.
   */
  def classify(
    node: LogicalPlan,
    depth: Int,
    childOrdinal: Int
  ): Option[(OperatorType, Seq[MutationCandidate])]

  /**
   * Rewrites a single Join node according to the mutation identified by `mutationIndex`.
   * Called only when MutantRegistry is ACTIVE and `coordinate` matches this node.
   *
   * @throws ShimMutationException if `node` is not a Join-shaped node in this Spark
   *         version's concrete AST, or if `mutationIndex` is out of range.
   */
  def mutateJoin(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /** Rewrites a single Filter node. Same contract shape as mutateJoin. */
  def mutateFilter(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /** Rewrites a single Aggregate node. Same contract shape as mutateJoin. */
  def mutateAggregate(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /** Rewrites a single Window node. Same contract shape as mutateJoin. */
  def mutateWindow(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /** Rewrites a single Project node. Same contract shape as mutateJoin. */
  def mutateProject(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /**
   * Computes the canonical expression signature for a node. Must be
   * byte-identical in output across all shim implementations for the same
   * logical query.
   */
  def canonicalExprSig(node: LogicalPlan, operatorType: OperatorType): String
}
