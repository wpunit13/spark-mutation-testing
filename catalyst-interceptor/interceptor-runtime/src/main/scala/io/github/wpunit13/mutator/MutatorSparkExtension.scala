package io.github.wpunit13.mutator

import io.github.wpunit13.mutator.dispatch.ShimDispatcher
import org.apache.spark.sql.SparkSessionExtensions

/**
 * Registered via spark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension.
 * Injects CatalystMutationRule at TWO points that share one coordinate space:
 *
 *  - injectPostHocResolutionRule: the analyzer's "Post-Hoc Resolution" batch
 *    (Once strategy — exactly one invocation per analysis, on the fully
 *    resolved analyzed plan). Discovery classification happens here, and in
 *    active mode the rule matches the active mutant's coordinate and tags the
 *    matched node; the plan is returned unchanged.
 *  - injectOptimizerRule: the extended operator-optimization rules. In active
 *    mode this rule performs the actual rewrite, matched shape-free.
 *
 * Batch-ordering verification (against the Spark 3.5.3 sources jars in
 * the local repository, spark-catalyst_2.13-3.5.3-sources.jar and
 * spark-sql_2.13-3.5.3-sources.jar):
 *
 *  - BaseSessionStateBuilder.analyzer appends
 *    `extensions.buildPostHocRules(session)` to Analyzer.postHocResolutionRules,
 *    which execute in the analyzer's "Post-Hoc Resolution" batch (Once
 *    strategy), after the main "Resolution" batch. One logical node => one
 *    NodeCoordinate => no duplicate mutants, and no coordinates minted from
 *    transient optimizer shapes (predicate push-down copies,
 *    InferFiltersFromConstraints-injected filters).
 *  - The analyzer's Finish Analysis check runs AFTER the Post-Hoc batch, so a
 *    rewrite applied there would be rejected before the optimizer — the only
 *    component that can repair schema-breaking plans — executes. Hence the
 *    Post-Hoc phase only records the match, and the rewrite is deferred to
 *    the optimizer.
 *  - BaseSessionStateBuilder.optimizer wraps
 *    `extensions.buildOptimizerRules(session)` into
 *    SparkOptimizer.extendedOperatorOptimizationRules, which execute inside
 *    the nested "Operator Optimization before/after Inferring Filters" batches
 *    of the top-level "Operator Optimizations" batch — strictly after analysis
 *    and its check, and before physical planning. Therefore a mutated Join
 *    type is still subject to normal join-strategy selection, satisfying
 *    §3.2: Discovery sees the fully resolved analyzed plan, and mutation
 *    application precedes optimization.
 *  - The cross-phase handoff is a JVM-global (mutantId, shape-free key) slot,
 *    NOT a node tag: QueryExecution clones the analyzed plan before
 *    optimization, and node tags do not survive the clone. The shape-free key
 *    (the node's coordinate computed at a fixed synthetic root position) is
 *    invariant across the clone, optimizer rebuilds, and predicate push-down.
 *
 * Because discovery and mutation-matching happen at the same deterministic
 * post-hoc shape, the active mutant's coordinate always recurs in the mutant
 * fork; because the rewrite happens at the optimizer entry, every mutation is
 * actually executed.
 */
class MutatorSparkExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    DriverFatalShutdownHook.register()
    extensions.injectPostHocResolutionRule(
      _ => new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.PostHoc))
    extensions.injectOptimizerRule(
      _ => new CatalystMutationRule(ShimDispatcher.activeShim, CatalystMutationRule.Optimizer))
  }
}