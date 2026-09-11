package io.github.wpunit13.mutator

import io.github.wpunit13.mutator.dispatch.ShimDispatcher
import org.apache.spark.sql.SparkSessionExtensions

/**
 * Registered via spark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension.
 * Injects CatalystMutationRule as an optimizer-extension rule (for active
 * mutation application and Discovery classification).
 */
class MutatorSparkExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Batch-ordering verification (against the Spark 3.5.3 sources jars in
    // the local repository, spark-catalyst_2.13-3.5.3-sources.jar and
    // spark-sql_2.13-3.5.3-sources.jar):
    //
    //  - BaseSessionStateBuilder.optimizer wraps `extensions.buildOptimizerRules(session)`
    //    into SparkOptimizer.extendedOperatorOptimizationRules.
    //  - Optimizer.defaultBatches appends extendedOperatorOptimizationRules to
    //    operatorOptimizationRuleSet, which executes inside the nested
    //    "Operator Optimization before/after Inferring Filters" batches of the
    //    top-level "Operator Optimizations" batch.
    //  - CostBasedJoinReorder executes in the separate top-level "Join Reorder"
    //    batch (FixedPoint(1)), which is appended AFTER the "Operator
    //    Optimizations" batch (and after "Pre CBO Rules" / "Early Filter and
    //    Projection Push-Down").
    //
    // Therefore an injectOptimizerRule-injected rule always runs before
    // cost-based join reordering in Spark 3.5.x, satisfying §3.2: Discovery
    // sees the fully resolved analyzed plan, and a mutated Join type is still
    // subject to normal join-strategy selection afterward.
    DriverFatalShutdownHook.register()
    extensions.injectOptimizerRule(_ => new CatalystMutationRule(ShimDispatcher.activeShim))
  }
}
