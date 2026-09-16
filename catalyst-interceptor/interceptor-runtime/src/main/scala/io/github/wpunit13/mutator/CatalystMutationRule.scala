package io.github.wpunit13.mutator

import io.github.wpunit13.mutator.api.{NodeCoordinateFactory, OperatorType, PlanMutatorShim}
import io.github.wpunit13.mutator.catalog.MutationCatalogAccess
import io.github.wpunit13.mutator.hash.DeterministicHasher
import io.github.wpunit13.mutator.model.OperatorTypeDto
import io.github.wpunit13.mutator.report.DiffSnippetStore
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.slf4j.LoggerFactory

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
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

  import CatalystMutationRule.{AlreadyMutatedTag, MutationPolicy, consumePendingRewrite, peekPendingRewrite, recordPendingRewrite}

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
   *
   * WP-19 filtering, both directives read once per invocation (the system
   * properties are static per fork JVM; in-process mode picks up live
   * values):
   *  - `spark.mutator.excludedMutators`: candidates whose operator type is
   *    excluded are skipped (a user choice, not an error — but the skip is
   *    logged so it is observable).
   *  - `spark.mutator.targetModules`: when non-blank, candidates register
   *    only while the current file-path hint starts with one of the prefixes.
   *    If the hint is still the default "unknown" (no harness fed a hint),
   *    NOTHING is registered and a single warning is emitted — silent
   *    full-catalog behavior here would be the "every mutant survived"
   *    failure mode in disguise.
   */
  private def discovery(plan: LogicalPlan): LogicalPlan = {
    val policy = MutationPolicy.fromSystemProperties()
    CatalystMutationRule.warnUnrecognizedExclusionsOnce(policy.unrecognizedExclusions)
    val hint = CatalystMutationRule.currentFilePathHint
    if (policy.targetModulePrefixes.nonEmpty && hint == CatalystMutationRule.DefaultFilePathHint) {
      CatalystMutationRule.warnUnknownFilePathHintOnce(policy.targetModulePrefixes)
      return plan
    }
    val targetAllowed = policy.targetModulePrefixes.isEmpty ||
      policy.targetModulePrefixes.exists(hint.startsWith)
    var excludedSkips = 0

    def walk(node: LogicalPlan, depth: Int, childOrdinal: Int): Unit = {
      shim.classify(node, depth, childOrdinal).foreach { case (operatorType, candidates) =>
        val dto = toDto(operatorType)
        if (policy.excludedOperators.contains(dto)) {
          excludedSkips += 1
        } else if (targetAllowed) {
          val operatorTag = NodeCoordinateFactory.operatorTypeTag(operatorType)
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
      }
      node.children.zipWithIndex.foreach { case (child, ordinal) =>
        walk(child, depth + 1, ordinal)
      }
    }

    walk(plan, 0, -1)
    if (excludedSkips > 0) {
      CatalystMutationRule.log(
        s"discovery skipped $excludedSkips candidate(s) on excluded operators " +
          policy.excludedOperators.mkString("{", ", ", "}") +
          " (spark.mutator.excludedMutators)")
    }
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
    if (MutationPolicy.fromSystemProperties().excludedOperators.contains(meta.getOperatorType)) {
      // Fail-safe: an excluded mutant must never apply, even if a stale
      // catalog entry exists. Exclusion is a user choice, not an error — but
      // the refusal is observable.
      CatalystMutationRule.log(
        s"active mutant $activeMutantId (${meta.getOperatorType}) is excluded by " +
          "spark.mutator.excludedMutators; refusing to match")
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
            recordPendingRewrite(activeMutantId, candidate.coordinate.toHex, node)
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
    val pendingMutantId = pending.mutantId
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
    if (MutationPolicy.fromSystemProperties().excludedOperators.contains(meta.getOperatorType)) {
      // Fail-safe: an excluded mutant must never apply, even if a stale
      // catalog entry or a pending rewrite exists. User choice, not an error
      // — but the refusal is observable.
      CatalystMutationRule.log(
        s"active mutant $activeMutantId (${meta.getOperatorType}) is excluded by " +
          "spark.mutator.excludedMutators; refusing to rewrite")
      return plan
    }

    var matched: Option[LogicalPlan] = None
    val shapeFreeKey = pending.shapeFreeKey

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

    if (matched.isEmpty) {
      // Fallback: optimizer rewrites (constant/cast folding, alias removal,
      // push-down rebuilds, column pruning) can change the analyzed node's
      // expression tree AND reshape its output schema, so the shape-free key
      // recorded at post-hoc no longer matches any node. Re-identify the
      // matched node by its stable identity instead:
      //   - node class,
      //   - the recorded output columns must still be produced by the
      //     candidate (recorded ⊆ candidate): predicate push-down can WIDEN a
      //     Filter's schema (it moves below a Project onto the wider child),
      //     while a candidate that LOST recorded columns is a different site
      //     — refusing it prevents cross-branch rewrites,
      //   - an inserted-null-guard check (predicate push-down splits analyzed
      //     And-filters and inserts IsNotNull guards; mutating a pure guard
      //     would under-apply the mutant and can fake a SURVIVED),
      //   - a mutation-index availability check (the shim must actually offer
      //     this index for the candidate's shape).
      // Empty-schema stubs (e.g. the Project(Nil) ColumnPruning inserts under
      // count(1)-style aggregates) are never matched: a rewrite against them
      // would be a no-op masquerading as an applied mutation. Same "first node
      // wins" ambiguity policy as the exact key; a miss here still leaves the
      // honesty guard's not-applied classification intact.
      def walkFallback(node: LogicalPlan): Unit = {
        if (matched.isEmpty) {
          val candidateFieldNames = node.schema.map(_.name).toSet
          if (!node.getTagValue(AlreadyMutatedTag).contains(true) &&
              node.schema.nonEmpty &&
              node.getClass.getSimpleName == pending.nodeClass &&
              (pending.referencedColumns.isEmpty ||
                pending.referencedColumns.exists(candidateFieldNames.contains)) &&
              !isInsertedNullGuard(node, pending.exprClasses) &&
              offersMutationIndex(node, meta.getMutationIndex)) {
            matched = Some(node)
          }
          if (matched.isEmpty) {
            node.children.foreach(walkFallback)
          }
        }
      }
      walkFallback(plan)
      matched.foreach { _ =>
        CatalystMutationRule.log(
          s"mutant $activeMutantId matched via identity fallback: the optimizer rewrote the " +
            "analyzed node's expressions or pruned its output, so its shape-free key drifted")
      }
    }

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
        // WP-19: pure capture of the before/after plan shapes around the
        // rewrite. Observation only — it never alters the returned plan, the
        // coordinate space, or the applied-mutation record.
        val diffSnippet = astDiffSnippet(node, rewritten)
        // setTagValue returns Unit in Spark 3.5.x (it returned this.type
        // in older Spark lines), so the tag is set in place on `rewritten`.
        rewritten.setTagValue(AlreadyMutatedTag, true)
        // Honesty tracking: record the applied fact only after a rewrite
        // was actually applied — never on a no-match.
        AppliedMutantTracker.record(activeMutantId)
        persistDiffSnippet(activeMutantId, diffSnippet)
        // One-shot: consume the pending entry so later batches/iterations
        // cannot rewrite a second node for the same mutant.
        consumePendingRewrite()
        // Replace only the matched node; reference-equality guard ensures
        // exactly one node is spliced and every other node is preserved.
        plan.transformDown { case n if n eq node => rewritten }
    }
  }

  /**
   * Fallback guard — inserted-null-guard refusal. Predicate push-down splits
   * an analyzed And-filter and INSERTS IsNotNull guards that the original
   * node never had. A candidate whose every top-level expression is such a
   * guard (and whose recorded expressions contained none) is an
   * optimizer-inserted artifact, not the recorded node's descendant:
   * mutating it under-applies the mutant and can fake a SURVIVED — the worst
   * failure mode this tool produces. Everything else that offers the recorded
   * mutation index is accepted: the optimizer legitimately ADDS conjuncts to
   * join conditions and Alias wrappers to project lists, so a structural
   * class-subset check would refuse legitimate evolutions.
   */
  private def isInsertedNullGuard(candidate: LogicalPlan, recordedExprClasses: Set[String]): Boolean =
    candidate.expressions.nonEmpty &&
      candidate.expressions.forall(_.getClass.getSimpleName == "IsNotNull") &&
      !recordedExprClasses.contains("IsNotNull")

  /**
   * Fallback guard — the candidate must actually OFFER the recorded
   * mutation index (e.g. keep-left/keep-right require a top-level And;
   * window mutations require an order spec). Prevents the shim from throwing
   * ShimMutationException on a shape-incompatible node mid-rewrite.
   */
  private def offersMutationIndex(node: LogicalPlan, mutationIndex: Int): Boolean =
    shim.classify(node, 0, -1).exists { case (_, candidates) =>
      candidates.exists(_.mutationIndex == mutationIndex)
    }

  /**
   * Builds the schema's astDiffSnippet: "<before-fragment> => <after-fragment>",
   * each fragment a single-line plan string truncated to
   * [[CatalystMutationRule.MaxPlanFragmentChars]] (newlines flattened to
   * " | ").
   */
  private def astDiffSnippet(before: LogicalPlan, after: LogicalPlan): String =
    s"${CatalystMutationRule.planFragment(before)} => ${CatalystMutationRule.planFragment(after)}"

  /**
   * Persistence of the captured snippet. In-process paths (JUnit 5
   * standalone, the PySpark driver) reach the final report through the live
   * catalog singleton, so recording there is sufficient. Externally
   * orchestrated mutant forks have an output directory and the `mutant`
   * phase set: their in-memory copy dies with the JVM, so the snippet is
   * additionally persisted as a `diffs/<mutantId>.json` sidecar that the
   * coordinator merges before writing reports. A failed sidecar write is a
   * contract violation and fails loudly rather than silently dropping data.
   */
  private def persistDiffSnippet(mutantId: String, snippet: String): Unit = {
    MutationCatalogAccess.recordAstDiffSnippet(mutantId, snippet)
    if (MutantBootstrap.phaseOrNull() == MutantBootstrap.PHASE_MUTANT) {
      MutantBootstrap.outputDirectoryOrNull() match {
        case null | "" =>
          // No output directory configured: nothing to persist. The harness
          // contract would already have failed the applied-marker step.
        case dir =>
          try {
            DiffSnippetStore.write(java.nio.file.Path.of(dir), mutantId, snippet)
          } catch {
            case e: IOException =>
              throw new IllegalStateException(
                s"Could not write diff snippet sidecar for mutant '$mutantId' under $dir.", e)
          }
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

  /** Which registration point this rule instance serves. */
  sealed trait Phase
  case object PostHoc extends Phase
  case object Optimizer extends Phase

  /**
   * The file-path hint every harness starts with. `spark.mutator.targetModules`
   * treats it as "no hint fed" and refuses to register anything (fail-safe).
   */
  val DefaultFilePathHint: String = "unknown"

  /** Max chars per plan fragment in astDiffSnippet (single shared constant). */
  val MaxPlanFragmentChars: Int = 2000

  /**
   * The WP-19 fork directives, parsed once per rule invocation (system
   * properties are static per fork JVM, so this is behaviorally identical to
   * reading once per instance there; in-process mode picks up live values).
   * Exclusion keys are OperatorType names, case-insensitive; unrecognized
   * tokens (e.g. legacy mutator display names understood only by the
   * embedding harness) are reported back so the caller can warn once and
   * ignore.
   */
  private[mutator] final case class MutationPolicy(
      excludedOperators: Set[OperatorTypeDto],
      targetModulePrefixes: Vector[String],
      unrecognizedExclusions: Vector[String])

  private[mutator] object MutationPolicy {
    val ExcludedMutatorsProp: String = "spark.mutator.excludedMutators"
    val TargetModulesProp: String = "spark.mutator.targetModules"

    private val operatorNames: Set[String] =
      Set("JOIN", "FILTER", "AGGREGATE", "WINDOW", "PROJECT", "OTHER")

    def fromSystemProperties(): MutationPolicy = {
      val excludedRaw = splitCsv(System.getProperty(ExcludedMutatorsProp))
      val excluded = excludedRaw
        .map(_.toUpperCase)
        .filter(operatorNames.contains)
        .map(OperatorTypeDto.valueOf)
        .toSet
      val unrecognized = excludedRaw.filterNot(token => operatorNames.contains(token.toUpperCase))
      MutationPolicy(excluded, splitCsv(System.getProperty(TargetModulesProp)), unrecognized)
    }

    private def splitCsv(value: String): Vector[String] =
      if (value == null || value.isBlank) Vector.empty
      else value.split(',').map(_.trim).filter(_.nonEmpty).toVector
  }

  /**
   * The rule's observable-skip/warn channel. slf4j (provided scope) binds to
   * the driver's existing log4j2 runtime — the same logger every Spark
   * extension uses. Everything logged here is WARN-level so it survives the
   * default Spark log level; the `spark-mutator` logger name keeps the
   * messages greppable and separately tunable.
   */
  private val logger = LoggerFactory.getLogger("spark-mutator")

  private[mutator] def log(message: String): Unit = logger.warn(message)

  private val warnedUnknownFilePathHint = new AtomicBoolean(false)

  /** Single warning when targetModules is set but no hint was ever fed. */
  private[mutator] def warnUnknownFilePathHintOnce(prefixes: Vector[String]): Unit =
    if (warnedUnknownFilePathHint.compareAndSet(false, true)) {
      log(
        "spark.mutator.targetModules is set to " + prefixes.mkString("[", ", ", "]") +
          " but no harness has fed a file-path hint (hint is the default \"" +
          DefaultFilePathHint + "\"); registering NO candidates. Feed real hints via " +
          "CatalystMutationRule.setCurrentFilePathHint or unset the property.")
    }

  private val warnedUnrecognizedExclusions = new AtomicBoolean(false)

  /**
    * Single warning for exclusion tokens that are not OperatorType names —
    * e.g. legacy mutator display names, which only the embedding harness can
    * resolve (the Python layer enforces those itself). Ignoring them here is
    * deliberate; staying silent about it would not be.
    */
  private[mutator] def warnUnrecognizedExclusionsOnce(tokens: Vector[String]): Unit =
    if (warnedUnrecognizedExclusions.compareAndSet(false, true) && tokens.nonEmpty) {
      log(
        "ignoring non-OperatorType exclusion token(s) in " +
          MutationPolicy.ExcludedMutatorsProp + ": " + tokens.mkString("[", ", ", "]") +
          " (canonical keys are JOIN, FILTER, AGGREGATE, WINDOW, PROJECT, OTHER; " +
          "legacy mutator names are enforced by the embedding harness)")
    }

  /** Tags a node that has already been rewritten for the active mutant. */
  private[mutator] val AlreadyMutatedTag = TreeNodeTag[Boolean]("spark-mutator.alreadyMutated")

  /**
   * Cross-phase handoff from PostHoc (match) to Optimizer (rewrite).
   *
   * @param mutantId     the mutant the pending rewrite belongs to.
   * @param shapeFreeKey the matched node's root-classification coordinate —
   *                     primary match key. Optimizer rewrites (constant/cast
   *                     folding, alias removal, push-down rebuilds) can change
   *                     the analyzed node's expression tree, so this key can
   *                     drift; nodeClass + referencedColumns + exprClasses
   *                     are the stable fallback.
   * @param nodeClass    simple class name of the matched node.
   * @param referencedColumns the attribute names the recorded node's
   *                     expressions reference. Schema width evolves in BOTH
   *                     directions across replanning (push-down widens a
   *                     Filter below a Project; pruning narrows
   *                     joins/projects), so width is not compared — the
   *                     fallback requires only that the evolved node still
   *                     references at least one recorded column (non-empty
   *                     overlap).
   * @param exprClasses  every expression-class simple name appearing in the
   *                     recorded node's expressions. Folding only removes
   *                     classes, so an evolved candidate's top-level classes
   *                     must be a subset; optimizer-INSERTED expressions
   *                     (IsNotNull guards) introduce new classes and are
   *                     excluded.
   *
   * A JVM-global reference — deliberately NOT a node tag — because
   * QueryExecution clones the analyzed plan before optimization and node tags
   * do not survive the clone.
   */
  private[mutator] final case class PendingRewrite(
      mutantId: String,
      shapeFreeKey: String,
      nodeClass: String,
      referencedColumns: Set[String],
      exprClasses: Set[String])

  private val pendingRewrite = new AtomicReference[PendingRewrite](null)

  private def recordPendingRewrite(
      mutantId: String, shapeFreeKey: String, node: LogicalPlan): Unit =
    pendingRewrite.set(PendingRewrite(
      mutantId,
      shapeFreeKey,
      node.getClass.getSimpleName,
      node.expressions.flatMap(_.references).map(_.name).toSet,
      node.expressions.flatMap(_.collect { case e => e.getClass.getSimpleName }).toSet))

  private def peekPendingRewrite(): PendingRewrite = pendingRewrite.get()

  /** One-shot consumption after a successful rewrite. */
  private def consumePendingRewrite(): Unit = pendingRewrite.set(null)

  /** Single-line plan fragment for astDiffSnippet (shared constant bound). */
  private[mutator] def planFragment(plan: LogicalPlan): String = {
    val singleLine = plan.toString.replace("\r\n", " | ").replace("\n", " | ")
    if (singleLine.length > MaxPlanFragmentChars) singleLine.substring(0, MaxPlanFragmentChars)
    else singleLine
  }

  private val filePathHint = new AtomicReference[String](DefaultFilePathHint)

  /** Wired in by the test harness; defaults to "unknown" until then. */
  def setCurrentFilePathHint(filePath: String): Unit = filePathHint.set(filePath)

  def currentFilePathHint: String = filePathHint.get()
}