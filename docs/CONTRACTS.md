# `spark-mutator` — Interface & Data Contracts

**Status:** Normative interface specification. Signatures, types, exceptions,
and thread-safety guarantees below are binding. Method bodies are
intentionally omitted — implementation is out of scope for this document.

All Java signatures assume Java 17+. All Scala signatures assume Scala
2.12/2.13 cross-build compatibility (no Scala 3–only syntax). Package root
for all JVM types: `io.github.wpunit13.mutator`.

---

## 1. `MutantRegistry`

**Module:** `mutator-core` (Java). **Nature:** Thread-safe singleton state
machine. Exactly one instance exists per JVM process (per driver).

### 1.1 State Machine

```
      clearActiveMutant()
   ┌────────────────────────┐
   │                         │
   ▼                         │
┌──────┐  setActiveMutant()  ┌────────────┐
│ IDLE │ ───────────────────▶│ ACTIVE(id) │
└──────┘                     └────────────┘
   ▲                              │
   └──────────── reset() ─────────┘
                (force-clear, any state)
```

- `IDLE`: no mutant is currently being evaluated; `CatalystMutationRule` runs
  as a structural no-op pass-through in this state.
- `ACTIVE(id)`: exactly one mutant id is active; `CatalystMutationRule`
  applies mutations addressed to that id.
- Transition `IDLE → ACTIVE(id)` is only legal from `IDLE`. Attempting it
  from `ACTIVE(*)` is a contract violation (§1.3).
- Transition `ACTIVE(id) → IDLE` via `clearActiveMutant()` is only legal when
  the caller is clearing the *currently* active id (enforced by requiring the
  caller to pass the id it expects to clear — see below — to catch
  reset-ordering bugs early rather than silently clearing the wrong mutant).
- `reset()` is an unconditional escape hatch (any state → `IDLE`), used only
  by the circuit-breaker/session-recovery path (`ARCHITECTURE.md` §6.2),
  never by the normal per-mutant loop.

### 1.2 Interface

```java
package io.github.wpunit13.mutator;

public final class MutantRegistry {

    /** Returns the single JVM-wide instance. Thread-safe, idempotent. */
    public static MutantRegistry getInstance();

    /**
     * Transitions IDLE -> ACTIVE(mutantId).
     *
     * @param mutantId non-null, non-blank, matching the MutantID format (16 lowercase hex chars, see §5.1).
     * @throws IllegalArgumentException if mutantId is null, blank, or fails the format check.
     * @throws IllegalStateException if the registry is already ACTIVE for a different mutant id
     *         (i.e. the previous mutant's reset sequence did not run to completion).
     */
    public void setActiveMutant(String mutantId);

    /**
     * Transitions ACTIVE(mutantId) -> IDLE. Must be the last step of the reset sequence
     * (ARCHITECTURE.md §3.2).
     *
     * @param mutantId the id the caller expects to be currently active.
     * @throws IllegalStateException if the registry is IDLE, or if the currently active id
     *         does not equal {@code mutantId} (guards against clearing the wrong mutant due to
     *         a race or ordering bug — this is deliberately strict, not idempotent-on-mismatch).
     */
    public void clearActiveMutant(String mutantId);

    /**
     * Non-blocking read of current state.
     *
     * @return the active mutant id, or {@code null} if IDLE.
     */
    public String getActiveMutantOrNull();

    /**
     * Unconditional forced reset to IDLE regardless of current state. Used exclusively by the
     * driver-resilience circuit breaker (ARCHITECTURE.md §6.2), never by the normal mutation loop.
     * Idempotent.
     */
    public void reset();
}
```

### 1.3 Thread-Safety Model

- All public methods are internally synchronized such that state transitions
  are linearizable: implemented via a single `synchronized` monitor or an
  `AtomicReference<String>` with `compareAndSet`-based transition logic — the
  chosen implementation MUST guarantee no method blocks on anything other
  than acquiring this internal, in-memory lock (specifically: **never** on
  Spark job completion, I/O, or network calls). This is what allows the
  control-channel watchdog thread (`ARCHITECTURE.md` §2.2) to safely call
  `getActiveMutantOrNull()` concurrently with an in-flight execution-channel
  call is running a Spark job.
- `setActiveMutant`/`clearActiveMutant` throwing on misuse is intentional
  fail-fast behavior — silently tolerating an invalid transition would mask
  reset-sequence bugs that corrupt result attribution (`ARCHITECTURE.md` §2.5).

---

## 2. `PlanMutatorShim`

**Module:** `interceptor-api` (Scala). **Nature:** Version-agnostic SPI.
Exactly one implementation is loaded per JVM process, selected at runtime via
`ServiceLoader` by `interceptor-dispatch` (`ARCHITECTURE.md` §5.1–5.4).

### 2.1 Supporting Types (all in `interceptor-api`)

```scala
package io.github.wpunit13.mutator.api

/** Fixed, version-independent operator classification. Never derived from a
  * concrete Catalyst class name — see ARCHITECTURE.md §4.2. */
sealed trait OperatorType
object OperatorType {
  case object Join extends OperatorType
  case object Filter extends OperatorType
  case object Aggregate extends OperatorType
  case object Window extends OperatorType
  case object Project extends OperatorType
  case object Other extends OperatorType
}

/** Opaque 64-bit coordinate per ARCHITECTURE.md §4.2. Equality/hashCode are
  * value-based. toString renders the fixed 16-char lowercase hex form. */
final case class NodeCoordinate(value: Long) {
  def toHex: String
}

/** One entry in the version-agnostic catalog of applicable mutations for a
  * single plan node, produced during Discovery. */
final case class MutationCandidate(
  coordinate: NodeCoordinate,
  operatorType: OperatorType,
  mutationIndex: Int,
  description: String // e.g. "INNER -> CROSS", human-readable, report-facing only
)

/** Thrown when a shim is asked to mutate a node whose live structure no
  * longer matches what was catalogued at Discovery time (e.g. the optimizer
  * restructured the tree in a way the shim did not anticipate between the
  * cataloguing pass and the mutation pass). Always maps to ERRORED. */
final class ShimMutationException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
```

### 2.2 Shim Interface

The SPI is deliberately split into a **classification** phase (used during
Discovery, read-only, must not construct new plan nodes) and a **rewrite**
phase (used during the mutation loop, one method per mutator category from
the spec's Mutator Catalog, §3).

```scala
package io.github.wpunit13.mutator.api

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait PlanMutatorShim {

  /** Identifies the exact Spark/Scala combination this shim targets,
    * e.g. SparkShimVersion("3.5", "2.13"). Used for diagnostic logging only. */
    * runtime selection itself happens one layer up, in interceptor-dispatch. */
  def supportedVersion: SparkShimVersion

  /**
   * Read-only classification of a single plan node during Discovery.
   * MUST NOT mutate or construct any new LogicalPlan/Expression node.
   *
   * @param node a single node from the analyzed LogicalPlan tree (not the whole tree).
   * @param depth pre-order DFS depth of `node` from the tree root (ARCHITECTURE.md §4.2).
   * @param childOrdinal this node's index within its parent's `children`, or -1 for the root.
   * @return `None` if `node` is not a supported mutation target (e.g. it is a leaf relation);
   *         otherwise `Some` of its classification plus every applicable MutationCandidate
   *         (one per row of the Mutator Catalog applicable to this operator type).
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
   * @throws ShimMutationException if `node` is not a Join-shaped node in this Spark version's
   *         concrete AST, or if `mutationIndex` is out of range for Join mutators.
   */
  def mutateJoin(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /** Rewrites a single Filter node. Same contract shape as mutateJoin. */
  def mutateFilter(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /** Rewrites a single Aggregate node. Same contract shape as mutateJoin. */
  def mutateAggregate(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /** Rewrites a single Window node. Same contract shape as mutateJoin. */
  def mutateWindow(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /** Rewrites a single Project node (covers Null/Type mutators operating on
    * projected expressions, e.g. coalesce-stripping, Decimal->Double downgrade).
    * Same contract shape as mutateJoin. */
  def mutateProject(node: LogicalPlan, mutationIndex: Int): LogicalPlan

  /**
   * Computes the canonical expression signature for a node, per the grammar
   * fixed in ARCHITECTURE.md §4.3. Must be byte-identical in output across all
   * shim implementations for the same logical query.
   */
  def canonicalExprSig(node: LogicalPlan, operatorType: OperatorType): String
}

/** Immutable value identifying a shim's target Spark/Scala combination. */
final case class SparkShimVersion(sparkMinor: String, scalaBinary: String)
```

### 2.3 Thread-Safety Model

`PlanMutatorShim` implementations MUST be **stateless and side-effect-free**
with respect to instance fields (pure functions of their arguments). Exactly
one instance is constructed by `interceptor-dispatch` at extension-injection
time and reused for the lifetime of the `SparkSession`; because Catalyst
optimizer rules can, in principle, be invoked from more than one thread when
multiple queries are planned concurrently, shim methods must be safe to call
concurrently with no shared mutable state.

---

## 3. `CatalystMutationRule`

**Module:** `catalyst-interceptor` root (Scala), version-agnostic — this
class itself does not touch concrete case classes; it delegates entirely to
the `PlanMutatorShim` resolved by `interceptor-dispatch`.

### 3.1 Integration Points

```scala
package io.github.wpunit13.mutator

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Registered via spark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension
 * (ARCHITECTURE.md §5.3/5.4). Injects CatalystMutationRule as both an analyzer-extension
 * rule (post-hoc, for Discovery classification) and an optimizer-extension rule
 * (for active mutation application).
 */
class MutatorSparkExtension extends (SparkSessionExtensions => Unit) {
  def apply(extensions: SparkSessionExtensions): Unit
}

/**
 * A single Rule[LogicalPlan] instance, added once, reused for the SparkSession's lifetime.
 * Behavior branches entirely on MutantRegistry.getInstance().getActiveMutantOrNull():
 *   - null (IDLE)      -> classify-and-catalogue mode (Discovery), returns `plan` unchanged.
 *   - non-null (ACTIVE) -> mutate mode: locates the node whose NodeCoordinate matches the
 *                          active mutant's target coordinate and applies exactly one rewrite.
 *
 * MUST be idempotent per invocation of `apply` (Catalyst may invoke a rule more than once
 * per query, e.g. under batch re-execution) — re-applying to an already-mutated plan for the
 * same active mutant id must be a no-op, not a double mutation. Implementations satisfy this
 * by tagging the rewritten node with a TreeNodeTag unique to this rule so repeat visits skip it.
 *
 * @throws io.github.wpunit13.mutator.api.ShimMutationException propagated unchanged from the
 *         underlying shim call; caught by the harness and classified ERRORED.
 */
class CatalystMutationRule(shim: io.github.wpunit13.mutator.api.PlanMutatorShim)
  extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan
}
```

### 3.2 Contract Notes

- `MutatorSparkExtension` is injected exactly once per `SparkSession`
  (standard Spark extension semantics — re-registering the same extensions
  class on `getOrCreate()` against an already-running session is a no-op by
  Spark's own contract, not something this project needs to guard against
  separately).
- `CatalystMutationRule` is registered as a **post-analysis, pre-optimization
  batch rule** (via `extensions.injectOptimizerRule` positioned in the
  earliest custom batch) so that (a) Discovery classification sees the fully
  resolved analyzed plan (attributes resolved, types resolved — required for
  `canonicalExprSig` to be meaningful) and (b) mutation happens before
  Spark's own cost-based optimizations run, so a mutated `Join` type, for
  instance, is still subject to normal join-strategy selection afterward.
- Discovery-mode invocations of this rule (`MutantRegistry` `IDLE`) MUST
  report classification results to `mutator-core`'s catalog builder via a
  narrow callback interface (`MutationCatalogSink`, defined in
  `mutator-core`, accepting only `mutator-core`-owned types —
  `MutationCandidate` is re-exported/mapped to a `mutator-core` DTO at this
  boundary to preserve the "core never imports catalyst" isolation
  guarantee) rather than by the rule returning anything other than the
  unmodified `plan`.

---

## 4. Py4J Wire Signatures

**Module:** `mutator-core` (the classes below must live in `mutator-core`,
*not* `catalyst-interceptor`, so they are reachable without requiring the
active shim jar to expose driver-facing methods — the wire surface must not
depend on which shim happens to be mounted).

All methods below are the **exact** set of JVM entry points the Python
driver process is permitted to call across the Py4J gateway. Every
parameter and return type is restricted to Py4J's natively bridged types
(`String`, `boolean`, `long`) per `ARCHITECTURE.md` §2.4 — no custom POJOs
cross the boundary; structured data is JSON-encoded as a `String`.

```java
package io.github.wpunit13.mutator;

public final class MutantRegistry {
    // (declared fully in §1.2 — the same class is the Py4J entry point;
    //  no separate driver-facing wrapper class is introduced for this contract.)

    public static MutantRegistry getInstance();          // called once per pytest session
    public void setActiveMutant(String mutantId);         // execution channel
    public void clearActiveMutant(String mutantId);       // execution channel
    public String getActiveMutantOrNull();                // control channel (health poll)
    public void reset();                                  // control channel (circuit breaker only)
}

package io.github.wpunit13.mutator.reset;

public final class SessionResetFacade {
    /**
     * Runs the full canonical reset sequence (ARCHITECTURE.md §3.2) against the given
     * SparkSession, EXCLUDING the final MutantRegistry.clearActiveMutant step (callers invoke
     * that separately via MutantRegistry so the two systems remain independently testable).
     *
     * @param spark the active org.apache.spark.sql.SparkSession, passed across Py4J as the
     *              existing bridged Java object the Python `SparkSession._jsparkSession` already
     *              exposes -- this is the one exception to the "no custom POJOs" rule, since
     *              SparkSession is a Spark-native type Py4J already knows how to bridge.
     * @param sinceTimestampMillis wall-clock millis marking the start of the just-finished test,
     *              used to scope the temp-view diff (checklist item #4) to views created during
     *              that window.
     * @return JSON string: {"clearedCacheEntries": <int>, "unpersistedRddCount": <int>,
     *              "droppedTempViews": [<string>, ...]} -- diagnostic only, not used for control flow.
     * @throws RuntimeException wrapping any underlying Spark exception; caller classifies as ERRORED.
     */
    public static String resetSessionState(Object spark, long sinceTimestampMillis);
}

package io.github.wpunit13.mutator.catalog;

public final class MutationCatalogAccess {
    /**
     * Returns the full Discovery-phase mutant catalog as a JSON array string. Called once,
     * after the baseline phase completes, before the mutation loop begins.
     *
     * Each element shape: {"mutantId": <string>, "filePath": <string>, "lineNumber": <int>,
     *   "operatorType": <string>, "mutationIndex": <int>, "description": <string>,
     *   "coordinateHex": <string>, "mappedTestIds": [<string>, ...]}
     * (mirrors the MutantMetadata schema, §5.1).
     */
    public static String getFullCatalogJson();

    /**
     * Returns the test-impact-mapped subset relevant to a single mutant, as a JSON array of
     * test id strings, matching MutantMetadata.mappedTestIds for that mutant. Provided as a
     * separate narrow accessor so the Python side is not forced to parse the full catalog
     * just to schedule one mutant's test run.
     *
     * @throws IllegalArgumentException if mutantId is not present in the catalog.
     */
    public static String getMappedTestIdsJson(String mutantId);

    /**
     * Replaces the entry's astDiffSnippet (WP-19 plan-diff capture). Called by the
     * Catalyst rule immediately after a rewrite is applied; the snippet is pure
     * observation and never alters the plan, the coordinate space, or the
     * applied-mutation record. In-process paths (JUnit 5 standalone, the PySpark
     * driver) reach the final report through this catalog; externally-orchestrated
     * mutant forks additionally persist the snippet as a diffs/<mutantId>.json
     * sidecar (see DiffSnippetStore) for the aggregating coordinator.
     *
     * @throws IllegalArgumentException if mutantId is not present in the catalog.
     */
    public static void recordAstDiffSnippet(String mutantId, String astDiffSnippet);
}

package io.github.wpunit13.mutator.report;

public final class ReportSink {
    /**
     * Records one mutant's terminal outcome. Called by the Python harness immediately after
     * classifying a mutant (ARCHITECTURE.md §1.2, stage C4-C7), once per mutant, exactly once.
     *
     * @param mutantId must already exist in the catalog (see MutationCatalogAccess).
     * @param status one of "KILLED", "SURVIVED", "TIMED_OUT", "ERRORED" (exact string match,
     *               case-sensitive; any other value throws IllegalArgumentException).
     * @param elapsedMillis wall-clock duration of this mutant's test execution.
     * @param failureDetailOrNull for KILLED/ERRORED: the failing assertion message or exception
     *               string; null for SURVIVED; null or a diagnostic reason string for TIMED_OUT.
     * @throws IllegalArgumentException if mutantId is unknown or status is not a valid enum value.
     * @throws IllegalStateException if this mutantId has already been recorded (results are
     *               write-once; re-recording indicates a harness-level double-execution bug).
     */
    public static void recordOutcome(
        String mutantId,
        String status,
        long elapsedMillis,
        String failureDetailOrNull
    );

    /**
     * Finalizes and writes all configured report artifacts (terminal table, HTML,
     * mutation-report.json, SARIF) to `output_dir`. Called once, after the mutation loop
     * exhausts all mutants.
     *
     * @return the absolute path of the primary mutation-report.json as a String.
     */
    public static String finalizeAndWriteReports();
}
```

### 4.1 Gateway Configuration Requirements

- `auto_convert` / `auto_field` are disabled on the Python-side `Gateway`
  configuration used to reach these classes (`ARCHITECTURE.md` §2.4);
  callers must use exact static-method call syntax, e.g.:
  `spark._jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance().setActiveMutant(mutant_id)`.
- Every method above is safe to call from either the execution-channel thread
  or the control-channel thread (`getActiveMutantOrNull`, `reset` are
  explicitly control-channel-only by convention, not by technical
  enforcement — `MutantRegistry`'s thread-safety model §1.3 makes them safe
  either way, but architecturally they should only ever be invoked from the
  watchdog).

---

## 5. Mutant Metadata & Report Schema

### 5.1 `MutantMetadata` (canonical POJO, `mutator-core`)

```java
package io.github.wpunit13.mutator.model;

public final class MutantMetadata {
    private final String mutantId;          // 16-char lowercase hex, per ARCHITECTURE.md §4.4
    private final String filePath;          // source file of the test/pipeline module, project-relative
    private final int lineNumber;           // best-effort source line of the mutated call site; -1 if unknown
    private final OperatorTypeDto operatorType; // enum mirror of api.OperatorType (mutator-core-owned, no catalyst import)
    private final int mutationIndex;        // 0-based index within the operator's mutation rule list
    private final String description;       // human-readable, e.g. "INNER -> CROSS"
    private final String coordinateHex;      // NodeCoordinate.toHex(), 16-char lowercase hex
    private final String astDiffSnippet;     // short textual before/after plan fragment, report-facing only
    private final List<String> mappedTestIds; // immutable, from Test Impact Analysis

    // All fields set exactly once at construction (constructor-injected); this type is immutable.
    // equals()/hashCode() are defined over mutantId alone (the canonical identity).
}

public enum OperatorTypeDto { JOIN, FILTER, AGGREGATE, WINDOW, PROJECT, OTHER }
```

### 5.2 `MutantResult` (canonical POJO, `mutator-core`)

```java
package io.github.wpunit13.mutator.model;

public final class MutantResult {
    private final String mutantId;
    private final MutantStatus status;
    private final long elapsedMillis;
    private final String failureDetailOrNull;
    private final long recordedAtEpochMillis;

    // Immutable; constructed exactly once by ReportSink.recordOutcome.
}

public enum MutantStatus { KILLED, SURVIVED, TIMED_OUT, ERRORED, SKIPPED }
```

`SKIPPED` is included in the POJO enum (used by the pre-flight cardinality
gate, `ARCHITECTURE.md` §6.3) even though the spec's §4.3 four-state
classification does not enumerate it; `SKIPPED` mutants are explicitly
excluded from the Mutation Score denominator (§5.4) to keep the score formula
exactly as specified.

### 5.3 JSON Schema — `mutation-report.json`

```json
{
  "$schema": "https://spark-mutator.wpunit13.io/schema/mutation-report/v1.json",
  "schemaVersion": 1,
  "generatedAtEpochMillis": 1732000000000,
  "config": {
    "targetModules": ["my_pipeline.transforms"],
    "excludedMutators": ["CrossJoinMutator"],
    "timeoutMultiplier": 2.0,
    "minMutationScore": 80.0
  },
  "summary": {
    "totalMutants": 0,
    "killed": 0,
    "survived": 0,
    "timedOut": 0,
    "errored": 0,
    "skipped": 0,
    "mutationScore": 0.0
  },
  "mutants": [
    {
      "mutantId": "a1b2c3d4e5f60718",
      "filePath": "my_pipeline/transforms/orders.py",
      "lineNumber": 42,
      "operatorType": "JOIN",
      "mutationIndex": 1,
      "description": "INNER -> CROSS",
      "coordinateHex": "9f8e7d6c5b4a3210",
      "astDiffSnippet": "- Join Inner, (a.id = b.id)\n+ Join Cross",
      "mappedTestIds": ["test_orders.py::test_join_matches_customers"],
      "result": {
        "status": "SURVIVED",
        "elapsedMillis": 842,
        "failureDetailOrNull": null,
        "recordedAtEpochMillis": 1732000000123
      }
    }
  ]
}
```

**Field-level contract notes:**

- `astDiffSnippet` is populated by the engine at rewrite time (WP-19): the
  Catalyst rule captures the matched node's before/after plan strings as a
  single-line `"<before> => <after>"` fragment (each side truncated to 2000
  chars, newlines flattened to `" | "`). In-process paths record it through
  `MutationCatalogAccess.recordAstDiffSnippet`; externally-orchestrated mutant
  forks persist it as a sidecar file `<outputDir>/diffs/<mutantId>.json`
  (`{"mutantId": ..., "astDiffSnippet": ...}`, written by `DiffSnippetStore`)
  which the coordinator merges before writing reports. The sidecar is NOT a
  field in this schema; a missing sidecar legally leaves the field `""`.
- `mutants[].result` is nullable **only** transiently during report
  generation if the process crashed mid-run without recording an outcome;
  `finalizeAndWriteReports()` MUST classify any such mutant as `ERRORED`
  with `failureDetailOrNull = "no outcome recorded; run terminated early"`
  rather than emitting a null `result` object, so the schema's `result`
  field is **never actually null** in a written report.
- `summary.mutationScore` is computed strictly as specified in the spec:
  $\left(\frac{\text{killed} + \text{timedOut}}{\text{killed} + \text{timedOut} + \text{survived}}\right) \times 100$,
  rounded to 2 decimal places, with `errored` and `skipped` mutants excluded
  from both numerator and denominator (an `ERRORED` mutant indicates the
  *tool*, not the test suite, failed to reach a verdict, and must not
  silently inflate or deflate the score).
- `mutantId` and `coordinateHex` are always exactly 16 lowercase hex
  characters (`ARCHITECTURE.md` §4.2/§4.4); any other length/casing is a
  contract violation to be caught by report-writer validation, not silently
  accepted.

### 5.4 SARIF Output

The SARIF artifact is derived entirely from the same `mutants[]` array (no
independent data collection): each `SURVIVED` mutant becomes one SARIF
`result` object at `level: "warning"` with `ruleId` set to the mutator name
(e.g. `"JoinTypeRelaxationMutator"`, derived from `operatorType` +
`mutationIndex` via a fixed lookup table in `mutator-core`), `message.text`
set to `description`, and `locations[0].physicalLocation` set from
`filePath`/`lineNumber`. `KILLED`/`TIMED_OUT` mutants are omitted from SARIF
entirely (SARIF is a CI-gating surfaced-problems format; a killed mutant is
not a problem to surface). `ERRORED` mutants are emitted at `level: "error"`
so tooling can distinguish "the suite has a gap" from "the tool itself
failed" in CI annotations.

Known limitation: until Test Impact Analysis (§1.2) wires the file-path
hint, `filePath` is the sentinel `"unknown"` and `lineNumber` is `-1`, so
SARIF results carry `uri: "unknown"` and omit `region`. The hint is a direct
input to `computeMutantId`, so wiring it per test class would mint multiple
mutantIds for one logical mutation; consumers should therefore key on
`ruleId`/`level` until lineage capture makes the URI meaningful.
