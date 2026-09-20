# Catalyst, from first principles — and how spark-mutator bends it

> **Audience.** New engineers joining this project, and anyone who wants to
> understand *why* plan-level mutation testing works at all. This document
> teaches the Apache Spark Catalyst optimizer from zero, then maps every
> relevant Catalyst behavior onto the exact mechanisms this library uses.
> Everything marked **observed** was verified empirically in this repository
> (diagnostic runs, report artifacts) or against the Spark 3.5.3 source;
> everything marked **untested** is flagged honestly.
>
> Companion reading: [`ARCHITECTURE.md`](ARCHITECTURE.md) (system overview),
> [`developer-guide.md`](developer-guide.md) §1–§7 (operational surface),
> [`CONTRACTS.md`](CONTRACTS.md) (frozen schemas),
> `prompts_execution/RCA-2026-09-15-inprocess-ERRORED-mutants.md` (the case
> studies in §12 reference it).

---

## 1. Catalyst in one page

Catalyst is Spark SQL's query compiler. Every DataFrame operation you write
never touches data — it grows a **tree**. Data is only touched when an
**action** (`collect`, `count`, `write`, …) forces Spark to walk that tree
through a compiler pipeline and execute the result.

The pipeline has four phases:

```
parsed  →  analyzed  →  optimized  →  physical  →  executed
(unresolved)  (references     (rule batches)   (SparkPlan)   (RDDs/jobs)
               resolved)
```

- **Parsed**: a string becomes an unresolved logical plan (`UnresolvedRelation`,
  `UnresolvedAttribute`).
- **Analyzed**: the analyzer's rule batches resolve every name against the
  catalog, assign every attribute a unique **exprId**, and run a final
  `CheckAnalysis` pass that rejects ill-typed plans.
- **Optimized**: rule batches rewrite the plan — predicate push-down, constant
  folding, column pruning, projection collapsing, join reordering.
- **Physical**: the logical plan becomes `SparkPlan` nodes (joins pick
  broadcast vs sort-merge, etc.), which compile to RDDs.

Two facts make this pipeline *hard* to instrument, and both are the reason
this library exists in its current shape:

1. **The plan is a moving target.** Between any two phases, nodes are cloned,
   rebuilt, relocated, and their expressions rewritten. Nothing about a node
   is stable except what it *means*.
2. **Analysis is eager, optimization is lazy.** Every `Dataset` you create
   analyzes immediately; optimization happens only when an action runs. A
   pipeline of five DataFrame steps produces five analyses but (usually) one
   optimization.

spark-mutator's entire job is: *grow a mutation into that tree, prove it
executed, and find out whether your tests noticed* — without breaking any of
the invariants above.

---

## 2. The tree model: everything is a `TreeNode`

Catalyst is built on one abstraction: `TreeNode`. Both plan nodes
(`LogicalPlan` subtypes: `Filter`, `Join`, `Project`, `Aggregate`, `Window`,
`Range`, `LocalRelation`, …) and expressions (`GreaterThan`, `And`, `Alias`,
`Literal`, `AttributeReference`, …) extend it. A plan node is a case class
whose fields are its children (other plan nodes) and its expressions.

What `TreeNode` gives every node:

| Facility | Signature flavor | spark-mutator usage |
|---|---|---|
| Children | `children: Seq[PlanType]` | pre-order plan walks (discovery, matching) |
| Transformation | `transformDown` / `transformUp` | splicing exactly one rewritten node back into the plan |
| Tags | `getTagValue` / `setTagValue` on a `TreeNodeTag` | `AlreadyMutatedTag` — marks a node already rewritten |
| Copy | `makeCopy` / case-class `copy` | the shims build mutated nodes via `copy(...)` |
| Rendering | `toString` / `simpleString(maxFields)` | `astDiffSnippet` before/after fragments |

**Observed gotcha #1 — tags die in clones.** `QueryExecution` clones the
analyzed plan before optimization. Tags are per-instance metadata; the clone
creates new instances and tags do not ride along. This single fact is why the
PostHoc→Optimizer handoff (§7) is a JVM-global record and not a node tag.

**Observed gotcha #2 — exprIds are JVM-global and monotonic.** Every resolved
attribute carries an `exprId` from a static counter that only ever increases.
Re-analyzing the *same* code in the same JVM yields *different* exprIds. No
stable signature may ever contain a raw exprId — this is why the shim's
`substituteExprIds` replaces every attribute with its **output-position
ordinal** (`#0`, `#1`, …), keeping signatures byte-identical across
independent analyses, builds, and Spark versions.

---

## 3. The life of a query — and when each phase actually runs

The single most useful mental model for working on this codebase:

| You write | Catalyst does | When |
|---|---|---|
| `spark.read.parquet(...)` / `createDataFrame(...)` | builds a `LogicalRelation` / `LocalRelation` | immediately |
| `.join(...)`, `.filter(...)`, `.select(...)`, `.withColumn(...)` | builds a child-extended plan and **analyzes it eagerly** (`Dataset` construction calls `assertAnalyzed`) | immediately, per step |
| `.cache()` | registers the plan with the `CacheManager` — **nothing executes** | immediately (registration only) |
| `.collect()` / `.count()` / `.write()` / first use of a cached DF | **optimizes** the plan (all batches, including injected ones), plans physically, executes | at the action |

**Observed consequence — multi-shape discovery.** Because every `Dataset`
step analyzes eagerly, a five-step pipeline produces five *analyzed plans*,
each a superset of the previous. Discovery (§6) registers candidates on every
analyzed plan while the registry is idle. The same logical `Filter` node,
appearing at different depths in successive shapes, yields **different
positional coordinates → different mutant ids**. One filter in a five-step
pipeline is not one mutation site; it is one site *per shape it appears in*.
This is not a bug — it is the coordinate space working as designed (§6.2) —
but it means mutant counts grow with pipeline length, and it is why the
complex-plan stress fixture (`ComplexPlanStressTest`) produces ~90 mutants
from ~10 operators.

**Observed consequence — the cache boundary.** `cache()` materializes lazily
at the first action over the cached data. At that moment the `CacheManager`
optimizes the plan-to-cache (running *all* injected rules, including ours) and
stores it as an `InMemoryRelation`. From then on, every outer plan that
touches the cached data has the subtree **replaced** by the
`InMemoryRelation` leaf during optimization (`withCachedData` substitution).
Nodes inside a cached plan are invisible to any rule walking outer plans —
they were last seen at materialization time. This is why mutants catalogued
at outer shapes whose nodes ended up inside a cache are classified
not-applied (ERRORED) by the honesty guard: the mutation genuinely never
executes.

**Untested → tested (see §13):** Adaptive Query Execution re-optimizes plans
at runtime as shuffle stages materialize. Two structural facts, verified
against the Spark 3.5.3 source and now pinned by a reactor test
(`ComplexPlanStressTest.aqeClassificationParityWithDefaultSession`):

1. **Injected optimizer rules do not re-fire during AQE re-optimization.**
   `AdaptiveSparkPlanExec` re-optimizes via `AQEOptimizer`, whose rule set is
   its own built-in batches plus a *separate* extension point
   (`injectRuntimeOptimizerRule`). A rule injected with
   `injectOptimizerRule` — ours — runs only in the initial logical
   optimization, so AQE cannot double-apply a mutation by construction.
2. **The mutated node survives re-optimization.** AQE re-optimizes the
   *current logical plan*, which carries the mutated node; its built-in rules
   (Propagate Empty Relations, Dynamic Join Selection, Eliminate Limits,
   Optimize One Row Plan) re-plan around the mutation without undoing it.

The parity fixture additionally injects the Optimizer-phase rule through
`injectRuntimeOptimizerRule` — forcing the rule to re-fire during every AQE
re-optimization round on rebuilt plans — and asserts the AQE-on and AQE-off
runs produce **identical `mutantId → status` maps**. One-shot semantics
(consumed pending) are what make that parity hold; a regression that
double-applied would flip statuses and fail the build.

---

## 4. The rule engine: batches, strategies, idempotency

Each phase is a list of **batches**; each batch holds `Rule[LogicalPlan]`
instances and a strategy:

- `Once` — run at most once (used by the analyzer's final batches).
- `fixedPoint(maxIterations)` — run until the plan stops changing (used by the
  operator-optimization batches; the optimizer may iterate many times).

Two properties of this engine shape our design:

1. **A rule may be invoked more than once per query** (multiple batches,
   re-analysis, re-planning). Any mutating rule must therefore be
   **idempotent per invocation** — ours tags the rewritten node with
   `AlreadyMutatedTag` and consumes a one-shot pending record, so a second
   visit can never double-apply.
2. **Rules see the plan mid-compiler.** An optimizer rule runs *between*
   built-in rewrites: the node it visits may have been rebuilt by
   `PushDownPredicates` one batch earlier and be rebuilt again by
   `CollapseProject` one batch later. Nothing observed in a rule invocation is
   guaranteed to still be there in the next one.

---

## 5. The extension surface — and the two hooks spark-mutator uses

`SparkSessionExtensions` (wired via `spark.sql.extensions=<class name>`) lets
a jar inject behavior into every phase. The inventory:

| Injection point | Runs | spark-mutator uses it |
|---|---|---|
| `injectParser` / `injectFunction` / `injectTableFunction` | parsing, catalog | no |
| `injectResolutionRule` | analyzer resolution batches | no |
| **`injectPostHocResolutionRule`** | analyzer **"Post-Hoc Resolution"** batch, `Once` — exactly one invocation per analysis, on the fully-resolved plan | **yes — `CatalystMutationRule(PostHoc)`** |
| `injectCheckRule` | `Finish Analysis` checks | no |
| **`injectOptimizerRule`** | operator-optimization batches — after analysis checks, before physical planning | **yes — `CatalystMutationRule(Optimizer)`** |
| `injectPreCBORule` / `injectColumnar` | pre-CBO, columnar | no |

Why these two, and why the same rule class twice:

- **Discovery must see fully-resolved plans.** Coordinates are computed from
  resolved attributes and expressions; unresolved plans are useless. The
  Post-Hoc batch is the first point where the plan is complete — and `Once`
  means exactly one deterministic invocation per analysis.
- **Mutation must run where schema-breaking rewrites are repairable.** An
  `INNER → ANTI` join rewrite breaks the plan's schema; the analyzer's
  `Finish Analysis` check would reject it. Injecting into the optimizer means
  the rewrite lands *before physical planning* and the optimizer's own
  subsequent rules repair the plan (e.g. ColumnPruning drops the USING-join
  dedup Project's dead right-side columns before an ANTI join drops them).
- **One class, two instances, branching on registry state.** `CatalystMutationRule`
  takes a `Phase` parameter. With `MutantRegistry` idle it *discovers*; with a
  mutant active it *matches* (PostHoc) or *rewrites* (Optimizer). The registry
  is the switch; the rule is stateless per invocation.

The wiring is one line of user config — this is the entire "zero-touch"
contract:

```
spark.sql.extensions = io.github.wpunit13.mutator.MutatorSparkExtension
```

---

## 6. Discovery: reading plans without touching them

### 6.1 The walk

With the registry idle, the PostHoc rule performs a pre-order walk
(`walk(node, depth, childOrdinal)`) and asks the **shim** to classify each
node:

```scala
shim.classify(node, depth, childOrdinal)
  : Option[(OperatorType, Seq[MutationCandidate])]
```

The shim is the *only* component that knows Spark-version-specific plan
shapes. It returns, per supported operator, the operator type and every
applicable mutation:

| Node | Candidates (index → description) |
|---|---|
| `Join` (Inner) | 0 → `INNER → LEFT`, 1 → `INNER → CROSS`, 2 → `INNER → ANTI` |
| `Filter` (top-level And) | 0 → keep left conjunct, 1 → keep right conjunct, 2 → `FALSE`, 3 → `¬P` |
| `Filter` (single predicate) | 2 → `FALSE`, 3 → `¬P` |
| `Aggregate` | 0 → swap function, 1 → zero aggregate |
| `Window` (orderSpec non-empty) | truncate frame, invert order |
| `Project` | 1 → `INJECT_NULL` (plus a coalesce-bypass candidate when a `Coalesce` is present) |

Each candidate carries a **coordinate**:

```
canonical = "depth|OPERATOR_TAG|childOrdinal|exprSig"
mutantId  = DeterministicHasher.hashToHex(canonical, filePathHint, operatorTag, mutationIndex)
```

`exprSig` renders the node's expressions with every attribute reference
replaced by its **output-position ordinal** — never its exprId (§2, gotcha #2).
This is what makes ids *deterministic*: the same pipeline, analyzed in any
JVM, on any Spark/Scala combination in the supported matrix, produces the same
ids. The id space is **frozen** (`CONTRACTS.md`); cross-version goldens pin it.

### 6.2 The registry: IDLE vs ACTIVE

`MutantRegistry` is a per-JVM singleton with two states:

- **IDLE** → the PostHoc rule *discovers* (registers candidates, changes
  nothing in the plan).
- **ACTIVE** (one mutant id set by the orchestrator) → the PostHoc rule
  *matches* the active mutant's node and records a pending rewrite; the
  Optimizer rule *applies* it.

Who flips the state, and how the active mutant crosses process boundaries, is
the **fork-boundary protocol** (`developer-guide.md` §2): `MutantRegistry`,
the catalog, and the report sink are per-JVM singletons, so the Maven plugin's
orchestrator and its Surefire forks communicate only through files
(`catalog.json`, `outcomes/<id>.json`, applied markers) and system properties
(`spark.mutator.active.mutant`, `spark.mutator.phase`). The in-process path
(JUnit 5 extension) skips the file protocol entirely — same JVM, same
singletons.

---

## 7. Mutation: the two-phase loop, and why a handoff exists at all

Applying a mutant is a *two-phase* operation because of one brutal property of
`QueryExecution`:

> **The analyzed plan is cloned before optimization.** Node tags set during
> analysis do not reach the optimizer; positions recorded during analysis do
> not survive predicate push-down; expression trees recorded during analysis
> are rewritten by folding and alias removal.

So the rule cannot simply "remember which node to mutate" — the node it saw
will not exist by optimization time. The design:

```
PostHoc (analysis)                     Optimizer (execution planning)
──────────────────                     ──────────────────────────────
walk analyzed plan                     walk optimized plan
find node matching the catalog         primary: node whose SHAPE-FREE key
coordinate (positional)                   matches the pending key
record pending:                        fallback: class + referenced-column
  shapeFreeKey (shape-free key)           overlap + no inserted guards +
  nodeClass, referencedColumns,           offers the mutation index
  exprClasses                             ↓
                                       one rewrite via shim.mutate*
                                       tag AlreadyMutatedTag
                                       record AppliedMutantTracker
                                       consume pending
                                       splice: transformDown { n eq node ⇒ rewritten }
```

The **shape-free key** re-classifies the matched node *as if it were the plan
root* (`classify(node, 0, -1)`) — position-invariant across the clone and
across push-down relocation. It is the internal handoff key only; the public
mutant ids remain the frozen positional coordinates.

### 7.1 Why the fallback exists (the drift catalog)

The shape-free key hashes the node's **expression tree** (with exprIds
substituted by ordinals). The optimizer rewrites those trees. Every mechanism
below was **observed** in this repository's diagnostic runs:

| Mechanism | Optimizer rules | Observed before → after |
|---|---|---|
| Cast folding | `SimplifyCasts`, `ConstantFolding` | `Filter (id#37L > cast(2 as bigint))` → `Filter (id#35L > 2)` |
| Alias collapse | `CollapseProject`, `RemoveNoopOperators` | `Project [id#35L AS id#37L]` → eliminated / merged |
| Column pruning | `ColumnPruning` | join output 5 cols → 4; `Project` under `count(1)` → `Project(Nil)` |
| Predicate push-down | `PushPredicateThroughJoin` | And-filter split into per-conjunct filters **plus inserted `IsNotNull` guards** |
| Cache substitution | `CacheManager.useCachedData` | subtree → `InMemoryRelation` leaf (inner nodes invisible) |

Any of these changes the rendered signature → different hash → the pending key
matches nothing → the rewrite never applies. The **identity fallback**
re-identifies the matched node by what *survives* replanning:

1. not tagged `AlreadyMutatedTag`,
2. schema non-empty (excludes the `Project(Nil)` stubs ColumnPruning inserts
   under `count(1)`-style aggregates — rewriting those would be a no-op
   masquerading as an applied mutation),
3. same simple class name,
4. the recorded node's referenced columns **overlap** the candidate's schema
   (width evolves in both directions — push-down widens a Filter below a
   Project, pruning narrows joins — only the overlap is invariant; vacuously
   true for expression-less nodes such as USING joins, whose condition is
   `None`),
5. **not an inserted null-guard**: a candidate whose every top-level
   expression is an `IsNotNull` (when the recorded node contained none) is an
   optimizer artifact — mutating it under-applies the mutant and can fake a
   SURVIVED,
6. the shim **offers the recorded mutation index** for the candidate's shape
   (keep-left/keep-right require a top-level And; window mutations require an
   order spec) — prevents a `ShimMutationException` mid-rewrite.

Guards 4–6 were not designed on paper; they were **forced out by the
complex-plan stress fixture** (`ComplexPlanStressTest`), which exposed three
real defects in the first fallback version (a shim crash, a fake-SURVIVED via
guard mutation, and a schema-direction whack-a-mole). See the RCA appendix B
for the full iteration log.

### 7.2 What the fallback cannot do (stated plainly)

- **Ambiguity is refused, not guessed.** Two same-class nodes with identical
  shapes are indistinguishable to both the primary key and the fallback. When
  the fallback's identity criteria match MORE THAN ONE node, the rewrite is
  refused (WARN-logged) and the honesty guard classifies the run as
  not-applied — first-match-wins would make the verdict depend on plan order,
  which shifts with replan timing under CPU contention (measured: the same
  mutant flipped SURVIVED/KILLED between sequential and concurrent fork runs
  before this refusal existed). A single-candidate fallback still applies and
  is WARN-logged with an auditable `astDiffSnippet`.
- **Designed not-applied.** Nodes eliminated before execution (pruned stubs,
  cache-hidden subtrees) are honestly ERRORED by the guard. On multi-shape
  plans this is expected and bounded — but invisible to the score formula
  (see §10.2 and WP-24).
- **AQE** is exercised by the parity fixture (`ComplexPlanStressTest
  .aqeClassificationParityWithDefaultSession`): AQE forced on, broadcast joins
  disabled (sort-merge → runtime BHJ conversion), and the rule re-injected
  into AQE's runtime re-optimizer — classification must be identical to the
  non-AQE run (§13).

---

## 8. The honesty contract

Plan-level mutation testing has one catastrophic failure mode: **a fake
SURVIVED** — reporting "your tests can't catch this bug" when in fact the bug
was never injected. The library's answer is a chain of proofs:

| Proof | Mechanism | Fails as |
|---|---|---|
| The mutant was *activated* | `MutantRegistry.setActiveMutant` before the re-run | — |
| The rewrite *executed* | `AppliedMutantTracker.record` — set only after a real `shim.mutate*` call, never on a no-match | not-applied → **ERRORED** |
| The mutation *ran inside the query* | in-process: the tracker is read after the test; fork: an applied marker file the coordinator requires before KILLED/SURVIVED | missing marker → ERRORED |
| The suite *noticed* | AssertionError → KILLED; unhandled exception → ERRORED; nothing → SURVIVED | — |

Two corollaries every engineer must internalize:

1. **A harness that swallows the unknown-mutant error turns a broken handoff
   into "every mutant survived"** — the single worst failure mode. Fail
   loudly, always (`developer-guide.md` §7).
2. **The score formula excludes ERRORED and NOT_APPLIED** —
   `(killed + timedOut) / (killed + timedOut + survived)`. A run where 22 of
   24 mutants errored printed `mutationScore: 100.0` (§12). WP-24 now splits
   the two ERRORED populations (designed not-applied vs real failure) and
   gates them separately (`spark.mutator.maxNotAppliedRatio`,
   `spark.mutator.maxErroredCount`) — but the score itself still cannot see
   either population.

---

## 9. State isolation between mutants

A mutant re-run must observe a pristine engine, or outcomes leak between
mutants. The two orchestration paths isolate differently:

| | Fork path (Maven plugin) | In-process path (JUnit 5 / Gradle) |
|---|---|---|
| Isolation unit | a fresh JVM per mutant (Surefire fork) | a fresh **SparkSession** per re-run |
| What resets | everything | Catalyst caches, session state (`SessionResetFacade`), and — critically — the session itself |
| Session lifecycle | fork-local | the extension re-runs the suite's full class lifecycle per mutant (`@BeforeAll` → tests → `@AfterAll`), so a `spark.stop()` in user `@AfterAll` is safe: the next re-run's `getOrCreate()` sees the stopped context and builds fresh |

The last row is a hard-won lesson: the in-process loop originally re-ran only
`@BeforeEach`/`@Test`/`@AfterEach`, so every mutant after the first executed
against the *stopped* session from the outer run's `@AfterAll` — 22 of 24
mutants ERRORED (§12, case A). The fix mirrors the fork's semantics: full
lifecycle, fresh session, per mutant.

Between mutants, `SessionResetFacade.resetSessionState` runs the canonical
reset (clear cache manager, unpersist persisted RDDs, invalidate the session
catalog, drop temp views) — with every Spark interaction behind reflection,
because `mutator-core` must never import Spark.

---

## 10. Case studies (teaching material)

### Case A — the 100% score that was 92% broken

The example pipelines' in-process runs reported `killed: 2, errored: 22,
mutationScore: 100.0`. Three independent blind spots aligned:

1. the examples are not reactor modules, so `mvn clean test` never ran them;
2. the JVM e2e (`verify_e2e_jvm.py`) drives the fork path only — fresh JVMs
   cannot hit the dead-session bug;
3. the score formula excludes ERRORED, so a 92%-broken run scored a perfect
   100.

The unit fixtures that *did* run in every session were structurally blind:
they used `getActiveSession()` inside test bodies, held no static session, had
no `@BeforeAll`/`@AfterAll`, and were launched from inside an outer `@Test` —
the loop always ran before any teardown existed.

Lesson: **a green build is not a verification claim.** The report artifact was
the only witness, and nothing asserted on it.

### Case B — the empty-plan bypass

Pre-fix, exactly 2 of 24 mutants were KILLED — both `FILTER → FALSE`. The
session was dead; how did they execute? They didn't: `FILTER → FALSE` prunes
the plan to an empty `LocalRelation`, and Spark 3.5's `LocalTableScanExec`
overrides `executeCollect()` to return the in-memory rows directly — for an
empty plan it never touches the SparkContext. The assertion failed on 0 rows
→ KILLED, honestly, on a dead session.

Lesson: **execution paths have short-circuits.** "The query ran" and "the
context is alive" are not the same statement — and a mutation that prunes the
plan can silently change which Spark code paths are exercised.

### Case C — the drift that only complex plans reveal

The shape-free key's invariance claim held for every plan the e2e verified —
and failed for `range(0,5).toDF("id").filter("id > 2")` (a cast folds between
phases), for aliased projections (collapsed), and for pruned joins (schema
shrank). The lesson generalizes: **an invariant verified on simple plans is a
hypothesis, not a fact.** The complex-plan stress fixture exists to convert
that hypothesis into a regression-tested fact (§9, B.1).

---

## 11. Rules of thumb for pipeline authors

These follow directly from the mechanics above and materially change mutation
scores:

1. **Assert exact rows, never counts.** `count()` also plans an extra
   whole-stage `Aggregate` node that shifts every coordinate below it and
   destabilizes ids; `collectAsList()` (not `count()`) keeps the plan honest
   and the assertion strong.
2. **Expect one mutant per (node, shape).** Longer pipelines → more analyzed
   shapes → more mutants. This is correct, not noise.
3. **Casts, aliases, and prunable columns are drift sources.** The fallback
   handles them, but plans whose expressions survive optimization match by the
   precise primary key — prefer explicit, stable predicates in mutation
   targets.
4. **A cached subtree is frozen at materialization.** Mutations catalogued
   inside a cache are applied when the cache materializes; outer-shape
   mutants of the same node will report not-applied.
5. **Stopping the session in `@AfterAll` is safe** under
   `@EnableSparkMutationTesting` — the loop re-runs the full lifecycle per
   mutant — but expect a session rebuild per mutant (~0.1–0.5 s warm).
6. **ERRORED in a report is a signal, not noise.** Read `failureDetailOrNull`;
   "not applied" means the mutation never executed.

---

## 12. Shims: why Catalyst forces a version matrix

Catalyst has no semver. `LogicalPlan` subtypes, optimizer rules, and physical
operators change freely between minor versions, and the Scala 2.12/2.13
binary boundary is not linkable. The library therefore compiles one **shim
module per Spark×Scala combination** (`interceptor-spark-3.5_2.12`,
`interceptor-spark-3.5_2.13`, `interceptor-spark-4.2_2.13`), each implementing
the same `PlanMutatorShim` trait (`classify` + `mutateJoin/Filter/Aggregate/
Window/Project`) and sharing version-agnostic logic in
`Spark35ShimBase`. The rule, the registry, the catalog, and the reports are
version-agnostic; only the shim touches version-specific plan shapes.

The frozen invariants that make cross-version mutation meaningful:

- the coordinate formula (`NodeCoordinateFactory`),
- the mutant-id hash (`DeterministicHasher`),
- the operator tags and mutation indexes,
- the report schemas (`CONTRACTS.md` §5.3).

Cross-version goldens pin these: a coordinate computed under 3.5 addresses the
same logical mutation under 4.2.

---

## 13. Glossary

| Term | Meaning |
|---|---|
| **exprId** | JVM-global monotonic id assigned to every resolved attribute. Never stable across analyses. |
| **shape-free key** | A node re-classified as plan root (`depth 0, ordinal −1`) — the internal PostHoc→Optimizer handoff key. |
| **positional coordinate** | `depth \| OP \| childOrdinal \| exprSig` — the frozen public mutant-id space. |
| **shape (of a plan)** | One analyzed plan tree produced by one `Dataset` step. Nodes recur across shapes at different positions. |
| **drift** | Any optimizer rewrite that changes a node's expression tree, schema, or position between the PostHoc match and the Optimizer rewrite. |
| **honesty guard** | The rule that a mutant whose rewrite never executed is ERRORED — never KILLED, never SURVIVED. |
| **InMemoryRelation** | The logical leaf that replaces a cached subtree during optimization. Its inner plan is invisible to outer-plan rules. |
| **applied marker** | Fork-path file proving a mutation executed; required before KILLED/SURVIVED classification. |
| **twin nodes** | Same-class, same-shape nodes that no structural key can distinguish; resolved first-wins. |
