# spark-mutator Developer Guide

This is the authoritative guide for building against, configuring, and operating
the JVM side of `spark-mutator`. It documents the two orchestration modes, the
fork-boundary protocol, the configuration/system-property surface, and the
quality gate. Read [`core_idea.md`](core_idea.md) for the vision,
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the mechanics,
[`CONTRACTS.md`](CONTRACTS.md) for the frozen API surface, and
[`RELEASING.md`](RELEASING.md) for the release runbook.

---

## 1. Two orchestration modes

The JVM side has **two** ways to run mutation testing. They share the same
engine (`mutator-core` + `catalyst-interceptor`) but differ in *who drives the
loop* and *where it runs*.

| | **Maven plugin** (external) | **JUnit 5 extension** (in-process) |
|---|---|---|
| Invocation | `mvn spark-mutation-testing:mutate` | `mvn test` (or the IDE) |
| Orchestrator | `MutateMojo` + `MutationLoopCoordinator` (in the Maven JVM) | `SparkMutatorExtension` (in the test JVM) |
| Test runner | Surefire forks one JVM per mutant | JUnit 5 runs tests reflectively in-process |
| Isolation | Process-level (fresh JVM per mutant) | In-process, cache-reset between mutants |
| Use case | CI / enterprise gate; **sequential today** — parallel fork execution is planned (WP-28) | IDE, quick feedback, zero-plugin `mvn test` |

### 1.1 Maven plugin path

The Mojo (`maven-plugin/src/main/java/io/github/wpunit13/mutator/maven/MutateMojo.java`)
does four things on `mvn spark-mutation-testing:mutate`:

1. **Detect** the Spark/Scala version from the project's **test** classpath
   (`SparkVersionDetector`).
2. **Resolve** the matching `interceptor-spark-<major.minor>_<scala>` bundle via
   Maven Resolver/Aether.
3. **Configure** Surefire: append
   `-Dspark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension` to
   `argLine` and add the interceptor JAR to `additionalClasspathElements`
   (`SurefireConfigurator`). On modular runtimes (Java 9+) the configurator
   also merges Spark's mandatory JVM opens (the `--add-opens` set plus the
   Netty/reflect flags, verbatim from the root POM's `spark.test.jvm.args`
   minus `-Xmx2g`) into `argLine` — without them the driver dies with
   `InaccessibleObjectException` before any test runs. Disable with
   `-Dspark.mutator.injectAddOpens=false` (e.g. fork JDK managed via
   toolchains).
4. **Run the mutation loop** (`MutationLoopCoordinator`): baseline → discover →
   fork-per-mutant → classify → merge → report → gate.

> **At least one annotated class must run in each fork** — unless the engine-side
> handoff is active, which is the default since WP-26: `MutatorSparkExtension`
> (injected into `argLine` by the plugin itself) performs all three §7
> obligations, so a pom-only setup needs **no annotation at all**. The
> `SparkMutatorExtension` bridge (via `@EnableSparkMutationTesting`, a shared
> base class, or equivalent registration) remains as an idempotent co-writer
> when present. If neither runs — e.g. no Spark session is ever created in the
> fork — the baseline fork hands back an empty catalog and the Mojo
> writes a **silent empty report** (zero mutants, score 0.0%, exit 0) — check
> the `Detected …` / mutant-count log lines to confirm discovery actually ran.

### 1.2 JUnit 5 in-process path

Annotate a test class with `@EnableSparkMutationTesting` (or register
`SparkMutatorExtension` directly). The extension:

1. Ensures `MutatorSparkExtension` is active on the `SparkSession`.
2. Runs the baseline (unmutated) pass, discovering candidates.
3. On `afterAll`, runs the mutation loop **in-process** via reflection that
   re-runs the suite's full class lifecycle per mutant — `@BeforeAll`, the
   `@Test` methods (filtered by test-impact mapping), `@AfterAll` — mirroring
   the fork-per-mutant path's fresh JVM (a `spark.stop()` in `@AfterAll` is
   safe: the next re-run's `@BeforeAll` rebuilds the session), resetting
   Catalyst caches between mutants.
4. Writes the report and (optionally) applies the score gate.

This path needs no Maven plugin declaration; it works anywhere JUnit 5 runs —
including Gradle's `test` task (thin path, no plugin):
[`GRADLE.md`](GRADLE.md).

> **One annotated class drives the loop.** In standalone mode the annotated
> class's `afterAll` runs the full mutation loop and writes the report. The
> tested configuration is exactly one such class per run (select it with
> Surefire `<includes>` or a profile, as the examples do). Multiple annotated
> classes in one run each trigger their own loop and re-write the report —
> untested; the Maven plugin path is the mode designed for many classes.

> **JVM opens on the in-process path.** Unlike the Maven plugin path (§1.1),
> the extension cannot add JVM arguments after the JVM has started. When you
> run in-process on Java 17+, your Surefire/Gradle config must carry Spark's
> mandatory `--add-opens` set (the canonical list is the root POM's
> `spark.test.jvm.args`, reproduced in [`GRADLE.md`](GRADLE.md) §3) — without
> it the driver dies with `InaccessibleObjectException` before any test runs.

### 1.3 Runtime sizing (plan before you run)

Wall-clock for the fork loop decomposes into three additive parts:

```
T ≈ T_baseline
  + M × S_fork                                  // per-mutant fork startup
  + Σ per-mutant test execution                 // see mix below
```

- `M` = catalogued mutants, `S_fork` = JVM + SparkSession startup per fork
  (measure once on your box; 30–60 s is typical for `local[1]`).
- Per-mutant deadline = `ceil(T_baseline × timeoutMultiplier)`; a `TIMED_OUT`
  mutant costs the *full* deadline, a `SURVIVED` one costs its mapped tests'
  runtime, a `KILLED` one usually dies fast (fail-fast).

**Worked example.** Suite baseline 4 min, 40 mutants, multiplier 2.0
(deadline 8 min), fork startup 45 s, mix 60% killed / 30% survived / 10%
timed-out:

```
T_baseline                    4 min
fork startup   40 × 45s      ≈ 30 min
killed   24 × ~1 min          ≈ 18 min
survived 12 × ~2.5 min        ≈ 30 min
timed out  4 × 8 min          ≈ 32 min
                              ─────────
realistic                     ≈ 1 h 50 min
hard upper bound (every mutant hits its deadline): 4 + 40×8 ≈ 5.4 h
```

Planning rules of thumb:

1. **Pilot first.** Run the loop once on a small module or with
   `-Dspark.mutator.excludedMutators=WINDOW,OTHER` to measure `S_fork` and the
   kill/survive mix before budgeting a full run.
2. **The in-process path has no fork startup** (`mvn test` reuses the
   session) — for local iteration on large catalogs it is the fast loop; the
   fork loop is the CI-grade one. Per-mutant deadlines are enforced here too
   since WP-25: a hung mutant is classified `TIMED_OUT` and the loop
   continues (§6.2).
3. **Shrink the catalog, not the deadline.** `excludedMutators` removes whole
   families; lowering `timeoutMultiplier` below 2.0 risks flaky `TIMED_OUT`
   verdicts on legitimately slow mutants.
4. Parallel fork execution is the designed answer to large catalogs —
   planned as WP-28 (sequential today).

### 1.4 Flaky baselines

Contract: **any baseline failure aborts the entire run.** This is deliberate —
a red baseline makes every subsequent classification meaningless (the score
would measure the baseline bug, not the suite). There is no retry.

Operational guidance when it bites:

1. **Treat the flake as the finding.** A test that intermittently fails also
   intermittently *passes* — under mutation, that same test flips verdicts
   between runs and destroys the deterministic-id reproducibility the report
   depends on. Fix it or quarantine it before mutation runs, not after.
2. **Quarantine via Surefire** — `<excludes>` or a profile, the same
   mechanism the weak/hardened example profiles use. Quarantined tests simply
   don't run; mutants they would have killed are honestly `SURVIVED` in the
   report.
3. **Nondeterministic assertions are flakes in waiting** — sort collections
   before comparing, assert floating point with tolerance, never assert on
   wall-clock or row *order* without an explicit `orderBy`.

Retry-on-flake is a deliberate non-feature: a mutant whose mapped tests
sometimes fail and sometimes pass is not a verdict, it is a test bug — a
retry would launder it into whichever outcome the gate prefers.

---

## 2. The fork boundary and file-based IPC

This is the single most important architectural fact: **`MutantRegistry`,
`InMemoryMutationCatalog`, and `ReportSink` are per-JVM singletons.** In the
Maven path, the Mojo runs in the Maven JVM while Surefire forks a *separate*
test JVM, so these singletons cannot be shared across the boundary.

State crosses the boundary **only via files** under
`${project.build.directory}/spark-mutator-reports/`:

```
spark-mutator-reports/
├── catalog.json                 # baseline fork WRITES, Mojo READS
├── outcomes/
│   └── <mutantId>.json          # Mojo WRITES one per mutant, then READS back
├── mutation-report.json         # Mojo WRITES (final merge)
├── mutation-report.sarif
└── mutation-report.html
```

| State | Direction | Mechanism |
|---|---|---|
| Discovery (`catalog.json`) | fork → orchestrator | baseline fork's bridge serializes `MutationCatalogAccess.allEntries()` via `MutationCatalogIo` |
| Active mutant | orchestrator → fork | system property, set on the fork's launch |
| Outcomes (`outcomes/<id>.json`) | orchestrator → disk → orchestrator | `OutcomeFileStore`; one disjoint file per mutant (parallel-safe) |
| Final report | orchestrator | `ReportWriter.writeReports(catalog, results)` |

Why files and not sockets/shared JVMs: Surefire forks are the mechanism the
Maven plugin already uses, and file-based exchange is deterministic, debuggable,
and trivially parallelizable (each fork owns a disjoint write path; a later
merge pass produces the single authoritative report).

### The two relevant classes

- `mutator-core/.../catalog/MutationCatalogIo.java` — `catalog.json` round-trip
  (full `MutantMetadata`, including `mappedTestIds` and `astDiffSnippet`).
- `mutator-core/.../report/OutcomeFileStore.java` — `outcomes/<id>.json`
  write/read.

Frameworks other than JUnit 5 implement the same handoff inside their own test
lifecycle — the three mandatory obligations are specified in §7 (Harness
integration contract).

---

## 3. System property contract

Two distinct namespaces must not be conflated.

### 3.1 Orchestrator → fork directives (internal)

Set by `MutationLoopCoordinator` and read by `MutantBootstrap`
(`mutator-core/.../MutantBootstrap.java`) and the JUnit 5 bridge.

**End users set nothing here.** The first two rows are pure plumbing — the
coordinator stamps them onto every Surefire fork it launches
(`baselineProperties()` / `mutantProperties()`); setting them by hand corrupts
the protocol (a stray `active.mutant` with no catalog behind it makes
activation throw unknown-mutant). The last three rows are the §3.2 user
parameters (`outputDirectory`, `targetModules`, `excludedMutators`) observed
from the fork side: you configure them via the Mojo `<configuration>` or
`-Dspark.mutator.*`, and the coordinator echoes the resolved values into each
fork on this channel.

| Property | Values | Meaning |
|---|---|---|
| `spark.mutator.phase` | `baseline` \| `mutant` (or unset) | External orchestration marker. Unset ⇒ standalone in-process mode. **Internal — never set by hand.** |
| `spark.mutator.active.mutant` | 16-char lowercase hex | Which mutant is ACTIVE in this fork. **Internal — never set by hand.** |
| `spark.mutator.outputDirectory` | file path | Report/output directory (same key as the user-facing config — one key, both surfaces) |
| `spark.mutator.targetModules` | comma-separated module-path prefixes | Discovery registers a candidate only when the current file-path hint starts with one of the prefixes. Blank/unset ⇒ no filtering. With the property set while the hint is still the default `unknown` (no harness fed one), NOTHING is registered and a single warning is emitted — fail-safe, because silent full-catalog behavior would be the "every mutant survived" failure mode in disguise |
| `spark.mutator.excludedMutators` | comma-separated OperatorType names (`JOIN`, `FILTER`, `AGGREGATE`, `WINDOW`, `PROJECT`, `OTHER`; case-insensitive) | Discovery skips excluded operators; the match/rewrite path refuses them even if a stale catalog entry exists (observable skip, never an error). Unrecognized tokens are ignored with a one-time warning. The Python path additionally accepts legacy mutator display names, enforced in the pytest plugin |

The bridge reads these once at JVM startup and drives `MutantRegistry`, so the
activation logic is **framework-agnostic** (JUnit 5, ScalaTest, … all just call
`MutantBootstrap.activateFromSystemProperties()`).

### 3.2 User-facing configuration

**Maven plugin parameters** (`mvn -D...=...` or `<configuration>` in the plugin
declaration):


Every entry is optional — omit a parameter and its default (column 3 of the
table below) applies. The same values can be supplied per-invocation instead:
`mvn -Dspark.mutator.minMutationScore=80 -Dspark.mutator.excludedMutators=WINDOW ...`.

| Parameter | Property | Default | Meaning |
|---|---|---|---|
| `outputDirectory` | `spark.mutator.outputDirectory` | `${project.build.directory}/spark-mutator-reports` | report dir |
| `timeoutMultiplier` | `spark.mutator.timeoutMultiplier` | `2.0` | per-mutant deadline = `ceil(baseline × mult)` |
| `minMutationScore` | `spark.mutator.minMutationScore` | `0.0` | score floor (`0.0` = gate off) |
| `maxErroredCount` | `spark.mutator.maxErroredCount` | `0` | WP-24: max real-failure ERRORED (dead sessions, shim violations, mutation crashes); zero tolerance by default, negative disables |
| `maxNotAppliedRatio` | `spark.mutator.maxNotAppliedRatio` | `0.20` | WP-24: max ratio of designed not-applied mutants (`notApplied / (total − skipped)`); negative disables |
| `perTestAttribution` | `spark.mutator.perTestAttribution` | `false` | runs every mapped test per killed mutant (no fail-fast) so the report names ALL failing tests — feeds the test-value report's sole-killer verdicts; costs runtime on killed mutants |
| `targetModules` | `spark.mutator.targetModules` | *(empty)* | comma-separated module-path prefixes limiting Discovery (see §3.1 for the engine-side semantics) |
| `excludedMutators` | `spark.mutator.excludedMutators` | *(empty)* | comma-separated OperatorType names excluded from mutation; echoed into the report's `config.excludedMutators` block |
| `exitProcessOnGateFailure` | `spark.mutator.exitProcessOnGateFailure` | `true` | `false` ⇒ a gate violation throws `MojoFailureException` (Maven exit code 1, reactor honors `--fail-at-end`/`--fail-never`) instead of terminating the JVM with the dedicated exit code 2 — the multi-module escape hatch |
| `injectAddOpens` | `spark.mutator.injectAddOpens` | `true` | inject Spark's mandatory modular-runtime JVM args (the `--add-opens` set) into Surefire's `argLine`; no-op on Java 8 |


All parameters in a single `<configuration>` block:

```xml
<plugin>
  <groupId>io.github.wpunit13</groupId>
  <artifactId>spark-mutation-testing-maven-plugin</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <configuration>
    <!-- Where the reports go (default: ${project.build.directory}/spark-mutator-reports) -->
    <outputDirectory>${project.build.directory}/spark-mutator-reports</outputDirectory>

    <!-- Per-mutant deadline = ceil(baseline run time × multiplier) -->
    <timeoutMultiplier>2.0</timeoutMultiplier>

    <!-- Quality gates. All three are enforced after the reports are flushed. -->
    <minMutationScore>80.0</minMutationScore>   <!-- 0.0 = score gate off -->
    <maxErroredCount>0</maxErroredCount>        <!-- negative = disabled -->
    <maxNotAppliedRatio>0.20</maxNotAppliedRatio> <!-- negative = disabled -->

    <!-- Comma-separated on the CLI; one element per <targetModules> here -->
    <targetModules>com.example.pipeline.transforms</targetModules>
    <targetModules>com.example.pipelinesupport</targetModules>

    <!-- Operator types to skip: JOIN, FILTER, AGGREGATE, WINDOW, PROJECT, OTHER -->
    <excludedMutators>WINDOW</excludedMutators>
    <excludedMutators>OTHER</excludedMutators>

    <!-- Multi-module reactors: fail via MojoFailureException (Maven exit 1,
         honors the fail-at-end flag) instead of terminating the JVM with exit code 2 -->
    <exitProcessOnGateFailure>true</exitProcessOnGateFailure>

    <!-- Inject Spark's mandatory --add-opens set into Surefire argLine -->
    <injectAddOpens>true</injectAddOpens>
  </configuration>
</plugin>
```

**In-process gate** (JUnit 5 path):

| Property | Default | Meaning |
|---|---|---|
| `spark.mutator.minMutationScore` | unset | score floor; unset = gate off |
| `spark.mutator.enabled` / `spark.mutator.disabled` | — | force the extension on/off |

These are **test-JVM system properties** — `SparkMutatorExtension` reads them
with `System.getProperty` inside the fork that runs your tests. A `-D` on the
`mvn` command line stays in the Maven JVM and never reaches that fork (§3.3),
so set them on one of these surfaces:

Maven — Surefire `systemPropertyVariables` (or the equivalent `<argLine>-D…`):

```xml
<plugin>
  <artifactId>maven-surefire-plugin</artifactId>
  <configuration>
    <systemPropertyVariables>
      <spark.mutator.minMutationScore>80</spark.mutator.minMutationScore>
      <!-- optional: turn the extension off for a plain baseline run -->
      <!-- <spark.mutator.disabled>true</spark.mutator.disabled> -->
    </systemPropertyVariables>
  </configuration>
</plugin>
```

Gradle — the `test` task:

```groovy
test {
    systemProperty 'spark.mutator.minMutationScore', '80'
    // systemProperty 'spark.mutator.disabled', 'true'   // plain baseline run
}
```

IDE — add `-Dspark.mutator.minMutationScore=80` to the run configuration's VM
options.

**Spark extension** (always required for interception):

| Property | Value |
|---|---|
| `spark.sql.extensions` | `io.github.wpunit13.mutator.MutatorSparkExtension` |

Two equivalent surfaces — pick one:

1. **Session builder config** (canonical; what the examples do):

```java
SparkSession.builder()
    .master("local[1]")
    .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
    .getOrCreate();
```

2. **System property** (Spark seeds `SparkConf` from `spark.*` system
   properties) — same surfaces as the gate above:
   `-Dspark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension` in
   Surefire `systemPropertyVariables`, Gradle `systemProperty`, or IDE VM
   options.

`@EnableSparkMutationTesting` self-heals either way: `beforeAll` sets the
system property if it is missing, and appends the class if a different
extension is already configured. Declare it explicitly anyway — it keeps the
session config truthful for runs that bypass the annotation. One caveat: the
self-heal runs in `beforeAll`, so create the session in the suite body or
`@BeforeAll` — a session built in a static initializer is created before the
extension can act.

**Naming convention.** User-configurable `spark.mutator.*` keys are camelCase
(`outputDirectory`, `timeoutMultiplier`, `minMutationScore`) and use **one key
across both surfaces** — set it as a Maven `-D` for the Mojo, or as a Surefire
system property for the in-process path; the fork receives the same key.
Internal-only fork directives (`phase`, `active.mutant`) are dot-lowercase and
are set by the orchestrator, never by users. The Python (PySpark) path
propagates the output directory through the `SPARK_MUTATOR_OUTPUT_DIR`
environment variable instead, which `ReportSink` checks as a fallback.

### 3.3 Getting properties into the fork

`-D` on the `mvn` command line are Maven/plugin properties, **not** child-JVM
system properties. To push a property into the forked Surefire JVM (e.g. the
in-process gate), declare it explicitly:

```xml
<plugin>
  <artifactId>maven-surefire-plugin</artifactId>
  <version>3.2.5</version>
  <configuration>
    <systemPropertyVariables>
      <spark.mutator.minMutationScore>80</spark.mutator.minMutationScore>
    </systemPropertyVariables>
  </configuration>
</plugin>
```

(or the equivalent `<argLine>-Dspark.mutator.minMutationScore=80</argLine>`).

---

## 4. The quality gate (`minMutationScore`)

The mutation score is:

```
(killed + timedOut) / (killed + timedOut + survived) × 100
```

Errored and skipped mutants are excluded from both numerator and denominator.

Both paths enforce the same floors, but through different channels:

- **Maven plugin:** `MutateMojo` logs the failure and terminates the Maven JVM
  with exit code 2 when any gate is breached (see the exit-code note below).
- **In-process:** `SparkMutatorExtension.afterAll` throws
  `IllegalStateException` ⇒ JUnit container fails ⇒ Surefire exits non-zero.

Three gates, in enforcement order (all after the reports are flushed, so the
artifacts always survive a failure):

1. **WP-24 real-failure ERRORED (zero tolerance).**
   `spark.mutator.maxErroredCount` (default `0`): a real-failure ERRORED — a
   dead session, a shim violation, or a mutation crashing the pipeline — means
   the harness or engine misbehaved. `errored > maxErroredCount` fails the
   gate; negative disables.
2. **WP-24 designed not-applied (ratio).**
   `spark.mutator.maxNotAppliedRatio` (default `0.20`): the designed
   not-applied population (nodes hidden inside caches, pruned stubs, shapes
   that never execute) is shape-dependent and legitimate in bounded quantities
   — `notApplied / (totalMutants − skipped) > maxNotAppliedRatio` fails the
   gate; negative disables.
3. **Score floor (opt-in).** `spark.mutator.minMutationScore` (default `0.0`
   = off): `score < minScore` fails the gate.

The WP-24 split exists because the score formula cannot see either ERRORED
population (both are excluded from numerator and denominator) — the incident
that motivated it printed `mutationScore: 100.0` while 22 of 24 mutants never
executed.

Semantics in both cases are **opt-in**: `0.0` / unset means "report only, never
fail". The in-process path writes the report *before* throwing, so a failing
gate still leaves a CI artifact explaining the shortfall.

The score is computed once in
`mutator-core/.../report/ReportWriter.computeScore(...)` in both modes; the
in-process path reaches it via `ReportSink.computeMutationScore()`.

> **Exit codes (WP-19):** the Maven plugin path terminates the Maven JVM with
> the dedicated governance-gate exit code **2** when the gate fails — the
> reports are already flushed to disk, so CI can distinguish "gate failed (2)"
> from Maven's generic "build failed (1)". Tradeoff: the exit deliberately
> kills the Maven JVM, so in a multi-module reactor the remaining modules do
> not build (see `MutateMojo`'s javadoc). The pytest plugin path exits **2**
> the same way, with the report finalized first. The JUnit 5 in-process path
> CANNOT control Surefire's process exit code and stays a generic non-zero
> failure — a known limitation, deliberately not faked.
>
> **Multi-module reactors:** set
> `-Dspark.mutator.exitProcessOnGateFailure=false` to deliver the gate failure
> as a `MojoFailureException` instead — Maven still fails (exit code 1), but
> the reactor honors `--fail-at-end` / `--fail-never`, so the remaining
> modules build and every module's gate verdict is collected. The dedicated
> exit code 2 is lost in this mode; route CI on the report artifact or the
> Maven exit code. Alternatively, invoke the goal per module
> (`mvn -pl <module> test-compile spark-mutation-testing:mutate`) so the JVM
> termination only ever affects one module.

---

## 5. Report outputs

Written to the report directory (see §3.2):

| File | Notes |
|---|---|
| `mutation-report.json` | schema v2 (`docs/CONTRACTS.md` §5.3; v2 adds the WP-24 `NOT_APPLIED` split) |
| `mutation-report.sarif` | SARIF 2.1.0; `SURVIVED` = `warning`, `ERRORED` = `error`, `KILLED`/`TIMED_OUT` omitted |
| `mutation-report.html` | self-contained (inline CSS, no external assets) |
| `test-value-report.json` / `.html` | per-test kill attribution (see below) |

### 5.1 Test value report (per-test redundancy flag)

`test-value-report.{json,html}` answers a question the mutation score cannot:
**which test cases are redundant?** A test is `LOAD_BEARING` when at least one
mutant exists whose *only* failing test is this one (remove the test and that
mutant escapes); a test with zero sole kills is a `REDUNDANT_CANDIDATE` —
everything it catches, another test in this run also catches.

Verdicts are **relative to the current suite**, never absolute: delete the
load-bearing test and the "redundant" ones suddenly matter. Verdicts also
require per-mutant failing-test attribution:

| Path | Attribution | Verdicts |
|---|---|---|
| Maven fork (Java/Scala) | all failing tests per mutant with `spark.mutator.perTestAttribution=true`; otherwise the fork aborts at the first failure (first killer only) | meaningful with the flag; biased without |
| PySpark | all failing tests only with `per_test_attribution = true` (otherwise fail-fast records the first killer) | meaningful with the flag; biased without |
| JUnit 5 in-process (incl. Gradle thin path) | none recorded | `INSUFFICIENT_DATA` |

`attributionCoverage` (killed mutants with named failing tests / total killed)
quantifies the fidelity; the HTML report carries the same caveat.

Any catalogued mutant with no recorded outcome is synthesized as `ERRORED`, so
the schema never carries a null result. The whole pipeline is driven by
`mutator-core/.../report/ReportWriter.writeReports(catalog, results)` in both
modes.

---

## 6. Adding a Spark/Scala shim

Adding a new `(major.minor × scalaBinary)` combination is a separate, frozen runbook:
[`VERSION_ADDITION_SOP.md`](VERSION_ADDITION_SOP.md). In short: create a new
`interceptor-spark-<ver>_<scala>` module, prove compatibility *by compiling*,
add the golden cross-version test, register the module, and add the bundle +
`version_detect.py` matrix entry. Never edit an existing shim to "add" a
version.

---

## 7. Harness integration contract (frameworks other than JUnit 5)

The engine is test-framework agnostic: it hooks Spark via `spark.sql.extensions`
and runs whenever a query is analyzed, whatever code drove the query. What IS
framework-specific is the **harness** — the glue that bridges the fork boundary
inside a test framework's lifecycle. `mutator-junit5` ships that glue for
JUnit 5 (`SparkMutatorExtension`). The ScalaTest bridge (`mutator-scalatest`,
WP-18, `SparkMutatorBeforeAfterAll`) is designed but **not yet implemented** —
for the current Scala support story see [`SCALA_PIPELINES.md`](SCALA_PIPELINES.md).
Any other framework (TestNG, Spock, a
custom runner) implements the same contract against public `mutator-core` APIs:

| # | When | Phase | Obligation |
|---|---|---|---|
| 1 | Before any test runs | `mutant` | `MutantBootstrap.activateFromSystemProperties()` — activates the fork's mutant AND loads `catalog.json` into this JVM's catalog (fork-side handoff). Throws if the activated mutant is unknown; **never swallow it**. |
| 2 | After all tests ran | `baseline` | `MutationCatalogIo.writeCatalogJson(outputDir, MutationCatalogAccess.allEntries())` — the baseline fork hands Discovery's catalog to the coordinator. |
| 3 | After all tests ran | `mutant` | If `AppliedMutantTracker.lastOrNull() == activeMutant`, write `AppliedMarkerStore.write(outputDir, activeMutant)`; otherwise throw. A mutation that never executed must never be classified KILLED or SURVIVED. |

**Resolved by WP-26 for the fork path:** when the fork is driven by the Maven
plugin, `MutatorSparkExtension` performs all three obligations itself
(activation at session-extension init, `catalog.json` / applied markers from
the `ForkHandoffShutdownHook` at JVM exit) — the table above then only applies
to harnesses that must coexist with engines older than WP-26, or to frameworks
whose fork also runs the standalone in-process loop.

`outputDir` is `MutantBootstrap.outputDirectoryOrNull()`. Optional:
`TestContextTracker.setCurrentTestId(...)` / `clearCurrentTestId()` around each
test enables test-impact mapping (`mappedTestIds`), which the coordinator uses
as the per-mutant `-Dtest=` filter.

> **WP-26 (implemented):** these three obligations moved into
> `MutatorSparkExtension` (config-activated, shutdown-hook handoff), making the
> Maven plugin path pom-only — no harness glue, no annotation. The JUnit 5
> bridge stays for the standalone in-process loop and co-writes the same files
> idempotently when an annotated class runs; other frameworks can still
> implement the contract per §7.

Failing loudly is part of the contract. A harness that swallows an unknown-
mutant error or skips the marker turns a broken handoff into "every mutant
survived" — the single worst failure mode this tool can produce. When in doubt,
crash the fork.

Two boundaries of the contract: (a) it covers the **externally-orchestrated**
(Maven plugin) path — a self-orchestrated loop like the JUnit 5 extension's
standalone mode is an optional extra, not an obligation; (b) the coordinator,
not the harness, writes outcomes and final reports — the harness only ever
writes `catalog.json` and applied markers.

---

## Current status
---

## Current status

Working end-to-end: PySpark (pytest plugin), the Spark 3.5.x / Scala 2.12 + 2.13
shims with Join / Filter / Aggregate / Window / Null-Coalesce / Project mutators,
the Maven plugin + JUnit 5 paths, and the JVM survival/kill examples (WP-15).
The ScalaTest bridge is specified as WP-18 (`mutator-scalatest`) but deferred:
its planned discovery runner (`org.scalatest.junit.JUnitRunner`) does not exist
in the managed ScalaTest 3.2.18 — status, evidence, and the resume plan live in
[`SCALA_PIPELINES.md`](SCALA_PIPELINES.md). WP-17 tightens discovery to a
single plan shape and adds the applied-mutation honesty guard. WP-26
(zero-touch fork path, pitest parity), WP-27
(`Decimal → Double` type-downgrade mutator), and WP-28 (mutation-loop
performance: measure first, then parallelize or shard) are
designed-but-unimplemented future work. WP-25 (in-process per-mutant
watchdog) shipped: the JUnit 5 standalone loop enforces
`ceil(baseline × timeoutMultiplier)` per mutant with a cancel + interrupt
escalation ladder, classifies deadline hits as `TIMED_OUT`, and abandons to
a flushed partial report when a re-run thread ignores both channels
(reports record `config.timeoutEnforced: true`).