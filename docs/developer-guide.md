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
| Use case | CI / enterprise gate; can parallelize | IDE, quick feedback, zero-plugin `mvn test` |

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
   (`SurefireConfigurator`).
4. **Run the mutation loop** (`MutationLoopCoordinator`): baseline → discover →
   fork-per-mutant → classify → merge → report → gate.

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

| Property | Values | Meaning |
|---|---|---|
| `spark.mutator.phase` | `baseline` \| `mutant` (or unset) | External orchestration marker. Unset ⇒ standalone in-process mode |
| `spark.mutator.active.mutant` | 16-char lowercase hex | Which mutant is ACTIVE in this fork |
| `spark.mutator.outputDirectory` | file path | Report/output directory (same key as the user-facing config — one key, both surfaces) |
| `spark.mutator.targetModules` | comma-separated module-path prefixes | Discovery registers a candidate only when the current file-path hint starts with one of the prefixes. Blank/unset ⇒ no filtering. With the property set while the hint is still the default `unknown` (no harness fed one), NOTHING is registered and a single warning is emitted — fail-safe, because silent full-catalog behavior would be the "every mutant survived" failure mode in disguise |
| `spark.mutator.excludedMutators` | comma-separated OperatorType names (`JOIN`, `FILTER`, `AGGREGATE`, `WINDOW`, `PROJECT`, `OTHER`; case-insensitive) | Discovery skips excluded operators; the match/rewrite path refuses them even if a stale catalog entry exists (observable skip, never an error). Unrecognized tokens are ignored with a one-time warning. The Python path additionally accepts legacy mutator display names, enforced in the pytest plugin |

The bridge reads these once at JVM startup and drives `MutantRegistry`, so the
activation logic is **framework-agnostic** (JUnit 5, ScalaTest, … all just call
`MutantBootstrap.activateFromSystemProperties()`).

### 3.2 User-facing configuration

**Maven plugin parameters** (`mvn -D...=...` or `<configuration>` in the plugin
declaration):

| Parameter | Property | Default | Meaning |
|---|---|---|---|
| `outputDirectory` | `spark.mutator.outputDirectory` | `${project.build.directory}/spark-mutator-reports` | report dir |
| `timeoutMultiplier` | `spark.mutator.timeoutMultiplier` | `2.0` | per-mutant deadline = `ceil(baseline × mult)` |
| `minMutationScore` | `spark.mutator.minMutationScore` | `0.0` | score floor (`0.0` = gate off) |
| `targetModules` | `spark.mutator.targetModules` | *(empty)* | comma-separated module-path prefixes limiting Discovery (see §3.1 for the engine-side semantics) |
| `excludedMutators` | `spark.mutator.excludedMutators` | *(empty)* | comma-separated OperatorType names excluded from mutation; echoed into the report's `config.excludedMutators` |

**In-process gate** (JUnit 5 path):

| Property | Default | Meaning |
|---|---|---|
| `spark.mutator.minMutationScore` | unset | score floor; unset = gate off |
| `spark.mutator.enabled` / `spark.mutator.disabled` | — | force the extension on/off |

**Spark extension** (always required for interception):

| Property | Value |
|---|---|
| `spark.sql.extensions` | `io.github.wpunit13.mutator.MutatorSparkExtension` |

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

Both paths enforce the same floor, but through different channels:

- **Maven plugin:** `MutateMojo` logs the failure and terminates the Maven JVM
  with exit code 2 when `minMutationScore > 0.0 && score < minMutationScore`
  (see the exit-code note below).
- **In-process:** `SparkMutatorExtension.afterAll` throws
  `IllegalStateException` when `spark.mutator.minMutationScore` is set and
  `score < min` ⇒ JUnit container fails ⇒ Surefire exits non-zero.

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

---

## 5. Report outputs

Written to the report directory (see §3.2):

| File | Notes |
|---|---|
| `mutation-report.json` | schema v1 (`docs/CONTRACTS.md` §5.3) |
| `mutation-report.sarif` | SARIF 2.1.0; `SURVIVED` = `warning`, `ERRORED` = `error`, `KILLED`/`TIMED_OUT` omitted |
| `mutation-report.html` | self-contained (inline CSS, no external assets) |

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

`outputDir` is `MutantBootstrap.outputDirectoryOrNull()`. Optional:
`TestContextTracker.setCurrentTestId(...)` / `clearCurrentTestId()` around each
test enables test-impact mapping (`mappedTestIds`), which the coordinator uses
as the per-mutant `-Dtest=` filter.

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

Working end-to-end: PySpark (pytest plugin), the Spark 3.5.x / Scala 2.12 + 2.13
shims with Join / Filter / Aggregate / Window / Null-Coalesce / Project mutators,
the Maven plugin + JUnit 5 paths, and the JVM survival/kill examples (WP-15).
The ScalaTest bridge is specified as WP-18 (`mutator-scalatest`) but deferred:
its planned discovery runner (`org.scalatest.junit.JUnitRunner`) does not exist
in the managed ScalaTest 3.2.18 — status, evidence, and the resume plan live in
[`SCALA_PIPELINES.md`](SCALA_PIPELINES.md). WP-17 tightens discovery to a
single plan shape and adds the applied-mutation honesty guard.