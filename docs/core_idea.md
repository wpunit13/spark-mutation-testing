# Project Specification: Spark Semantic Mutation Testing Engine (`spark-mutation-testing`)

> **Note:** This is the *target* specification — the full vision and requirements,
> not the current state. For what is implemented today see the
> [README](../README.md) and the "Current status" section of
> [`developer-guide.md`](developer-guide.md). Delivered since this spec was
> written: the aggregate/window/null mutators, the Java/Scala Maven path, the
> JVM config parity + plan diffs + governance gates (WP-19/WP-24, including
> the dedicated exit code and the `NOT_APPLIED` split), the Gradle thin path
> (WP-21), the Spark 3.5 (Scala 2.12/2.13) + 4.2 (2.13) shims, and the
> in-process per-mutant watchdog (WP-25). Still
> planned: the N-2 Spark window (4.1/4.0), first-class ScalaTest support
> (WP-18, deferred), and the zero-touch fork
> path (WP-26) — tracked as local work packets, planned.

## 1. Overview & Problem Statement
Traditional mutation testing tools (like PIT) operate on JVM bytecode and cannot evaluate distributed query semantics, while standard test suites often pass despite lacking coverage for join edge cases, filter drops, or window boundary changes. 

`spark-mutation-testing` is a zero-touch semantic mutation testing framework for Apache Spark that mutates Spark Catalyst Logical Plans and AST expressions directly during automated test execution. It operates out of a single monorepository providing unified mutation semantics across **Java, Scala, and PySpark** test pipelines.

**The mutation target is not the measurement target.** The plan is perturbed
because it is the *compiled artifact* of the user's declarative code — the
same role bytecode plays for pitest; mutating a plan node is mutating the
semantics of the `.join()`/`.filter()` call the user wrote. What is measured
is always the **suite**: a killed mutant proves some test's assertions plus
fixture data distinguish the mutated behavior; a survived mutant is a blind
spot in the suite. One honest conflation: killability depends on fixture
data too (an `INNER → ANTI` mutant is unkillable against a fixture with no
unmatched keys, however strong the assertions) — test data is part of test
quality, which is precisely the failure mode the Join mutators exist to
expose.

---

## 2. Core Architectural Requirements

* **Zero-Touch Integration:** 
  * Users do not modify pipeline logic or test code.
  * Hooks via standard build and test harnesses:
    * **Java & Scala:** Maven plugin setting Surefire/Failsafe JVM arguments.
    * **Python (PySpark):** `pytest` plugin registered via standard package entry points.
  * *Current state:* true end-to-end for PySpark; the JVM fork path still
    requires one `@EnableSparkMutationTesting`-annotated class (the harness
    glue) until WP-26 absorbs it into the engine (planned).
* **Plan-Level Semantic Mutation:** 
  * Mutations target the Catalyst Logical Plan and DataFrame AST rather than low-level bytecode or raw strings.
* **Isolation Between Mutants:** 
  * Two strategies, one per orchestration mode. **In-process** (JUnit 5
    extension, pytest plugin): a single reused `SparkSession` with explicit
    state reset between mutants (catalog invalidation, cache unpersist) to
    avoid driver-restart latency; the pytest watchdog may force-restart a
    wedged driver as a last resort. **External** (Maven plugin): a fresh
    fork JVM per mutant — process-level isolation, deliberately trading
    startup cost for determinism; the session-reuse requirement does not
    apply there.
* **Deterministic Mutation Identification:** 
  * Every mutant produces a stable ID computed via:
    $$\text{MutantID} = \text{trunc}_{64}(\text{SHA-256}(\text{FilePath} \| \text{NodeCoordinate} \| \text{OperatorType} \| \text{MutationIndex}))$$
  * The coordinate is **shape-free**: the node's structural position computed
    at a fixed synthetic root, invariant across plan clones, optimizer
    rebuilds, and predicate push-down (Spark has no stable plan-node ids of
    its own — the engine must derive the address from the plan itself, which
    is what lets a fresh fork JVM re-find the active mutation). The formula is
    frozen; see [`CONTRACTS.md`](CONTRACTS.md).

---

## 3. Mutator Catalog

| Operator Category | Mutation Rule | Target Transformation | Failure Mode Caught |
| :--- | :--- | :--- | :--- |
| **Join Mutators** | Invert / Relax Join Type | `INNER` $\to$ `LEFT`, `CROSS`, or `ANTI` | Test data lacks unmatched boundary keys. |
| **Filter Mutators** | Predicate Inversion / Drop | `filter(A AND B)` $\to$ `A`, `B`, `False`, or `~A` | Incomplete branch coverage or redundant predicates. |
| **Aggregation Mutators** | Function Swap / Group Drop | `.sum()` $\to$ `.max()`; drop 1 column from `groupBy` | Missing multi-grain verification in assertions. |
| **Window Mutators** | Frame & Order Alteration | `ROWS BETWEEN UNBOUNDED...` $\to$ `1 PRECEDING`; reverse `orderBy` | Lack of sequence/temporal ordering assertions. |
| **Null/Type Mutators** | Type Downgrade / Null Injection | `coalesce(c, default)` $\to$ `c`; `Decimal` $\to$ `Double` | Missed null-pointer or precision truncation bugs. |

Shipped status (verified against the shims): every row above is implemented
except the **`Decimal → Double` type downgrade**, which remains spec-only —
tracked as WP-27 (planned). The null
mutators ship under the Project operator (`COALESCE_BYPASS` index 0,
`INJECT_NULL` index 1). Aggregation ships a third mutation beyond this table:
zero-out of an aggregate expression to its type's default (`Literal.default`,
Aggregate index 2), and `Count` swaps to a typed `Literal(1L)` rather than
another aggregate function (keeps the column's LongType, avoiding a
schema-breaking re-analysis).

---

## 4. Execution Lifecycle & Harness

1. **Baseline Phase:**
   * Run the unmutated test suite once.
   * If any baseline test fails, abort the entire process immediately with a non-zero exit code.
   * Record per-test baseline runtimes and map test coverage per transformation.

2. **Discovery & Impact Mapping (Test Filtering):**
   * Trace pipeline transformations during the baseline run to identify mutation candidate nodes in the Catalyst plan.
   * Map each candidate mutant exclusively to the unit tests that exercise that specific plan node (Test Impact Analysis). Tests outside this scope are bypassed.

3. **Mutation Execution Loop & Fail-Fast Evaluation:**
   * Inject one mutant at a time into the active plan.
   * Execute mapped tests sequentially under a fail-fast policy: **the moment a single test assertion fails, halt further test execution for that mutant immediately.**
   * Enforce a per-mutant execution timeout threshold ($2 \times \text{baseline runtime}$) to abort infinite loops or data skew.
   * Classify outcomes into five terminal states (WP-24 split ERRORED into
     real failures vs. designed not-applied):
     * `KILLED`: At least one test assertion failed.
     * `SURVIVED`: All mapped tests passed despite the mutation (identifies missing test assertions or weak data fixtures).
     * `TIMED_OUT`: Test exceeded the allotted time limit (counted as killed).
     * `NOT_APPLIED`: The mutation never executed — the node was hidden inside
       a cache, pruned to a stub, or its shape never ran. Distinct from
       `ERRORED` so the gate can zero-tolerance real failures without
       punishing shape-dependent not-applied mutants; excluded from the score
       denominator, governed by `spark.mutator.maxNotAppliedRatio`.
     * `ERRORED`: A real failure — dead session, shim violation, or the
       mutation crashing the pipeline; zero-tolerance by default
       (`spark.mutator.maxErroredCount`).

4. **State Reset & Isolation:**
   * Clear temporary views, unpersist cached DataFrames, and clear session catalog metadata between mutant runs without stopping or re-creating the underlying `SparkContext`.

---

## 5. Repository Layout & Multi-Language Architecture

The framework is organized as a polyglot monorepo sharing a single core engine:

```
spark-mutator/
├── pom.xml                              # Root Maven aggregator build
├── mutator-core/                        # [Java] Shared state, hashing, catalog, reports (zero Spark imports)
├── mutator-junit5/                      # [Java] JUnit 5 extension: in-process orchestrator + fork bridge
├── catalyst-interceptor/                # [Scala] Catalyst plan rewriting & AST transformations
│   ├── interceptor-api/                 #     Spark-agnostic SPI (PlanMutatorShim)
│   ├── interceptor-dispatch/            #     Shim selection across Spark/Scala targets
│   ├── interceptor-runtime/             #     MutatorSparkExtension + CatalystMutationRule
│   └── interceptor-bundle-*/            #     Shaded uber-jars per Spark/Scala target
├── maven-plugin/                        # [Java] Maven plugin: fork-per-mutant loop for Java & Scala suites
├── python/                              # [Python] Pytest plugin for PySpark suites
│   ├── pyproject.toml                   # Wheel packaging configuration
│   └── pytest_spark_mutator/
│       ├── __init__.py
│       ├── plugin.py                    # Pytest hooks (pytest_runtest_protocol)
│       ├── bridge.py                    # Py4J facade over mutator-core entry points
│       ├── watchdog.py                  # Two-channel cancellation + circuit breaker
│       └── jars/                        # Embedded catalyst-interceptor shaded JARs
├── examples/                            # Verification pipelines
│   ├── spark-java-pipeline/
│   ├── spark-scala-pipeline/
│   ├── spark-gradle-junit5/
│   └── pyspark-pipeline/
├── scripts/                             # Frozen e2e verification (verify_e2e*.py)
└── docs/                                # Architecture, contracts, guides
```

### Module Responsibilities

| Sub-Module | Language | Responsibility |
| :--- | :--- | :--- |
| **`mutator-core`** | **Java 17+** | Core state machine (`KILLED`, `SURVIVED`, `TIMED_OUT`, `NOT_APPLIED`, `ERRORED`), deterministic ID hashing, catalog, test impact analysis mapping, and JSON/SARIF/HTML reporting. Zero Spark imports. |
| **`catalyst-interceptor`** | **Scala** | Implements `SparkSessionExtensions` (`MutatorSparkExtension`) with post-hoc-resolution and optimizer `Rule[LogicalPlan]` rewrites driven by version-dispatched shims. |
| **`mutator-junit5`** | **Java** | JUnit 5 extension (`@EnableSparkMutationTesting` / `SparkMutatorExtension`): standalone in-process orchestrator, and fork-side bridge (mutant activation, catalog/marker handoff) under external orchestration. |
| **`maven-plugin`** | **Java** | Maven plugin for **Java and Scala pipelines**. Auto-detects the Spark/Scala target, resolves the interceptor bundle, configures Surefire (argLine incl. the mandatory JVM opens + additionalClasspathElements), and coordinates the fork-per-mutant loop + governance gates. |
| **`pytest-spark-mutator`** | **Python** | Pytest plugin for **PySpark pipelines** (PyPI distribution: `pytest-spark-mutation-testing`). Injects `PYSPARK_SUBMIT_ARGS`, supplies the embedded interceptor JAR, drives the baseline + mutation loop with watchdog and circuit breaker. |

---

## 6. Developer Workflow Across Languages

### For Java & Scala Pipelines
Add the test-scoped dependencies and the plugin to `pom.xml`:
```xml
<dependency>
    <groupId>io.github.wpunit13</groupId>
    <artifactId>mutator-junit5</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>io.github.wpunit13</groupId>
    <artifactId>interceptor-bundle-spark-3.5_2.13</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <scope>test</scope>
</dependency>
<plugin>
    <groupId>io.github.wpunit13</groupId>
    <artifactId>spark-mutation-testing-maven-plugin</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</plugin>
```

Annotate (until WP-26 removes this — see §2):
```java
@EnableSparkMutationTesting
class MyPipelineTest { /* existing tests unchanged */ }
```

Execute:
```bash
mvn test-compile spark-mutation-testing:mutate   # fork-per-mutant loop (CI)
mvn test                                         # in-process loop, no plugin
```

### For PySpark Pipelines
Install the package (PyPI distribution name — the import/package dir is
`pytest_spark_mutator`):
```bash
pip install pytest-spark-mutation-testing
```
Execute:
```bash
pytest --spark-mutate
```

---

## 7. Multi-Version Spark Compatibility & Cross-Compilation

### 7.1 Architecture: Shim/Adapter Pattern
Due to frequent breaking changes in internal Catalyst AST case class signatures (`Join`, `Filter`, `Project`) and Scala binary incompatibilities (2.12 vs 2.13), the core engine must strictly isolate Spark version bindings:

* **Engine SPI (`interceptor-api`):** Defines a Spark-agnostic abstract interface (`PlanMutatorShim`) for AST manipulations and plan rewrites.
* **Versioned Adapters:** Dedicated sub-modules implement the SPI for specific Spark and Scala binary targets (e.g., `interceptor-spark-3.5_2.12`, `interceptor-spark-3.5_2.13`, `interceptor-spark-4.2_2.13`).
* **Isolation Guarantee:** Upstream AST signature shifts must only affect their respective version sub-module; `mutator-core` and reporter logic must never import `org.apache.spark.sql.catalyst` directly.

---

### 7.2 Version Lifecycle Policy (latest LTS + N-2)
To prevent maintenance sprawl and bloated dependencies, the project enforces a strict lifecycle policy (revised WP-20):

* **Scope:** Active support covers the **latest LTS release line** of Apache Spark **plus** the current major/minor release and the preceding two minor releases (e.g., with Spark 4.2 current and 3.5 LTS: Spark 4.2 and 3.5; the 4.1/4.0 combinations may be added from the window at any time via the version-addition SOP).
* **Deprecation Alignment:** Versions officially declared End-of-Life (EOL) by the Apache Software Foundation are dropped immediately by removing the corresponding version adapter module (e.g., Spark 3.4, EOL since October 2024, is not supported even though it once sat inside an N-2 window).
* **Zero Refactoring on EOL:** Deprecating an older version must require zero alterations to core mutator logic or the Python/Maven harness—only deletion of the target shim module.
* **Java Floor Alignment:** The Java bytecode floor (`maven.compiler.release`, currently 17) is bound to this matrix — it moves only at a Spark LTS transition that requires it (e.g. a future 4.5 LTS whose baseline exceeds the oldest supported line's pairing), never independently. The full policy lives in [`VERSION_ADDITION_SOP.md`](VERSION_ADDITION_SOP.md) ("Java floor policy").

---

### 7.3 Packaging & Runtime Resolution

#### PySpark / Python Wheels (`pytest-spark-mutation-testing`)
* **Multi-JAR Bundling:** Because compiled adapter JARs are lightweight (<200 KB each), the Python build process must bundle all active shims directly into the wheel under `pytest_spark_mutator/jars/`.
* **Zero-Config Runtime Detection:** During `pytest_configure`, the plugin inspects `pyspark.__version__` and the active Scala binary version, automatically selecting and mounting the appropriate JAR into `spark.jars`.
* **Fast-Fail Guard:** If the active environment runs an unsupported or deprecated Spark release, the runner must immediately abort execution with an explicit `UnsupportedSparkVersionError` listing currently supported versions.

#### JVM / Maven & Gradle Pipelines (`spark-mutation-testing-maven-plugin`)
* **Classpath Introspection:** The Maven plugin resolves the exact `org.apache.spark:spark-sql_<scala_ver>` dependency version present on the target project's test classpath.
* **Transitive Attachment:** It dynamically attaches the matching `interceptor-spark-<spark_ver>_<scala_ver>` artifact to Surefire/Failsafe runtime arguments without requiring manual configuration in the user's `../pom.xml`.

---

### 7.4 Extension Protocol for New Releases
Adding support for a newly released Apache Spark version must strictly follow this closed checklist:

1. Create a new module: `catalyst-interceptor/interceptor-spark-<major.minor>_<scala_ver>/`.
2. Implement `PlanMutatorShim` matching the new AST case class constructors.
3. Append the new version string to the supported runtime matrix in `pytest_spark_mutator/version_detect.py`.
4. Add the version target to the CI matrix test suite.

---

## 8. Configuration Specification & Reporting

Configuration is declared in `../pom.xml` (for Java/Scala) or `pyproject.toml` (for PySpark):

```toml
[tool.spark-mutator]
target_modules = ["my_pipeline.transforms"]
excluded_mutators = ["CrossJoinMutator"]
timeout_multiplier = 2.0
min_mutation_score = 80.0
output_dir = "target/spark-mutator-reports"
```

### Metrics & Reporting
* **Mutation Score Formula** (TIMED_OUT counts as killed; `ERRORED` and
  `NOT_APPLIED` are excluded from both terms — they are governed by the
  separate WP-24 population gates instead):
  $$\text{Mutation Score} = \left( \frac{\text{Killed} + \text{Timed Out}}{\text{Killed} + \text{Timed Out} + \text{Survived}} \right) \times 100$$
* **Artifacts:**
  * Terminal visual breakdown and execution status table.
  * Static HTML report highlighting surviving mutants alongside offending source code and Catalyst plan diffs.
  * Machine-readable `mutation-report.json` (schema v2) and SARIF output for automated CI/CD gating.
* **Governance gates** (WP-24, enforced after reports are flushed): real-failure
  `ERRORED` zero-tolerance (`maxErroredCount`), designed not-applied ratio
  (`maxNotAppliedRatio`), and the optional score floor (`minMutationScore`).
  The Maven path exits with the dedicated code 2; the in-process paths fail
  generically non-zero.

## 9. Technical considerations
1. **The Py4J IPC Control Channel (resolved):**
    * How Python tells the JVM which mutant is active. Implemented as designed:
      `python/pytest_spark_mutator/bridge.py` (`MutatorBridge.set_active_mutant`)
      calls `MutantRegistry.getInstance().setActiveMutant(id)` directly over the
      existing Py4J gateway (`spark._jvm`) — no environment-variable restarts,
      no sockets. The shim jar + extension conf ride `PYSPARK_SUBMIT_ARGS`.
2. **Catalyst Plan Caching & Memoization:**
    *  Spark aggressively caches analyzed logical plans and DataFrame lineage. The architecture must explicitly flush or bypass the query execution cache (spark.sessionState.catalog.invalidateAll(), unpersist RDDs) between mutant evaluations.
