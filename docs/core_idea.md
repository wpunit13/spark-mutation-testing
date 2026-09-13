# Project Specification: Spark Semantic Mutation Testing Engine (`spark-mutation-testing`)

> **Note:** This is the *target* specification — the full vision and requirements,
> not the current state. For what is implemented today see the
> [README](../README.md) and the "Current status" section of
> [`developer-guide.md`](developer-guide.md). The aggregate/window/null mutators
> and the Java/Scala Maven path called out for "later phases" below have since
> been delivered; the Spark-version matrix, first-class ScalaTest support, and
> the governance gates remain planned.

## 1. Overview & Problem Statement
Traditional mutation testing tools (like PIT) operate on JVM bytecode and cannot evaluate distributed query semantics, while standard test suites often pass despite lacking coverage for join edge cases, filter drops, or window boundary changes. 

`spark-mutation-testing` is a zero-touch semantic mutation testing framework for Apache Spark that mutates Spark Catalyst Logical Plans and AST expressions directly during automated test execution. It operates out of a single monorepository providing unified mutation semantics across **Java, Scala, and PySpark** test pipelines.

---

## 2. Core Architectural Requirements

* **Zero-Touch Integration:** 
  * Users do not modify pipeline logic or test code.
  * Hooks via standard build and test harnesses:
    * **Java & Scala:** Maven plugin setting Surefire/Failsafe JVM arguments.
    * **Python (PySpark):** `pytest` plugin registered via standard package entry points.
* **Plan-Level Semantic Mutation:** 
  * Mutations target the Catalyst Logical Plan and DataFrame AST rather than low-level bytecode or raw strings.
* **Single-Session Reuse:** 
  * Reuses a single active `SparkSession` across mutant executions to eliminate the latency of restarting the JVM driver context.
* **Deterministic Mutation Identification:** 
  * Every mutant produces a stable ID computed via:
    $$\text{MutantID} = \text{hash}(\text{FilePath} + \text{PlanNodeId} + \text{OperatorType} + \text{MutationIndex})$$

---

## 3. Mutator Catalog

| Operator Category | Mutation Rule | Target Transformation | Failure Mode Caught |
| :--- | :--- | :--- | :--- |
| **Join Mutators** | Invert / Relax Join Type | `INNER` $\to$ `LEFT`, `CROSS`, or `ANTI` | Test data lacks unmatched boundary keys. |
| **Filter Mutators** | Predicate Inversion / Drop | `filter(A AND B)` $\to$ `A`, `B`, `False`, or `~A` | Incomplete branch coverage or redundant predicates. |
| **Aggregation Mutators** | Function Swap / Group Drop | `.sum()` $\to$ `.max()`; drop 1 column from `groupBy` | Missing multi-grain verification in assertions. |
| **Window Mutators** | Frame & Order Alteration | `ROWS BETWEEN UNBOUNDED...` $\to$ `1 PRECEDING`; reverse `orderBy` | Lack of sequence/temporal ordering assertions. |
| **Null/Type Mutators** | Type Downgrade / Null Injection | `coalesce(c, default)` $\to$ `c`; `Decimal` $\to$ `Double` | Missed null-pointer or precision truncation bugs. |

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
   * Classify outcomes into four terminal states:
     * `KILLED`: At least one test assertion failed (or timed out).
     * `SURVIVED`: All mapped tests passed despite the mutation (identifies missing test assertions or weak data fixtures).
     * `TIMED_OUT`: Test exceeded the allotted time limit (counted as killed).
     * `ERRORED`: Unhandled driver, JVM, or unrecoverable execution crash.

4. **State Reset & Isolation:**
   * Clear temporary views, unpersist cached DataFrames, and clear session catalog metadata between mutant runs without stopping or re-creating the underlying `SparkContext`.

---

## 5. Repository Layout & Multi-Language Architecture

The framework is organized as a polyglot monorepo sharing a single core engine:

```
spark-mutator/
├── pom.xml                              # Root Maven aggregator build
├── mutator-core/                        # [Java] Shared state, hashing, metrics, reports
├── catalyst-interceptor/                # [Scala] Catalyst plan rewriting & AST transformations
├── maven-plugin/                        # [Java] Maven plugin for Java & Scala suites
├── python/                              # [Python] Pytest plugin for PySpark suites
│   ├── pyproject.toml                   # Wheel packaging configuration
│   └── pytest_spark_mutator/
│       ├── __init__.py
│       ├── plugin.py                    # Pytest hooks (pytest_runtest_protocol)
│       └── jars/                        # Embedded catalyst-interceptor shaded JAR
└── examples/                            # Verification pipelines
    ├── spark-java-pipeline/
    ├── spark-scala-pipeline/
    └── pyspark-pipeline/
```

### Module Responsibilities

| Sub-Module | Language | Responsibility |
| :--- | :--- | :--- |
| **`mutator-core`** | **Java 17+** | Core state machine (`KILLED`, `SURVIVED`), deterministic ID hashing, test impact analysis mapping, and JSON/HTML reporting. |
| **`catalyst-interceptor`** | **Scala** | Implements `SparkSessionExtensions` with custom `Rule[LogicalPlan]` pattern-matching rules to transform AST nodes (Joins, Filters, Aggregations) at runtime. |
| **`maven-plugin`** | **Java** | Maven plugin for **Java and Scala pipelines**. Auto-configures Surefire/Failsafe JVM arguments and coordinates the mutation test loop. |
| **`pytest-spark-mutator`** | **Python** | Pytest plugin for **PySpark pipelines**. Injects `PYSPARK_SUBMIT_ARGS`, supplies the embedded interceptor JAR, and drives the Python test harness. |

---

## 6. Developer Workflow Across Languages

### For Java & Scala Pipelines
Add the plugin to `../pom.xml`:
```xml
<plugin>
    <groupId>io.github.wpunit13</groupId>
    <artifactId>spark-mutation-testing-maven-plugin</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</plugin>
```

Execute:
```bash
mvn spark-mutation-testing:mutate
```

### For PySpark Pipelines
Install the package:
```bash
pip install pytest-spark-mutator
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
* **Versioned Adapters:** Dedicated sub-modules implement the SPI for specific Spark and Scala binary targets (e.g., `interceptor-spark-3.4_2.12`, `interceptor-spark-3.5_2.13`, `interceptor-spark-4.0_2.13`).
* **Isolation Guarantee:** Upstream AST signature shifts must only affect their respective version sub-module; `mutator-core` and reporter logic must never import `org.apache.spark.sql.catalyst` directly.

---

### 7.2 Version Lifecycle Policy ($N-2$)
To prevent maintenance sprawl and bloated dependencies, the project enforces a strict $N-2$ lifecycle policy:

* **Scope:** Active support is limited to the current major/minor release of Apache Spark and the preceding two minor releases (e.g., Spark 4.0, 3.5, and 3.4).
* **Deprecation Alignment:** Versions officially declared End-of-Life (EOL) by the Apache Software Foundation are dropped immediately by removing the corresponding version adapter module.
* **Zero Refactoring on EOL:** Deprecating an older version must require zero alterations to core mutator logic or the Python/Maven harness—only deletion of the target shim module.

---

### 7.3 Packaging & Runtime Resolution

#### PySpark / Python Wheels (`pytest-spark-mutator`)
* **Multi-JAR Bundling:** Because compiled adapter JARs are lightweight (<200 KB each), the Python build process must bundle all active shims directly into the wheel under `pytest_spark_mutator/jars/`.
* **Zero-Config Runtime Detection:** During `pytest_configure`, the plugin inspects `pyspark.__version__` and the active Scala binary version, automatically selecting and mounting the appropriate JAR into `spark.jars`.
* **Fast-Fail Guard:** If the active environment runs an unsupported or deprecated Spark release, the runner must immediately abort execution with an explicit `UnsupportedSparkVersionError` listing currently supported versions.

#### JVM / Maven & Gradle Pipelines (`spark-mutator-maven-plugin`)
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
* **Mutation Score Formula:**
  $$\text{Mutation Score} = \left( \frac{\text{Killed Mutants}}{\text{Killed Mutants} + \text{Survived Mutants}} \right) \times 100$$
* **Artifacts:**
  * Terminal visual breakdown and execution status table.
  * Static HTML report highlighting surviving mutants alongside offending source code and Catalyst plan diffs.
  * Machine-readable `mutation-report.json` and SARIF output for automated CI/CD gating.

## 9. Technical considerations
1. **The Py4J IPC Control Channel:**
    * How Python tells the JVM which mutant is active. PySpark should call spark._jvm.io.github.wpunit13.mutator.MutantRegistry.setActiveMutant("uuid") directly over the existing Py4J gateway, avoiding environment variable restarts or network sockets.
2. **Catalyst Plan Caching & Memoization:**
    *  Spark aggressively caches analyzed logical plans and DataFrame lineage. The architecture must explicitly flush or bypass the query execution cache (spark.sessionState.catalog.invalidateAll(), unpersist RDDs) between mutant evaluations.
