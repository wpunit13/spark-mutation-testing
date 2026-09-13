# Scala-based Spark pipelines

This page is the single reference for what works today when your pipeline tests
are written in Scala, what does not work yet, and why. Read
[`developer-guide.md`](developer-guide.md) §7 for the underlying harness
integration contract this page builds on.

---

## 1. Support matrix (as of 2026-09-13)

| Test style | Status | How |
|---|---|---|
| **JUnit 5 written in Scala** | ✅ Supported end-to-end | `mutator-junit5` (`SparkMutatorExtension`) — both orchestration modes |
| **ScalaTest** (`AnyFunSuite`, …) | ❌ Not yet — designed as WP-18, deferred | No harness glue exists; see §3 |
| Other JVM frameworks (TestNG, Spock, …) | ❌ Not yet | Contract is public — see [`developer-guide.md`](developer-guide.md) §7 |

The engine itself is framework-agnostic: it hooks Spark via
`spark.sql.extensions` and runs whenever a query is analyzed, whatever code
drove the query. Framework support is purely a question of *harness glue* — the
lifecycle hook that (a) activates the fork's mutant and loads `catalog.json`,
and (b) writes the applied marker after the tests run. That glue ships today
only for JUnit 5.

---

## 2. Supported today: JUnit 5 written in Scala

Scala pipelines get full mutation testing by writing the test suite in Scala
against the JUnit 5 API. This is not a fallback hack — it is the same
first-class path the Java example uses, and it exercises both orchestration
modes (Maven-plugin fork loop and in-process `mvn test`).

A runnable proof lives in `examples/spark-scala-pipeline/` (weak suite ⇒
mutants survive; hardened suite ⇒ mutants killed).

### 2.1 Setup

Test-scoped dependencies (versions of internal artifacts are pinned
`1.0.0-SNAPSHOT`; `junit-jupiter` is managed by the parent BOM):

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
<dependency>
  <groupId>org.junit.jupiter</groupId>
  <artifactId>junit-jupiter</artifactId>
  <scope>test</scope>
</dependency>
```

Build plugins: `scala-maven-plugin`, `maven-surefire-plugin`, and
`spark-mutation-testing-maven-plugin` (only for the external loop) — see the
example POM for the exact declarations, including the weak/hardened Surefire
profiles.

### 2.2 Suite shape

```scala
import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{Test, TestInstance}
import org.junit.jupiter.api.Assertions.assertEquals

@EnableSparkMutationTesting
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MyPipelineSpec {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("MyPipelineSpec")
    .config("spark.sql.extensions", "io.github.wpunit13.mutator.MutatorSparkExtension")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  @Test
  def myPipelineAssertsExactRows(): Unit = {
    val report = MyPipeline.buildReport(orders, customers)
    // assert the exact rows, not just a count — see §2.3
  }
}
```

Running it:

```bash
mvn test                                        # in-process mode (no plugin)
mvn test-compile spark-mutation-testing:mutate  # external fork-per-mutant loop
```

### 2.3 Assertion guidance (Scala-pipeline specific)

Two rules that materially affect mutation scores:

1. **Assert exact rows, not row counts.** A weak assertion such as
   `report.collect().length == 2` lets a mutated `INNER → ANTI` join or a
   `FILTER → ¬P` rewrite survive when it still happens to return 2 rows.
2. **Prefer `collect().length` over `count()`.** `count()` plans an extra
   whole-stage `Aggregate` node, which shifts every `NodeCoordinate` below it
   and destabilizes mutant ids. This is the same reason the PySpark example
   uses `len(df.collect())`.

The engine's shims cover **Scala 2.12 and 2.13** on Spark 3.5.x; the worked
example is 2.13.

---

## 3. ScalaTest: designed (WP-18), deferred

### 3.1 What WP-18 would have shipped

A first-class ScalaTest trait — `SparkMutatorBeforeAfterAll` in a new
`mutator-scalatest` module — implementing exactly the harness contract of
[`developer-guide.md`](developer-guide.md) §7 as `beforeAll`/`afterAll` glue,
so a ScalaTest user's only change would be mixing in one trait:

```scala
@RunWith(classOf[...JUnitRunner])   // discovery only — see §3.2
class OrdersPipelineWeakSpec extends AnyFunSuite with SparkMutatorBeforeAfterAll { ... }
```

The design was fully specified (trait behavior, module layout, unit spec,
example migration) and its engine-side dependencies are all public mutator-core
APIs. **No part of WP-18 was implemented; the working tree is untouched.**

### 3.2 Why it was deferred (recorded 2026-09-13)

The spec pinned the discovery runner `org.scalatest.junit.JUnitRunner`, which
**does not exist in the managed ScalaTest version**:

- The root POM manages `org.scalatest:scalatest_2.13:3.2.18`.
- ScalaTest removed `org.scalatest.junit.JUnitRunner` when it modularized in
  3.1; JUnit 4 integration now lives in a separate artifact,
  `org.scalatestplus:junit-4-13_2.13`, under the FQCN
  `org.scalatestplus.junit.JUnitRunner`.
- Verified empirically: no `org/scalatest/junit/JUnitRunner` entry exists in
  any `org.scalatest` `*_2.13` 3.2.18 jar. The spec's own STOP-and-report
  condition was therefore triggered, and per that instruction no substitute
  runner was invented.

Plain ScalaTest suites additionally have a second, independent problem: they
are not Surefire-discoverable, and the Maven plugin's fork-per-mutant loop runs
through Surefire. Both gaps would need closing.

### 3.3 Why not to half-wire ScalaTest today

Do not attempt to drive a ScalaTest suite through the Maven plugin before the
bridge exists. The failure modes are silent or misleading:

- **No harness glue** ⇒ the mutant fork never calls
  `MutantBootstrap.activateFromSystemProperties()`, so `catalog.json` is never
  loaded, the Catalyst rule cannot resolve the active mutant, nothing is
  applied, and no applied marker is written ⇒ **every mutant is classified
  ERRORED** (a missing marker reads as "the mutation never executed").
- **Swallowing the unknown-mutant error** (e.g., wrapping activation in
  try/catch "to be resilient") is worse: the broken handoff then masquerades as
  **"every mutant survived"** — the single worst failure mode this tool can
  produce. Fail loudly, always.

### 3.4 Resume plan

The blocker is a one-class-name amendment, not a redesign. When WP-18 is
picked up again:

1. Amend the spec's discovery import to
   `org.scalatestplus.junit.JUnitRunner` and add
   `org.scalatestplus:junit-4-13_2.13` to the example POM (version aligned to
   the ScalaTest line; the local cache holds `3.2.16.0` for offline
   verification, `3.2.18.0` matches the managed ScalaTest exactly).
2. Implement the rest of WP-18 unchanged: `mutator-scalatest` module (parent,
   `mutator-core` compile + ScalaTest provided, `scala-maven-plugin` +
   `scalatest-maven-plugin`, Surefire `skipTests=true`), the trait mirroring
   `SparkMutatorExtension`'s bridge mode, the unit spec, the example
   migration, and the root-POM `<module>` entry.

Durability notes for that future work: JUnit 4 is frozen (maintenance mode —
an ideal, never-changing shim layer; the library exposes no JUnit 4 API of its
own), Surefire's JUnit 4 provider is mature, and the trait itself would depend
only on `mutator-core` plus ScalaTest's stable `BeforeAndAfterAll` — so the
runner choice stays a per-suite concern and can be swapped in one line if
ScalaTest ever ships a JUnit Platform engine. JUnit 6 is expected to work with
the existing `mutator-junit5` extension (same extension API, Java 17 baseline
matches), but is unverified here; validating it is a `junit-bom` pin bump plus
a reactor run.

---

## 4. Adding glue for another framework

The three-obligation harness contract (activate-and-load, write `catalog.json`
on baseline completion, verify-and-write the applied marker on mutant
completion) is fully specified in [`developer-guide.md`](developer-guide.md)
§7 against public `mutator-core` APIs. A TestNG or Spock bridge is a small
lifecycle listener — the fork protocol itself needs no changes.
