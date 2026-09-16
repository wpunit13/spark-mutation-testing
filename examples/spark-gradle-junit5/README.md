# spark-gradle-junit5 example

Runnable proof of the Gradle thin path: full in-process mutation testing
(baseline → discover → mutate → report) under Gradle's standard `test` task,
with **no Gradle plugin**. See [`docs/GRADLE.md`](../../docs/GRADLE.md) for
the complete guide.

This example is **not** a Maven module and is deliberately outside the root
Maven aggregator.

## Prerequisites

- Java 17
- A locally installed Gradle 8+ (no wrapper is committed; run `gradle test`
  with your local Gradle)
- Maven 3.8+ — the engine artifacts are `1.0.0-SNAPSHOT` coordinates resolved
  from `mavenLocal()`, so build them first:

```bash
mvn -B clean install
```

(Run from the repository root. After the first Maven Central release, the
example can resolve released coordinates from Central instead.)

## Run

```bash
cd examples/spark-gradle-junit5
gradle test
```

What you should see:

1. The suite passes (`OrdersPipelineGradleSpec.weakTestAssertsRowCountOnly`).
2. The mutation loop engages: the log shows baseline discovery of mutants
   (join / filter sites) and the in-process loop re-running the suite against
   each one.
3. `build/spark-mutator-reports/mutation-report.json` (plus `.sarif` and
   `.html`) is written.

The suite is deliberately **weak** (row-count-only assertion): it kills the
join-shape mutants the fixture makes visible (12 of 24) and lets the rest —
including the `JOIN → ANTI` and `FILTER → ¬P` sites it was designed to miss —
survive. Harden the assertion to exact rows to kill the survivors (see the
Maven example's `OrdersPipelineHardenedTest`).

## Quality gate

The gate is off by default (report only). To enforce a floor, uncomment the
`spark.mutator.minMutationScore` line in `build.gradle` — with this weak suite
the gate will fail the build, which is the expected behavior.

## CI status

CI does **not** run Gradle. Gradle is community-supported surface
(best-effort); this example is verified manually. The Maven reactor remains
the CI-verified lane.
