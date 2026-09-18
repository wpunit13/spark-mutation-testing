# Gradle support (thin path)

Gradle users get **full in-process mutation testing today** — the same
baseline → discover → mutate → report loop, the same `minMutationScore` gate,
and reports identical in content to a Maven run. What they do **not** get is the
Maven plugin's fork-per-mutant external loop. This page is the complete story;
there is no Gradle plugin, and none is planned (see §6).

---

## 1. What works today

The JUnit 5 in-process path ([`developer-guide.md`](developer-guide.md) §1.2)
is build-tool agnostic. `mutator-junit5`'s `SparkMutatorExtension` is driven
purely by the JUnit 5 lifecycle and system properties — it imports nothing
Maven-specific — so it runs unchanged inside Gradle's standard `test` task:

- Full loop: baseline pass → mutant discovery → in-process mutation loop
  (Catalyst cache reset between mutants) → `mutation-report.json` / `.sarif` /
  `.html`.
- Optional quality gate via `spark.mutator.minMutationScore`.
- The same suite shape as the Maven examples — see
  [`SCALA_PIPELINES.md`](SCALA_PIPELINES.md) §2.2 for the canonical
  `@EnableSparkMutationTesting` suite (Java and JUnit 5-in-Scala alike).

A runnable proof lives in `examples/spark-gradle-junit5/`.

## 2. What does not work (Maven-only)

| Capability | Status under Gradle |
|---|---|
| In-process loop (`@EnableSparkMutationTesting`) | ✅ Works |
| Fork-per-mutant external loop | ❌ Maven-only (`spark-mutation-testing:mutate`) |
| `spark-mutation-testing:mutate` plugin goal | ❌ Maven-only |
| Dedicated governance-gate exit code 2 | ❌ Maven-only — under Gradle a failed gate surfaces as the generic JUnit/Gradle non-zero test failure (same limitation as the in-process path under Surefire; see [`developer-guide.md`](developer-guide.md) §4, WP-19) |
| Per-mutant timeout enforcement (`timeoutMultiplier`) | ❌ Maven-only — the in-process loop has no watchdog; a hung mutant hangs the test JVM. The property is still accepted and echoed into the report's `config` block, but nothing enforces a deadline. Tracked as WP-25 (in-process per-mutant watchdog, planned) |
| `exitProcessOnGateFailure` / `injectAddOpens` | N/A — Mojo parameters. Under Gradle, gate failures are ordinary test failures and the JVM opens are configured by hand (§3) |

The fork-per-mutant loop is built on Surefire's fork JVM lifecycle
(`argLine` injection, `-Dtest=` filters, fork orchestration). Gradle's `Test`
task has no Surefire equivalent, and reproducing the loop against the worker
API would be a second, permanently maintained orchestration stack. The
in-process extension already *is* a complete orchestrator, so the thin path
needs zero new runtime code.

## 3. Setup

Prerequisite: the engine artifacts must be installed locally first —
`mvn -B clean install` from the repository root puts
`io.github.wpunit13:mutator-junit5:1.0.0-SNAPSHOT` and
`io.github.wpunit13:interceptor-bundle-spark-3.5_2.13:1.0.0-SNAPSHOT` into
your local Maven repository (`mavenLocal()`). After the first Maven Central
release, resolve the released coordinates from Central instead.

Minimal `build.gradle` (Groovy DSL):

```groovy
plugins {
    id 'java'
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    testImplementation 'org.apache.spark:spark-sql_2.13:3.5.3'
    testImplementation 'io.github.wpunit13:mutator-junit5:1.0.0-SNAPSHOT'
    testImplementation 'io.github.wpunit13:interceptor-bundle-spark-3.5_2.13:1.0.0-SNAPSHOT'
    testImplementation platform('org.junit:junit-bom:5.10.2')
    testImplementation 'org.junit.jupiter:junit-jupiter'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}

test {
    useJUnitPlatform()

    // REQUIRED. Spark 3.5 on Java 17 fails without these opens.
    // Copied verbatim from the root POM's spark.test.jvm.args — do not
    // invent a variant. (The Maven plugin injects this set into Surefire
    // automatically; Gradle has no equivalent hook, so it stays manual here.)
    jvmArgs '-Xmx2g', '-XX:+IgnoreUnrecognizedVMOptions',
        '--add-opens=java.base/java.lang=ALL-UNNAMED',
        '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED',
        '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
        '--add-opens=java.base/java.io=ALL-UNNAMED',
        '--add-opens=java.base/java.net=ALL-UNNAMED',
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        '--add-opens=java.base/java.util=ALL-UNNAMED',
        '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED',
        '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED',
        '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
        '--add-opens=java.base/sun.nio.cs=ALL-UNNAMED',
        '--add-opens=java.base/sun.security.action=ALL-UNNAMED',
        '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED',
        '-Djdk.reflect.useDirectMethodHandle=false',
        '-Dio.netty.tryReflectionSetAccessible=true'

    // Reports default to target/spark-mutator-reports (a Maven-ism); point
    // them at Gradle's build dir instead.
    systemProperty 'spark.mutator.outputDirectory',
        layout.buildDirectory.dir('spark-mutator-reports').get().asFile.absolutePath
}
```

The `--add-opens` list is **mandatory**: without it the Spark driver crashes
on Java 17 with `InaccessibleObjectException` before any test runs.

## 4. Suite shape

Identical to the Maven path — see
[`SCALA_PIPELINES.md`](SCALA_PIPELINES.md) §2.2. In short: annotate the class
with `@EnableSparkMutationTesting` (or register `SparkMutatorExtension`
directly) and configure the session with
`spark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension`. The
extension ensures the Spark extension is active even if the session config is
forgotten, but declaring it explicitly keeps plain (unmutated) runs honest.

Assertion guidance that materially affects the mutation score is in
[`SCALA_PIPELINES.md`](SCALA_PIPELINES.md) §2.3 (assert exact rows, not
counts; prefer `collect()` over `count()`).

## 5. Configuration surface (in-process)

Everything the in-process path reads is a **test-JVM system property**, set on
the `test` task. The Gradle CLI trap mirrors Maven's: `gradle -Dspark.mutator.…`
lands in the Gradle build JVM and never reaches the test workers — only
`systemProperty` on the task (or `systemProperties` map) crosses over.

```groovy
test {
    systemProperty 'spark.mutator.minMutationScore', '80'
}
```

Full property surface, verified against the engine and extension code:

| Property | Default | Meaning |
|---|---|---|
| `spark.mutator.minMutationScore` | unset | score floor; unset = gate off |
| `spark.mutator.maxErroredCount` | `0` | WP-24: max real-failure ERRORED (dead sessions, shim violations); negative disables |
| `spark.mutator.maxNotAppliedRatio` | `0.20` | WP-24: max designed not-applied ratio; negative disables |
| `spark.mutator.enabled` / `spark.mutator.disabled` | — | force the extension on/off — `disabled=true` gives a plain baseline run with no mutation loop |
| `spark.mutator.outputDirectory` | `target/spark-mutator-reports` (a Maven-ism) | report dir; §3 points it at Gradle's `build/` instead |
| `spark.mutator.excludedMutators` | *(empty)* | CSV of operator types (`JOIN`, `FILTER`, `AGGREGATE`, `WINDOW`, `PROJECT`, `OTHER`); the engine skips them at discovery AND refuses to rewrite them mid-run |
| `spark.mutator.targetModules` | *(empty)* | ⚠️ **do not set on the JVM paths today** — no JVM harness feeds a file-path hint, so setting it registers NOTHING (fail-safe, with a one-time warning). See [`developer-guide.md`](developer-guide.md) §3.1 |
| `spark.mutator.timeoutMultiplier` | `2.0` | echoed into the report's `config` block only — **no deadline is enforced in-process** (§2; WP-25, planned) |
| `spark.sql.extensions` | — | interception; the extension self-heals it if forgotten, but declare it explicitly so plain runs are honest (§4) |

All three gates (ERRORED count, not-applied ratio, score floor) run in
`afterAll` **after** the report is flushed, so the artifact survives a breach;
a violation throws from `afterAll` and surfaces as Gradle's generic non-zero
test failure (§2) — there is no dedicated exit code on this path.

When the score is below the floor, the extension throws from `afterAll` after
the report is already on disk, so the Gradle build fails non-zero with the
report artifact intact.

## 6. Maintenance expectation

- Artifact coordinates are `1.0.0-SNAPSHOT` until the first Maven Central
  release; after that, pin released versions.
- Gradle is **community-supported surface**: best-effort, not a CI-verified
  lane. CI runs Maven only; the Gradle example is verified manually.
- There is no Gradle plugin and no fork-per-mutant Gradle loop. If demand for
  a native Gradle loop materializes, the public contract to implement against
  is [`developer-guide.md`](developer-guide.md) §7 (harness integration
  contract) — a future bridge would drive `mutator-core` directly, not this
  documentation.
