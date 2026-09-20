# spark-java-pipeline — the Java integration proof

A Java 17 / JUnit 5 Spark pipeline with **paired weak and hardened suites**.
It is the runnable proof of the Java integration story:

| Suite | Assertion style | Result under `mutate` |
|---|---|---|
| `OrdersPipelineWeakTest` | count-only checks | mutants **SURVIVE** |
| `OrdersPipelineHardenedTest` | exact-row checks | mutants **KILLED** |
| `ComplexPipelineHardenedTest` | exact totals + weak twin branch | filter/window mutants **KILLED**, twins SURVIVE (complex-plan smoke; 8 designed NOT_APPLIED) |

Same pipeline, same mutants — the only variable is assertion strength. That
difference is the mutation score.

---

## 1. What a downstream project actually needs

Copy three blocks into your own POM (versions pinned `1.0.0-SNAPSHOT` until the
first Maven Central release; build this repo with `mvn clean install` first so
they resolve from your local repository. Released consumers use `1.0.0` from
Maven Central — examples keep the SNAPSHOT for the dev workflow).

### a) Test-scoped dependencies

```xml
<dependency>
  <groupId>io.github.wpunit13</groupId>
  <artifactId>mutator-junit5</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <scope>test</scope>
</dependency>

<!-- Required for plain `mvn test`: MutatorSparkExtension lives here. The
     mutation plugin additionally injects the matching thin shim at mutate
     time. Match the bundle to your Spark/Scala line. -->
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

### b) The mutation plugin

```xml
<plugin>
  <groupId>io.github.wpunit13</groupId>
  <artifactId>spark-mutation-testing-maven-plugin</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</plugin>
```

No Surefire wiring is required: the plugin detects your Spark/Scala versions
from the test classpath, resolves the matching interceptor, and injects the
Spark extension plus the mandatory Java 17+ JVM opens (`--add-opens` set) into
Surefire's `argLine` itself.

### c) The annotation on your existing test class — optional

**The Maven plugin path needs no test-code change at all** (WP-26):
`MutatorSparkExtension` performs the fork handoff itself, so
`mvn test-compile spark-mutation-testing:mutate` is pom-only, pitest-style.
Annotate only for the in-process loop (`mvn test` driving the full mutation
loop inside the test JVM):

```java
import io.github.wpunit13.mutator.junit5.EnableSparkMutationTesting;

@EnableSparkMutationTesting
class OrdersPipelineHardenedTest { /* your existing tests, unchanged */ }
```

These suites ship without the annotation: `mutate` runs zero-touch, and the
annotated form produces identical verdicts (the bridge co-writes the same
handoff files idempotently).

---

## 2. Commands

```bash
mvn test                        # plain functional run (no mutation loop — the
                                # suites carry no annotation; see §1c)
                                # add -Dspark.mutator.disabled=true for a pure
                                # baseline run without the mutation loop

mvn test-compile spark-mutation-testing:mutate -Pweak      # mutants SURVIVE
mvn test-compile spark-mutation-testing:mutate -Phardened  # mutants KILLED

# one-command parallel run: baseline + 2 shard workers + merge + gates
mvn test-compile spark-mutation-testing:mutate -Phardened -Dspark.mutator.workers=2

# CI matrix: N shard jobs, then one merge job
mvn test-compile spark-mutation-testing:mutate -Phardened \
    -Dspark.mutator.shards=2 -Dspark.mutator.shard=0   # job 0 of 2
mvn test-compile spark-mutation-testing:mutate -Phardened \
    -Dspark.mutator.shards=2 -Dspark.mutator.shard=1   # job 1 of 2
mvn spark-mutation-testing:mutate -Dspark.mutator.mergeOnly=true   # merge job
```

Reports land in `target/spark-mutator-reports/`:
`mutation-report.json`, `mutation-report.sarif`, `mutation-report.html`.

### Optional knobs

```bash
# CI gate: fail when the score is below the floor (reports still written)
mvn test-compile spark-mutation-testing:mutate -Dspark.mutator.minMutationScore=80

# Multi-module reactors: fail via MojoFailureException (Maven exit 1) instead
# of terminating the JVM with exit code 2, so --fail-at-end keeps building
# the remaining modules
mvn test-compile spark-mutation-testing:mutate \
    -Dspark.mutator.exitProcessOnGateFailure=false --fail-at-end

# Skip a mutator family
mvn test-compile spark-mutation-testing:mutate -Dspark.mutator.excludedMutators=WINDOW
```

### Retarget to another supported Spark line

Override the version properties, e.g. for Spark 4.2:

```bash
mvn -Dspark.version=4.2.0 -Dscala.version=2.13.18 \
    -Djackson.version=2.21.2 \
    -Dbundle.artifact=interceptor-bundle-spark-4.2_2.13 \
    test-compile spark-mutation-testing:mutate -Pweak
```

---

## 3. Assertion guidance

Two rules materially affect your mutation score:

1. **Assert exact rows, not row counts.** `report.count() == 2` lets a mutated
   `INNER → ANTI` join survive when it still returns 2 rows.
2. **Prefer `collect()` over `count()`.** `count()` plans an extra whole-stage
   `Aggregate` node, which shifts every `NodeCoordinate` below it and
   destabilizes mutant ids.

---

## 4. References

- Orchestration modes, config surface, quality gate: [`docs/developer-guide.md`](../../docs/developer-guide.md)
- Scala suites (JUnit 5-in-Scala): [`docs/SCALA_PIPELINES.md`](../../docs/SCALA_PIPELINES.md)
- Gradle (in-process thin path): [`docs/GRADLE.md`](../../docs/GRADLE.md)
