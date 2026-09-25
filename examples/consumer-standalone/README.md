# consumer-standalone

A **standalone downstream project** — no parent POM, no reactor classes — that
exercises the published `spark-mutation-testing` artifacts exactly the way a
real consumer resolves them from Maven Central.

Why it exists: every 1.0.0-release defect (a missing published coordinate, a
partial publish, parent-POM coupling) was invisible to the in-repo test suite,
which runs against the reactor. This project is the consumer-shaped check.

Test complexity ramps deliberately:

| Class | Exercises |
|---|---|
| `SimpleReadWriteTest` | read → filter one column → write → read back |
| `JoinAggregateTest` | inner join + groupBy agg + filter-on-agg |
| `WindowTest` | window ranking + partition-wide aggregate |
| `ComplexPipelineTest` | nested JSON schema, explode, UDF, LEFT join, union, pivot, window, partitioned parquet roundtrip |

The 1.0.0-era `PROJECT` matching nondeterminism (identical runs classifying the
same mutants as `KILLED` or `NOT_APPLIED`) is fixed as of 1.0.1 — the catalog
filters optimizer-eliminated sites via computed-sig viability and re-anchors
survivors by deep subtree fingerprint. The not-applied ratio gate therefore
runs at its default (`spark.mutator.maxNotAppliedRatio=0.20`) with no
compensation in the POM; the real-failure `ERRORED` gate stays at its
zero-tolerance default.

Run it against a freshly built reactor (fresh local repository, no `~/.m2`):

```bash
mvn -B install -DskipTests -Dmaven.repo.local="$RUNNER_TEMP/m2"          # from the repo root
mvn -B test -f examples/consumer-standalone/pom.xml \
    -Dmaven.repo.local="$RUNNER_TEMP/m2" -Dmutator.version=1.0.0-SNAPSHOT
```

Or against a published release (the default `mutator.version`):

```bash
mvn -B test -f examples/consumer-standalone/pom.xml
```

CI runs both the in-process test path and the fork-per-mutant plugin path on
every PR and main push (`.github/workflows/ci.yml` → `consumer-verification`),
and the release workflow resolves the full coordinate set through
`scripts/verify-release.sh` before announcing a release.
