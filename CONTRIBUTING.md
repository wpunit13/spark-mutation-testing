# Contributing to spark-mutator

Thin entry point — the details live in the linked docs, don't duplicate them here.

## Build prerequisites

- **JDK 17 (mandatory)** — the Scala shims do not compile on newer JDKs
  (Scala 2.13.8 cannot parse their class files).
- Maven 3.8+
- Python 3.9+

## Dev loop

```bash
mvn -o clean test          # full reactor, JVM side
cd python && python -m pytest   # Python side
```

Internal artifacts resolve as `1.0.0-SNAPSHOT` from `mavenLocal()` after one
`mvn -B clean install`.

## Gates every PR must keep green

- `scripts/check_no_spark_imports.sh`
- Full reactor tests (`mvn -o clean test`)
- `scripts/verify_e2e.py` + `scripts/verify_e2e_jvm.py` — both **READ-ONLY**,
  never modify them to make a change pass
- Python tests (`cd python && python -m pytest`)

## Pointers

- Architecture & configuration: [`docs/developer-guide.md`](docs/developer-guide.md)
- Releases: [`docs/RELEASING.md`](docs/RELEASING.md) — releases happen ONLY via
  the runbook; never hand-edit versions
- Scala suites: [`docs/SCALA_PIPELINES.md`](docs/SCALA_PIPELINES.md)

## PR convention

Squash-and-merge only (repo setting): one commit per PR with a descriptive
subject. Internal commit granularity does not publish to main.
