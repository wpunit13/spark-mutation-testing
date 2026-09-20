# pytest-spark-mutation-testing

Semantic mutation testing for **PySpark** pipelines — a pytest plugin that
mutates Catalyst plans (joins, filters, aggregates, windows, projections) and
scores whether your test suite catches them.

## Install

```bash
pip install pytest-spark-mutation-testing
```

Requires Python 3.9+ and PySpark 3.5.x or 4.2.x (4.2 is Scala 2.13-only).

## Quick start

```bash
cd your-pipeline
pytest --spark-mutate
```

The plugin runs your suite once unmutated (baseline), discovers every
mutation site in the analyzed plans, then re-runs only the affected tests per
mutant — fail-fast — and writes:

- `mutation-report.{json,sarif,html}` — per-mutant verdicts (`KILLED` /
  `SURVIVED` / `TIMED_OUT` / `NOT_APPLIED` / `ERRORED`) and the mutation score
- `test-value-report.{json,html}` — per-test kill attribution that flags
  redundant test cases

Optional config in your `pyproject.toml`:

```toml
[tool.spark-mutator]
target_modules     = ["my_pipeline.transforms"]   # scope discovery
excluded_mutators  = ["CrossJoinMutator"]         # skip a rule
timeout_multiplier = 2.0                          # per-mutant deadline
min_mutation_score = 80.0                          # CI gate
```

## Documentation

- [Full developer guide](https://github.com/wpunit13/spark-mutation-testing/tree/main/docs)
- [PySpark quick start](https://github.com/wpunit13/spark-mutation-testing#quick-start)
- Runnable proof: [`examples/pyspark-pipeline/`](https://github.com/wpunit13/spark-mutation-testing/tree/main/examples/pyspark-pipeline)
  (weak suite ⇒ mutants survive; hardened suite ⇒ mutants killed)

## License

Apache-2.0 — see [LICENSE](https://github.com/wpunit13/spark-mutation-testing/blob/main/LICENSE).