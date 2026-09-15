# Test & Verification Strategy

The authoritative answer to two questions: **"how do I make sure a new Spark
version works fully?"** and **"how do I make sure I haven't broken anything?"**
If a verification step is not described here, it is not part of the strategy.

---

## 0. Vocabulary: a supported Spark/Scala combination

The unit of shim support is a **Spark/Scala combination** — one Spark minor
version × Scala binary version pair, keyed `<sparkMinor>_<scalaBinary>`:

| | Scala **2.12** | Scala **2.13** |
|---|---|---|
| **Spark 4.2** | — (Spark 4.x is 2.13-only) | ✅ supported |
| **Spark 4.1 / 4.0** | — | planned (N-2 window) |
| **Spark 3.5** (latest LTS) | ✅ supported | ✅ supported |
| **Spark 3.4** | ❌ ASF-EOL 2024-10 — never | ❌ ASF-EOL — never |

Why both dimensions force separate support: Catalyst internals have no semver
guarantee (each Spark minor needs a shim compiled against that line), and the
Scala 2.12/2.13 binary boundary is not linkable across. One combination = one
shim module, one wheel jar, one detection key, one CI matrix leg, one golden
suite. (The frozen `VERSION_ADDITION_SOP.md` refers to a combination as a
"cell" — same thing.)

---

## 1. The verification pyramid

Every layer catches a different class of breakage. A green lower layer never
implies a green higher one.

| Layer | Proves | Where | Cost |
|---|---|---|---|
| **L1 — Engine units** | Hashing, catalog, reports, registry, loop coordination — no Spark involved | `mutator-core`, `maven-plugin` surefire tests | seconds |
| **L2 — Shim contract (per Spark/Scala combination)** | `classify` / `mutate*` on real plans + **golden** signatures, coordinates and mutant IDs, pinned cross-version | each `interceptor-spark-*` module's ScalaTest suite | ~1 min each |
| **L3 — Engine in-process** | `spark.sql.extensions` → dispatcher → shim → Catalyst rule, end to end, on a real session | `interceptor-runtime` specs | ~1 min |
| **L4 — Product e2e** | The actual promise: a mutant that SURVIVES the weak suite is KILLED by the hardened suite | `scripts/verify_e2e.py` (PySpark path) · `scripts/verify_e2e_jvm.py` (JVM path) | ~2 min / ~10 min |

Cross-cutting CI gates (not layers — invariants):

- `scripts/check_no_spark_imports.sh` — `mutator-core` never imports Spark.
- `scripts/check_version_hygiene.sh` — versions change only via the release
  runbook (PR merge guard).
- `python/tests/test_version_detect.py::test_supported_versions_match_the_jvm_side`
  — `SparkVersionDetector.SUPPORTED_VERSIONS` (JVM detection) and
  `VERSION_MATRIX` (Python detection) must be identical sets.

CI wiring (`.github/workflows/ci.yml`): the `checks` job runs the gates once;
`build-and-test` is the fast lane (full reactor + wheel + Python) on every
push; `shim-matrix` runs one leg per supported combo on PRs and `main`.

---

## 2. The regression net — "did I break anything?"

Run, in order (or just run `scripts/verify_all.sh`, which does exactly this):

```bash
mvn -o clean test                                   # L1 + L2 + L3: whole reactor
python3 python/build_hooks/bundle_jars.py           # wheel carries every combo's jar
python3 -m pytest python/tests/ -q                  # detection, wheel, parity
python3 scripts/verify_e2e.py                       # L4: PySpark path
python3 scripts/verify_e2e_jvm.py                   # L4: JVM path
```

Interpretation:

| Failure in | Class of breakage |
|---|---|
| Reactor tests | engine logic, or a Catalyst API/coordinate drift in some shim |
| Python suite | packaging, version detection, or the JVM/Python matrix drifted apart |
| `verify_e2e.py` | the PySpark orchestration path (plugin, bridge, watchdog, reports) |
| `verify_e2e_jvm.py` | the JVM orchestration path (Mojo, forks, JUnit 5 bridge, reports) |

`verify_e2e*` prove the product promise, not internals: each runs a weak and a
hardened suite over the example pipelines and requires the pinned
`(operatorType, mutationIndex)` mutants to transition SURVIVED → KILLED.
Exit 0 on both is the single strongest "nothing broke" signal.

Environment prerequisites: **JDK 17** (Spark 3.5 fails to build on newer JDKs
— Scala 2.13.8 cannot parse their class files), and for `verify_e2e.py` a
Python env with `pyspark` + `pytest` installed (`PYSPARK_PYTHON` should point
at the same interpreter; Spark 4.x requires the worker to match the driver's
minor version).

---

## 3. Verifying a NEW Spark version — "does it work fully?"

After integrating a combination per `VERSION_ADDITION_SOP.md` (new module, compile
proof, goldens computed from the trusted shim, registrations), verify in this
order. Substituting the WP-20 combination throughout: `<combo>` = `4.2_2.13`,
`<X.Y.Z>` = `4.2.0`, `<S.V>` = `2.13.18`, `<J.V>` = `2.21.2`.

### 3.1 Reactor + shim suite (L1–L3, both orchestration paths' engine)

```bash
mvn -o clean test                                        # incl. the new shim's goldens
mvn -o test -pl catalyst-interceptor/interceptor-spark-<combo>
mvn -o clean install -DskipTests                         # artifacts for the e2e steps
python3 python/build_hooks/bundle_jars.py                # new jar into the wheel
```

### 3.2 PySpark path (L4)

```bash
python3 -m venv "$TMPDIR/venv-<xy>"
"$TMPDIR/venv-<xy>/bin/pip" install "pyspark==<X.Y.Z>" pytest
"$TMPDIR/venv-<xy>/bin/pip" install --no-deps -e python/   # keep the target pyspark
export PYSPARK_PYTHON="$TMPDIR/venv-<xy>/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
"$TMPDIR/venv-<xy>/bin/python" scripts/verify_e2e.py
```

`VERIFICATION PASSED` proves discovery, the mutation loop, classification,
reporting and the survival→kill transitions **on the new line**. Gate exit
code 2 on `weak.toml` (80% floor) is the quality gate, not a crash.

Prerequisite: a pyspark distribution for the target version whose **bundled
Scala binary** matches the combination — `detect_scala_binary()` reads
`spark-core_<scala>-*.jar` from the installed distribution, so pip reality
decides the combo. Check first:

```bash
python3 -c "import pyspark, pathlib; print([p.name for p in (pathlib.Path(pyspark.__file__).parent/'jars').iterdir() if p.name.startswith('spark-core')])"
```

### 3.3 JVM path (L4)

The example pipelines inherit the reactor defaults (Spark 3.5). Retarget one
per flag — every override is a normal Maven property:

```bash
cd examples/spark-java-pipeline    # and spark-scala-pipeline

# Baseline smoke (mutation loop disabled)
mvn -o clean test -Dspark.mutator.disabled=true \
    -Dspark.version=<X.Y.Z> -Dscala.version=<S.V> -Djackson.version=<J.V>

# Mutation runs (weak, then hardened) with per-combo report dirs
mvn -o test-compile spark-mutation-testing:mutate -Pweak \
    -Dspark.version=<X.Y.Z> -Dscala.version=<S.V> -Djackson.version=<J.V> \
    -Dbundle.artifact=interceptor-bundle-spark-<combo> \
    -Dspark.mutator.outputDirectory=target/reports-<combo>-weak
# ...repeat with -Phardened and target/reports-<combo>-hardened
```

What each override does:

| Property | Why |
|---|---|
| `-Dspark.version` | the example's `spark-sql_<scala>` dependency (inherited from the root pom otherwise) |
| `-Dscala.version` | the target line's Scala compiler/library pin |
| `-Djackson.version` | the root pom pins databind for the 3.5 line; Spark 4.x's `jackson-module-scala` needs a newer databind. Look the pin up in the target's `spark-parent` pom: `grep fasterxml.jackson.version ~/.m2/repository/org/apache/spark/spark-parent_2.13/<X.Y.Z>/spark-parent_2.13-<X.Y.Z>.pom` |
| `-Dbundle.artifact` | the bundle the example mounts for plain `mvn test`; it must be the **same line** as the injected shim, or `ShimDispatcher` fails loudly with "expected exactly one PlanMutatorShim implementation" |

`ShimDispatcher` refusing two shims is a safety feature, not a bug: never put
two Spark-line bundles/shims on one classpath.

Verification = the survival→kill proof on the new line: compare the two report
dirs for the required transitions (JOIN 2 / FILTER 3 on the examples), e.g.
reusing `verify_e2e_jvm.py`'s criteria — every `(coordinateHex)` that is
SURVIVED in weak and KILLED in hardened, at least one per required mutant.
The WP-20 probe proved this for Spark 4.2.0 on the Java example (JOIN 2: 3/4
coordinates transition, FILTER 3: 2/2, 100% hardened score).

> Do **not** inject `-Dspark.version=…` via `MAVEN_ARGS` around
> `verify_e2e_jvm.py`: its step 0 rebuilds the **whole reactor**, and the
> override would retarget the 3.5 shim modules too. Override per example
> invocation only (as above).

### 3.4 "Fully verified" checklist

- [ ] Reactor green including the new combination's goldens (L1–L3)
- [ ] PySpark e2e `VERIFICATION PASSED` on the new line (L4), if a matching wheel exists
- [ ] JVM e2e transitions proven on the new line (L4), per example
- [ ] `SUPPORTED_VERSIONS == VERSION_MATRIX` parity updated and green
- [ ] CI matrix combination list contains the new combination
- [ ] Docs state the matrix consistently (ARCHITECTURE §1.1/§5.1, core_idea
      §7.2, README, `version_detect.py` docstring)

---

## 4. Reference pins (as of WP-20)

| Line | `spark.version` | `scala.version` | `jackson.version` | Cells |
|---|---|---|---|---|
| 3.5.x (latest LTS) | 3.5.3 | 2.13.8 | 2.15.2 (root default) | `3.5_2.12`, `3.5_2.13` |
| 4.2.x (current GA) | 4.2.0 | 2.13.18 | 2.21.2 | `4.2_2.13` |

Lifecycle policy (core_idea §7.2): latest LTS line **plus** the N-2 window
from current GA, minus ASF-EOL lines (Spark 3.4, EOL 2024-10, is never added).

## 5. Known limitations

- The JVM path's `interceptor-runtime`/`interceptor-api` reactor artifacts are
  compiled against the reactor's Spark line (3.5). They are **proven** to
  execute on Spark 4.2 (WP-20 probe), but every new Spark line must re-verify
  §3.3 — binary linkage is a compile-independent property (the PySpark path is
  immune: its bundles recompile every Scala layer per line).
- The PySpark e2e runs one combo per environment (the installed pyspark
  decides); the CI matrix covers every declared combo at L2 + packaging.
- `verify_e2e*.py` are frozen (never edit them to make a run pass).
