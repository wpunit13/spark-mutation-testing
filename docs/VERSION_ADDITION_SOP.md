# Version Addition SOP (§5.5)

Runbook for adding support for a new Spark × Scala binary cell, or advancing an
existing shim to a new patch. This documents the "version-addition SOP" that
`docs/ARCHITECTURE.md` §7 (Risk Register #2) references as "§5.5 step 3".

The load-bearing facts this SOP is built on:

- Catalyst ships **with** Spark, versioned identically; there is no independent
  "Catalyst API version" to target. A shim is keyed to a
  `(major.minor × scalaBinary)` **cell**, e.g. `"3.5_2.13"`.
- Catalyst is an internal package with **no semver guarantee** — even patch
  releases have broken a `case class` signature before. Compatibility is proven
  by **compiling**, never assumed from release notes.
- Each version is a **new, isolated module**. Never edit an existing shim or the
  root `spark.version` to "add" a version.

---

## Decision gate: patch vs new cell

| Request | What it is | Action |
|---|---|---|
| `3.5.7` → `3.5.9` | same `major.minor`, same Scala | **Recompile** the existing shim against the new patch and re-pin; no new module |
| `4.2_2.13` (new minor) | new cell | Full runbook below |
| `4.2_2.12` (new Scala binary of a known minor) | new cell | Full runbook below; Scala binary is a hard ABI boundary |

For a patch, update the existing `interceptor-spark-<ver>_<scala>/pom.xml`'s
local `spark.version`, rebuild, and re-run the golden cross-version test + full
suite. Done.

For a new cell, follow the runbook. It uses `4.2_2.13` as the running example.

---

## 1. Create the shim module

```
catalyst-interceptor/interceptor-spark-4.2_2.13/
├── pom.xml
└── src/main
    ├── scala/io/github/wpunit13/mutator/spark42/ShimImpl.scala
    └── resources/META-INF/services/io.github.wpunit13.mutator.api.PlanMutatorShim
```

1. Copy `interceptor-spark-3.5_2.13/pom.xml` into a new module directory and
   change its `artifactId` to `interceptor-spark-4.2_2.13`.
2. Override `spark.version` (and `scala.binary.version` if it differs) **inside
   the new module's own `<properties>`**. Do **not** edit the root
   `pom.xml`'s `spark.version` — the root comment mandates "its own pinned values".
3. Copy `ShimImpl.scala`:
   - change package `io.github.wpunit13.mutator.spark35` → `io.github.wpunit13.mutator.spark42`;
   - change `supportedVersion` → `SparkShimVersion("4.2", "2.13")`.
4. The `META-INF/services` file must name the new FQCN
   `io.github.wpunit13.mutator.spark42.ShimImpl`.

## 2. Prove compatibility by compiling

```
mvn -pl catalyst-interceptor/interceptor-spark-4.2_2.13 -am compile
```

- Compiles **unchanged** → the AST surface the shim touches is compatible.
- Does **not** compile → the compiler error names the changed `case class` /
  method. Fix *only those lines* in the copied shim. The compiler is the
  change-detector; do not pre-research from release notes.

## 3. Add the golden cross-version test (§5.5 step 3)

Add a test asserting that, for a **fixed canonical query**, the new shim's
`canonicalExprSig` / `NodeCoordinate` output is **byte-identical** to the 3.5
shim's output. This is the non-negotiable guard against coordinate drift across
versions; if signatures drift, mutants stop being reproducible across runs.

Use the project's pinned golden values (see `docs/packets/README.md`) as the
source of truth; compute no expected value from a new implementation.

## 4. Register the module

- Add `<module>interceptor-spark-4.2_2.13</module>` to
  `catalyst-interceptor/pom.xml`.
- Add a `dependencyManagement` entry in the root `pom.xml` for the new artifact
  (mirror the existing `interceptor-spark-3.5_2.13` block).

## 5. Add the bundle module

```
catalyst-interceptor/interceptor-bundle-spark-4.2_2.13/pom.xml
```

- Copy the existing bundle POM, swap its shim dependency to
  `interceptor-spark-4.2_2.13`, and set
  `<finalName>interceptor-spark-4.2_2.13</finalName>`.
- One bundle per version (the bundle POM's own rule: "do not put two bundles on
  one classpath"). Add it to the aggregator and root `dependencyManagement`.

## 6. Add the matrix entry (the lookup key)

In `python/pytest_spark_mutator/version_detect.py`:

```python
VERSION_MATRIX = {
    "3.5_2.13": "interceptor-spark-3.5_2.13.jar",
    "4.2_2.13": "interceptor-spark-4.2_2.13.jar",
}
```

**The filename string must be identical across three places**: the bundle
`<finalName>`, the `VERSION_MATRIX` value, and the jar the build produces. This
exact-match is the contract that `resolve_shim_jar_filename` and
`bundle_jars.py` both rely on.

## 7. Build + verify end-to-end

```
mvn -B clean install
python python/build_hooks/bundle_jars.py    # now copies BOTH jars
cd python && python -m pytest -v
```

Confirm with `jar tf` that the new bundle contains `MutatorSparkExtension`,
`MutantRegistry`, the new `ShimImpl`, and `META-INF/services`, and that the
version-detect tests account for the new matrix entry.

## 8. CI

`scripts/check_no_spark_imports.sh` and the existing `ci.yml` already build the
whole reactor, so the only CI change is the pinning the new shim needs. Do not
add a Spark matrix; the architecture mounts exactly one shim per run.

---

## Java floor policy

The Java bytecode floor (`maven.compiler.release`, currently **17**) is
**bound to the supported Spark matrix — it moves only at a Spark LTS
transition that requires it, never independently.** Concretely: the floor is
what the oldest supported Spark line pairs with (3.5 LTS ↔ Java 17); when a
future Spark LTS (e.g. 4.5) both requires a newer JDK *and* the oldest line
exits the N-2 window, the floor, the CI JDK (`ci.yml`'s two
`setup-java` steps), and the version-addition motion land in one atomic PR —
no skew window, no second matrix dimension (JDK × Spark combo) in between.
Building or running on a newer JDK than the floor is always fine
(`--release 17` compiles under any newer JDK; the shim-injected JVM args are
forward-safe via `-XX:+IgnoreUnrecognizedVMOptions`).

---

## Invariants (do not violate)

1. **Never edit an existing shim, a sealed bundle, or the root `spark.version`.**
   Each version is a new, isolated module with its own pinned values.
2. **Distinct package name per shim** (`...spark35`, `...spark42`, …) so
   accidental co-presence can never class-collide at load time.
3. **The compiler is the API-diff tool.** "No change" is a compile result, not a
   documentation claim.
4. **The filename is the contract** — shim artifact, bundle `<finalName>`, and
   `VERSION_MATRIX` value are a single string.
5. **Relocation stays disabled** in the bundle (see the bundle POM's rationale);
   each shim lives in its own package instead.