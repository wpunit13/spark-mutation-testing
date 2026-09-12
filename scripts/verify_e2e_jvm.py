#!/usr/bin/env python3
"""WP-15: End-to-end JVM survival/kill verification.

Drives the `spark-mutation-testing` Maven plugin over the weak and hardened
example pipelines (Java 17 / JUnit 5 and Scala 2.13 / JUnit 5) and proves the
WP-15 packet's required transitions:

    JOIN 2   (INNER -> ANTI)        SURVIVED under weak   -> KILLED under hardened
    FILTER 3 (predicate inversion)  SURVIVED under weak   -> KILLED under hardened

Steps:
  0. Install the reactor into the local repository (the examples resolve the
     plugin, mutator-junit5 and the interceptor bundle as SNAPSHOT artifacts).
  1. Per pipeline: run a plain baseline `mvn test` with the extension disabled
     (pure functional smoke, no mutation loop).
  2. Per pipeline and profile (weak, hardened): compile, then run the mutation
     loop with a dedicated report directory (so the hardened run cannot
     overwrite the weak run's evidence).
  3. Parse both mutation-report.json files, print the comparison table, and
     exit 0 only when every required transition is proven.

Usage:
    python3 scripts/verify_e2e_jvm.py [--offline]
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

PIPELINES = {
    "java": REPO_ROOT / "examples" / "spark-java-pipeline",
    "scala": REPO_ROOT / "examples" / "spark-scala-pipeline",
}
PROFILES = ("weak", "hardened")

# (operatorType, mutationIndex, human label) — indices must match the Spark 3.5
# shim's classify() contract.
REQUIRED_TRANSITIONS = [
    ("JOIN", 2, "JOIN 2 (INNER -> ANTI)"),
    ("FILTER", 3, "FILTER 3 (NOT P)"),
]


def fail(message: str) -> "None":
    print(f"\nFAIL: {message}", file=sys.stderr)
    sys.exit(1)


def resolve_java_home() -> str:
    """Spark 3.5.x needs JDK 17. Honor JAVA_HOME, then macOS java_home -v 17."""
    env_home = os.environ.get("JAVA_HOME")
    if env_home and Path(env_home, "bin", "java").exists():
        return env_home
    if sys.platform == "darwin":
        try:
            out = subprocess.run(
                ["/usr/libexec/java_home", "-v", "17"],
                capture_output=True, text=True, check=True)
            candidate = out.stdout.strip()
            if candidate:
                return candidate
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
    fail("JDK 17 not found. Set JAVA_HOME to a Java 17 installation "
         "(Spark 3.5.x does not support newer JDKs).")


def run(cmd, cwd: Path, env: dict, offline: bool) -> None:
    full = list(cmd) + (["-o"] if offline else [])
    print(f"\n$ {' '.join(full)}\n  (cwd: {cwd})", flush=True)
    result = subprocess.run(full, cwd=str(cwd), env=env)
    if result.returncode != 0:
        fail(f"command exited {result.returncode}: {' '.join(full)}")


def report_path(pipeline: str, profile: str) -> Path:
    return (PIPELINES[pipeline] / "target"
            / f"spark-mutator-reports-{profile}" / "mutation-report.json")


def load_report(path: Path) -> dict:
    if not path.is_file():
        fail(f"missing report: {path}")
    data = json.loads(path.read_text())
    total = data.get("summary", {}).get("totalMutants", 0)
    if total <= 0:
        fail(f"{path} reports {total} mutants — discovery produced an empty "
             f"catalog (bridge did not write catalog.json, or no plan nodes "
             f"were exercised).")
    return data


def coordinate_transitions(weak: dict, hardened: dict, operator_type: str,
                           mutation_index: int):
    """Coordinate-matched SURVIVED -> KILLED transitions for one
    (operatorType, mutationIndex).

    Coordinates are deterministic across runs, so matching on coordinateHex
    pairs the weak and hardened entries 1:1. A single logical mutation can be
    catalogued under several coordinates (Catalyst batches see the same node
    at different plan depths — analyzer vs. post-pushdown), and NOT(P) is only
    count-preserving at some of those shapes, so the packet's requirement is
    'at least one coordinate transitions', not 'all of them'."""
    weak_by_coord = {m["coordinateHex"]: m["result"]["status"]
                     for m in weak.get("mutants", [])
                     if m.get("operatorType") == operator_type
                     and m.get("mutationIndex") == mutation_index}
    hardened_by_coord = {m["coordinateHex"]: m["result"]["status"]
                         for m in hardened.get("mutants", [])
                         if m.get("operatorType") == operator_type
                         and m.get("mutationIndex") == mutation_index}
    proven = sorted(
        c for c in weak_by_coord
        if c in hardened_by_coord
        and weak_by_coord[c] == "SURVIVED"
        and hardened_by_coord[c] == "KILLED")
    return proven, weak_by_coord, hardened_by_coord


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--offline", action="store_true",
                        help="run Maven in offline mode (-o)")
    args = parser.parse_args()

    java_home = resolve_java_home()
    env = os.environ.copy()
    env["JAVA_HOME"] = java_home
    env["PATH"] = os.path.join(java_home, "bin") + os.pathsep + env.get("PATH", "")
    print(f"JAVA_HOME: {java_home}")

    # Step 0: reactor artifacts (plugin, mutator-junit5, interceptor bundles)
    # must be in the local repository before the examples can resolve them.
    run(["mvn", "clean", "install", "-DskipTests"], REPO_ROOT, env, args.offline)

    # Steps 1 + 2: baseline smoke, then weak/hardened mutation runs.
    for pipeline, module_dir in PIPELINES.items():
        print(f"\n=== {pipeline} pipeline: baseline smoke "
              f"(mutation loop disabled) ===")
        run(["mvn", "test", "-Dspark.mutator.disabled=true"],
            module_dir, env, args.offline)

        for profile in PROFILES:
            out_dir = module_dir / "target" / f"spark-mutator-reports-{profile}"
            if out_dir.exists():
                shutil.rmtree(out_dir)
            print(f"\n=== {pipeline} pipeline: mutation run ({profile}) ===")
            run(["mvn", "test-compile", "spark-mutation-testing:mutate",
                 f"-P{profile}",
                 f"-Dspark.mutator.outputDirectory={out_dir}"],
                module_dir, env, args.offline)

    # Step 3: parse reports and prove the transitions.
    print("\n=== Survival/kill proof ===")
    header = (f"{'pipeline':<8} {'mutant':<24} {'weak':<10} {'hardened':<10} "
              f"{'proof'}")
    print(header)
    print("-" * len(header))

    all_proven = True
    for pipeline in PIPELINES:
        weak = load_report(report_path(pipeline, "weak"))
        hardened = load_report(report_path(pipeline, "hardened"))

        for operator_type, mutation_index, label in REQUIRED_TRANSITIONS:
            proven, weak_by_coord, hardened_by_coord = coordinate_transitions(
                weak, hardened, operator_type, mutation_index)
            if not weak_by_coord or not hardened_by_coord:
                all_proven = False
                print(f"{pipeline:<8} {label:<24} {'MISSING':<10} "
                      f"{'-':<10} FAIL")
                continue

            all_proven = all_proven and bool(proven)
            verdict = "PASS" if proven else "FAIL"
            print(f"{pipeline:<8} {label:<24} {weak_by_coord and next(iter(weak_by_coord.values())) and 'SURVIVED':<10} "
                  f"{hardened_by_coord and next(iter(hardened_by_coord.values())) and 'KILLED':<10} "
                  f"{verdict} ({len(proven)}/{len(weak_by_coord)} node(s) transition)")

    if not all_proven:
        fail("not every required mutant transitioned from SURVIVED (weak) to "
             "KILLED (hardened)")

    print("\nPASS: JOIN and FILTER survival-to-kill transitions proven for "
          "both pipelines.")
    sys.exit(0)


if __name__ == "__main__":
    main()
