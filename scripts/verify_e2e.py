#!/usr/bin/env python3
"""End-to-End Survival/Kill Verification Script (WP-07).

Executes the example pipeline under both weak and hardened test configurations,
parses the resulting mutation reports, and verifies that the exact same mutantId
is SURVIVED under the weak suite and KILLED under the hardened suite for at
least one JOIN mutant and at least one FILTER mutant.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


def find_repo_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in [current, *current.parents]:
        if (candidate / "examples").is_dir() and (candidate / "scripts").is_dir():
            return candidate
    raise RuntimeError("Repository root could not be located.")


def run_pytest(cwd: Path, test_path: str, config_file: str) -> None:
    """Run pytest with --spark-mutate and --spark-mutate-config."""
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        test_path,
        "-v",
        "--spark-mutate",
        f"--spark-mutate-config={config_file}",
    ]
    env = dict(os.environ)
    print(f"Running: {' '.join(cmd)} (cwd={cwd})")
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    # Output is captured; min_mutation_score gate may exit non-zero.
    print(proc.stdout)


def load_report(report_path: Path, run_name: str) -> dict:
    if not report_path.is_file():
        print(
            f"ERROR: {run_name} run failed to write report at {report_path}.\n"
            "This indicates the run crashed or aborted before writing reports, "
            "rather than cleanly tripping the min_mutation_score gate.",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(
            f"ERROR: Could not parse {run_name} report at {report_path}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)


def main() -> int:
    repo_root = find_repo_root()
    pipeline_dir = repo_root / "examples" / "pyspark-pipeline"

    weak_report_path = (
        pipeline_dir / "target" / "weak-reports" / "mutation-report.json"
    )
    hardened_report_path = (
        pipeline_dir / "target" / "hardened-reports" / "mutation-report.json"
    )

    # Clean old report files before execution to guarantee freshness
    if weak_report_path.exists():
        weak_report_path.unlink()
    if hardened_report_path.exists():
        hardened_report_path.unlink()

    # Step 1: Run weak suite
    print("=" * 70)
    print("STAGE 1: Running weak test suite under mutation")
    print("=" * 70)
    run_pytest(pipeline_dir, "tests/test_orders_weak.py", "weak.toml")

    # Step 2: Run hardened suite
    print("=" * 70)
    print("STAGE 2: Running hardened test suite under mutation")
    print("=" * 70)
    run_pytest(pipeline_dir, "tests/test_orders_hardened.py", "hardened.toml")

    # Step 3: Load and validate reports
    weak_report = load_report(weak_report_path, "Weak")
    hardened_report = load_report(hardened_report_path, "Hardened")

    weak_mutants = {m["mutantId"]: m for m in weak_report.get("mutants", [])}
    hardened_mutants = {
        m["mutantId"]: m for m in hardened_report.get("mutants", [])
    }

    # Step 4: Compare results
    survived_in_weak_killed_in_hardened = []
    comparison_rows = []

    # All mutant IDs across both
    all_ids = sorted(set(weak_mutants) | set(hardened_mutants))

    for mid in all_ids:
        w_entry = weak_mutants.get(mid)
        h_entry = hardened_mutants.get(mid)

        op = (w_entry or h_entry).get("operatorType", "UNKNOWN")
        desc = (w_entry or h_entry).get("description", "Unknown")

        w_status = w_entry["result"]["status"] if w_entry else "MISSING"
        h_status = h_entry["result"]["status"] if h_entry else "MISSING"

        comparison_rows.append((mid, op, desc, w_status, h_status))

        if w_status == "SURVIVED" and h_status == "KILLED":
            survived_in_weak_killed_in_hardened.append((mid, op, desc))

    # Print comparison table
    print("\n" + "=" * 70)
    print("MUTATION COMPARISON TABLE")
    print("=" * 70)
    header = f"{'Mutant ID':<18} {'Operator':<10} {'Weak Status':<14} {'Hardened Status':<16} {'Description'}"
    print(header)
    print("-" * len(header))
    for mid, op, desc, w_status, h_status in comparison_rows:
        print(f"{mid:<18} {op:<10} {w_status:<14} {h_status:<16} {desc}")
    print("=" * 70 + "\n")

    # Step 5: Assert proof requirements
    join_proved = [
        item for item in survived_in_weak_killed_in_hardened if item[1] == "JOIN"
    ]
    filter_proved = [
        item for item in survived_in_weak_killed_in_hardened if item[1] == "FILTER"
    ]

    print(
        f"Mutants proving survival -> kill transition: {len(survived_in_weak_killed_in_hardened)}"
    )
    for mid, op, desc in survived_in_weak_killed_in_hardened:
        print(f"  [{op}] {mid}: {desc} (SURVIVED in weak -> KILLED in hardened)")

    errors = []
    if not join_proved:
        errors.append(
            "Expected at least one JOIN mutant to be SURVIVED in weak and KILLED in hardened, but found none."
        )
    if not filter_proved:
        errors.append(
            "Expected at least one FILTER mutant to be SURVIVED in weak and KILLED in hardened, but found none."
        )

    if errors:
        print("\nVERIFICATION FAILED:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    print(
        "\nVERIFICATION PASSED: Both JOIN and FILTER survival->kill proofs verified successfully."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
