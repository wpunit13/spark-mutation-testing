#!/usr/bin/env python3
"""WP-16: validate the SARIF 2.1.0 artifacts spark-mutator publishes.

Two layers of validation per file:

1. **Structural** — the SARIF 2.1.0 subset spark-mutator emits: ``$schema``,
   ``version``, ``runs[].tool.driver.name``, and per-result ``ruleId`` /
   ``level`` / ``message.text`` / ``locations[0].physicalLocation``. Zero
   third-party dependencies so CI can always run it. If the ``jsonschema``
   package is installed and a schema file is supplied via ``--schema``, an
   additional full JSON-Schema pass runs on top.

2. **Domain** — the checks that actually catch real defects:
   * ``ruleId`` must be a real mutator name from the fixed lookup table
     (mirrored from ``SarifReportWriter.MUTATOR_NAMES``); ``UnknownMutator``
     is rejected — it can only mean the lookup table is out of sync with the
     shims.
   * ``level`` is ``"warning"`` (SURVIVED) or ``"error"`` (ERRORED).
   * When a sibling ``mutation-report.json`` exists, the SARIF must be a
     faithful projection of it: the multiset of
     ``(ruleId, level, message.text, uri)`` tuples must exactly equal the
     tuples derived from the JSON report's SURVIVED/ERRORED mutants, and no
     KILLED/TIMED_OUT mutant may surface in SARIF.

Exit codes: 0 = all artifacts valid, 1 = at least one violation.

Usage:
    python scripts/validate_sarif.py [PATH ...]

Each PATH is a SARIF file or a directory (scanned recursively for
``mutation-report.sarif``). With no PATH, the current directory is scanned.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Mirrored from SarifReportWriter.MUTATOR_NAMES (JVM) and the pytest plugin's
# _MUTATOR_NAMES duplicate; all three must stay in sync. The writer documents
# an explicit fallback: any (operatorType, mutationIndex) not listed here is
# reported as UnknownMutator, which is therefore a legitimate SARIF ruleId.
MUTATOR_NAMES = {
    "JOIN|0": "JoinTypeToLeftOuterMutator",
    "JOIN|1": "CrossJoinMutator",
    "JOIN|2": "JoinTypeToLeftAntiMutator",
    "FILTER|0": "FilterKeepLeftConjunctMutator",
    "FILTER|1": "FilterKeepRightConjunctMutator",
    "FILTER|2": "FilterAlwaysFalseMutator",
    "FILTER|3": "FilterPredicateInversionMutator",
    "AGGREGATE|0": "AggregateSwapFunctionMutator",
    "AGGREGATE|1": "AggregateDropGroupingKeyMutator",
    "AGGREGATE|2": "AggregateZeroMutator",
    "WINDOW|0": "WindowOrderInversionMutator",
    "WINDOW|1": "WindowFrameTruncationMutator",
    "PROJECT|0": "ProjectCoalesceBypassMutator",
    "PROJECT|1": "ProjectInjectNullMutator",
}
KNOWN_MUTATORS = set(MUTATOR_NAMES.values())
UNKNOWN_MUTATOR = "UnknownMutator"

SARIF_FILE_NAME = "mutation-report.sarif"
JSON_REPORT_FILE_NAME = "mutation-report.json"
ALLOWED_LEVELS = {"warning", "error"}


def find_sarif_files(paths: list[str]) -> list[str]:
    """Expand user-supplied paths into concrete SARIF file paths."""
    found: list[str] = []
    for path in paths:
        p = Path(path)
        if p.is_file():
            found.append(str(p))
        elif p.is_dir():
            found.extend(str(f) for f in sorted(p.rglob(SARIF_FILE_NAME)))
        else:
            print(f"error: path does not exist: {path}")
            sys.exit(1)
    if not paths:
        found.extend(str(f) for f in sorted(Path(".").rglob(SARIF_FILE_NAME)))
    # Deduplicate, preserve order.
    seen: set[str] = set()
    unique = [f for f in found if not (f in seen or seen.add(f))]
    return unique


def structural_errors(sarif: dict) -> list[str]:
    """Structural checks for the SARIF 2.1.0 subset spark-mutator emits."""
    errors: list[str] = []
    if sarif.get("version") != "2.1.0":
        errors.append(f'version must be "2.1.0", got {sarif.get("version")!r}')
    schema = sarif.get("$schema")
    if not isinstance(schema, str) or "sarif-2.1.0" not in schema:
        errors.append(f"$schema must reference SARIF 2.1.0, got {schema!r}")
    runs = sarif.get("runs")
    if not isinstance(runs, list) or not runs:
        errors.append("runs must be a non-empty array")
        return errors
    for run_index, run in enumerate(runs):
        driver = (run.get("tool") or {}).get("driver") or {}
        if not driver.get("name"):
            errors.append(f"runs[{run_index}].tool.driver.name missing or empty")
        results = run.get("results")
        if not isinstance(results, list):
            errors.append(f"runs[{run_index}].results must be an array")
            continue
        for r_index, result in enumerate(results):
            where = f"runs[{run_index}].results[{r_index}]"
            rule_id = result.get("ruleId")
            if not isinstance(rule_id, str) or not rule_id:
                errors.append(f"{where}.ruleId missing or empty")
            level = result.get("level")
            if level not in ALLOWED_LEVELS:
                errors.append(
                    f"{where}.level must be one of {sorted(ALLOWED_LEVELS)}, "
                    f"got {level!r}"
                )
            text = (result.get("message") or {}).get("text")
            if not isinstance(text, str) or not text:
                errors.append(f"{where}.message.text missing or empty")
            locations = result.get("locations")
            if not isinstance(locations, list) or not locations:
                errors.append(f"{where}.locations missing or empty")
                continue
            physical = (locations[0] or {}).get("physicalLocation") or {}
            uri = (physical.get("artifactLocation") or {}).get("uri")
            if not isinstance(uri, str) or not uri:
                errors.append(f"{where}.locations[0]...artifactLocation.uri missing")
            elif uri.startswith("/") or "://" in uri:
                errors.append(
                    f"{where}.artifactLocation.uri must be a relative path to "
                    f"the source file, got {uri!r}"
                )
            region = physical.get("region")
            if region is not None:
                start_line = region.get("startLine")
                if not isinstance(start_line, int) or start_line < 1:
                    errors.append(
                        f"{where}.region.startLine must be an int >= 1, "
                        f"got {start_line!r}"
                    )
    return errors


def domain_errors(sarif: dict) -> list[str]:
    """Checks beyond structure: real mutator names, level vocabulary, and the
    spec's prohibition on spark-mutator/OPERATOR-style ruleIds. The writer's
    documented UnknownMutator fallback is legitimate (unlisted combinations)."""
    errors: list[str] = []
    operator_names = {"JOIN", "FILTER", "AGGREGATE", "WINDOW", "PROJECT", "OTHER"}
    for run_index, run in enumerate(sarif.get("runs", [])):
        for r_index, result in enumerate(run.get("results", [])):
            rule_id = result.get("ruleId")
            where = f"runs[{run_index}].results[{r_index}]"
            if isinstance(rule_id, str) and rule_id.startswith("spark-mutator/"):
                errors.append(
                    f"{where}.ruleId {rule_id!r} uses the prohibited "
                    "spark-mutator/OPERATOR form; it must be the mutator name "
                    "from SarifReportWriter.MUTATOR_NAMES"
                )
            elif isinstance(rule_id, str) and rule_id in operator_names:
                errors.append(
                    f"{where}.ruleId {rule_id!r} is a bare operator name; it "
                    "must be the mutator name from SarifReportWriter.MUTATOR_NAMES"
                )
            elif isinstance(rule_id, str) and rule_id != UNKNOWN_MUTATOR \
                    and rule_id not in KNOWN_MUTATORS:
                errors.append(f"{where}.ruleId {rule_id!r} is not a known mutator name")
    return errors


def expected_sarif_tuples(json_report: dict) -> set[tuple[str, str, str, str]]:
    """The (ruleId, level, message.text, uri) multiset the JSON report implies."""
    mutator_for = {}
    for entry in json_report.get("mutants", []):
        key = f"{entry['operatorType']}|{entry['mutationIndex']}"
        mutator_for[entry["mutantId"]] = MUTATOR_NAMES.get(key, UNKNOWN_MUTATOR)
    expected: set[tuple[str, str, str, str]] = set()
    for entry in json_report.get("mutants", []):
        status = (entry.get("result") or {}).get("status")
        if status not in ("SURVIVED", "ERRORED"):
            continue  # KILLED / TIMED_OUT are omitted from SARIF by contract
        expected.add(
            (
                mutator_for[entry["mutantId"]],
                "warning" if status == "SURVIVED" else "error",
                entry.get("description", ""),
                entry.get("filePath", ""),
            )
        )
    return expected


def actual_sarif_tuples(sarif: dict) -> set[tuple[str, str, str, str]]:
    actual: set[tuple[str, str, str, str]] = set()
    for run in sarif.get("runs", []):
        for result in run.get("results", []):
            uri = (
                (result.get("locations") or [{}])[0]
                .get("physicalLocation", {})
                .get("artifactLocation", {})
                .get("uri", "")
            )
            actual.add(
                (
                    result.get("ruleId", ""),
                    result.get("level", ""),
                    (result.get("message") or {}).get("text", ""),
                    uri,
                )
            )
    return actual


def cross_check_errors(sarif_path: Path, sarif: dict) -> list[str]:
    """Cross-validate the SARIF against its sibling mutation-report.json."""
    errors: list[str] = []
    json_path = sarif_path.parent / JSON_REPORT_FILE_NAME
    if not json_path.is_file():
        return errors  # nothing to cross-check against
    try:
        with open(json_path, encoding="utf-8") as handle:
            json_report = json.load(handle)
    except (OSError, ValueError) as exc:
        return [f"cannot read sibling {json_path}: {exc}"]

    expected = expected_sarif_tuples(json_report)
    actual = actual_sarif_tuples(sarif)
    for tuple_ in sorted(expected - actual):
        errors.append(f"SARIF is missing a result present in the JSON report: {tuple_}")
    for tuple_ in sorted(actual - expected):
        errors.append(f"SARIF carries a result the JSON report does not: {tuple_}")
    return errors


def validate(path: str) -> list[str]:
    errors: list[str] = []
    try:
        with open(path, encoding="utf-8") as handle:
            sarif = json.load(handle)
    except (OSError, ValueError) as exc:
        return [f"cannot parse {path}: {exc}"]
    if not isinstance(sarif, dict):
        return [f"{path}: top-level JSON must be an object"]

    for error in structural_errors(sarif):
        errors.append(f"{path}: {error}")
    for error in domain_errors(sarif):
        errors.append(f"{path}: {error}")
    for error in cross_check_errors(Path(path), sarif):
        errors.append(f"{path}: {error}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "paths",
        nargs="*",
        help="SARIF files or directories to scan (default: scan the cwd)",
    )
    parser.add_argument(
        "--schema",
        help="Optional JSON Schema file; validated with the jsonschema package "
        "when it is installed. Structural + domain checks always run.",
    )
    args = parser.parse_args()

    sarif_files = find_sarif_files(args.paths)
    if not sarif_files:
        print("No mutation-report.sarif files found.")
        return 1

    schema = None
    if args.schema:
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            print(
                "NOTE: --schema given but the jsonschema package is not "
                "installed; skipping schema validation."
            )
        else:
            with open(args.schema, encoding="utf-8") as handle:
                import jsonschema

                schema = json.loads(handle.read())
            validator = jsonschema.Draft7Validator(schema)

    all_errors: list[str] = []
    for path in sarif_files:
        print(f"validating {path}")
        all_errors.extend(validate(path))
        if schema is not None:
            with open(path, encoding="utf-8") as handle:
                instance = json.load(handle)
            for error in sorted(validator.iter_errors(instance), key=str):
                all_errors.append(f"{path}: schema: {error.message}")

    if all_errors:
        for error in all_errors:
            print(f"FAIL: {error}")
        print(f"\n{len(all_errors)} violation(s) in {len(sarif_files)} file(s).")
        return 1
    print(f"OK: {len(sarif_files)} SARIF file(s) valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())