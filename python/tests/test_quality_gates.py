"""WP-16 quality-gate tests: the pytest exit-code contract.

The mutation quality gate is deliberately distinct from test failures:

* ``0``  — run clean, score met the threshold.
* ``1``  — test assertion failures (pytest's own TESTS_FAILED) or a red
           baseline; the gate did not fire.
* ``2``  — quality-gate breach: ``mutation score < min_mutation_score``.

Exit code 2 collides with pytest's own "interrupted by user" semantics; the
WP-16 spec accepts the collision because the gate message and the written
reports disambiguate.

Every nested pytest run is in-process (pytester) with in-memory fakes —
no real Spark, no real JVM, no real jar. The fakes mirror the ones in
``test_plugin_protocol.py``; the suite is kept self-contained so the exit-code
contract can be read (and broken) in one file.
"""

from __future__ import annotations

import importlib.resources
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pytest_spark_mutator import plugin as plugin_module

pytest_plugins = ["pytester"]

MUTANT_A = "a" * 16


# ---------------------------------------------------------------------------
# In-memory fakes (mirrors test_plugin_protocol.py's mutator_fakes)
# ---------------------------------------------------------------------------


class FakeBridge:
    def __init__(self):
        self.catalog = []
        self.mapped_tests = {}

    def get_full_catalog(self):
        return [dict(entry) for entry in self.catalog]

    def get_mapped_test_ids(self, mutant_id):
        return list(self.mapped_tests.get(mutant_id, []))

    def set_active_mutant(self, mutant_id):
        pass

    def clear_active_mutant(self, mutant_id):
        pass

    def reset_session_state(self, jsparksession, since_timestamp_millis):
        return {}

    def record_outcome(self, mutant_id, status, elapsed_millis, failure_detail):
        pass

    def finalize_reports(self):
        return "/fake/spark-mutator-report"


class FakeSparkContext:
    def setJobGroup(self, group_id, description, interrupt_on_cancel):
        pass

    def clearJobGroup(self):
        pass


class FakeSpark:
    def __init__(self):
        self.sparkContext = FakeSparkContext()
        self._jsparkSession = object()


@pytest.fixture
def mutator_fakes(monkeypatch, tmp_path):
    fake = SimpleNamespace(
        bridge=FakeBridge(),
        spark=FakeSpark(),
        jvm=MagicMock(),
        jar_path=tmp_path / "fake-shim.jar",
    )

    class _FakeJarTraversable:
        def joinpath(self, name):
            return self

        def is_file(self):
            return True

        def __str__(self):
            return str(fake.jar_path)

    monkeypatch.setattr(plugin_module, "MutatorBridge", lambda jvm: fake.bridge)
    monkeypatch.setattr(
        plugin_module, "get_or_create_session", lambda config: fake.spark
    )
    monkeypatch.setattr(plugin_module, "get_jvm", lambda spark: fake.jvm)
    monkeypatch.setattr(
        plugin_module,
        "resolve_shim_jar_filename",
        lambda pyspark_version=None, jars_dir=None: "interceptor-spark-3.5_2.13.jar",
    )
    monkeypatch.setattr(
        importlib.resources, "files", lambda package: _FakeJarTraversable()
    )
    monkeypatch.delenv("PYSPARK_SUBMIT_ARGS", raising=False)
    monkeypatch.delenv("SPARK_MUTATOR_OUTPUT_DIR", raising=False)
    return fake


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_nested(pytester, monkeypatch, *args):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    return pytester.runpytest("-p", "pytest_spark_mutator.plugin", *args)


def _write_config(pytester, body):
    path = pytester.path / "pyproject.toml"
    path.write_text("[tool.spark-mutator]\n" + body, encoding="utf-8")
    return path


def _entry(mutant_id, operator_type, mutation_index):
    # Shape per MutationCatalogAccess.getFullCatalogJson (JVM contract).
    return {
        "mutantId": mutant_id,
        "filePath": "pipeline.py",
        "lineNumber": 7,
        "operatorType": operator_type,
        "mutationIndex": mutation_index,
        "description": "synthetic",
        "coordinateHex": "0" * 16,
        "mappedTestIds": [],
    }


def _combined_output(result):
    return "\n".join(result.outlines + result.errlines)


# A mapped test that FAILS only under mutation, so the mutant is KILLED and
# the score reaches 100.0 (needed for the exact-boundary case).
_KILL_SUITE = (
    "import pytest_spark_mutator.plugin as plugin\n"
    "\n"
    "\n"
    "def _under_mutation():\n"
    "    session = plugin._active_session\n"
    "    return session is not None and session.phase == 'mutation'\n"
    "\n"
    "\n"
    "def test_alpha():\n"
    "    assert not _under_mutation()\n"
)


# ---------------------------------------------------------------------------
# The WP-16 exit-code contract
# ---------------------------------------------------------------------------


def test_quality_gate_fails_with_exit_code_2_when_score_below_threshold(
    pytester, mutator_fakes, monkeypatch
):
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    config_path = _write_config(pytester, "min_mutation_score = 100.0\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "JOIN", 0)]
    mutator_fakes.bridge.mapped_tests = {MUTANT_A: ["test_app.py::test_alpha"]}
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    # One SURVIVED mutant -> score 0.0 < 100.0 -> the gate fires with exit 2,
    # NOT pytest's test-failure exit code 1.
    assert result.ret == 2
    assert "below min_mutation_score" in _combined_output(result)


def test_quality_gate_passes_with_exit_code_0_when_score_meets_threshold(
    pytester, mutator_fakes, monkeypatch
):
    # Exact boundary: the mutant is killed under mutation, so the score is
    # 100.0 and the threshold is 100.0 — a meet is a pass, not a breach.
    pytester.makepyfile(test_app=_KILL_SUITE)
    config_path = _write_config(pytester, "min_mutation_score = 100.0\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "JOIN", 0)]
    mutator_fakes.bridge.mapped_tests = {MUTANT_A: ["test_app.py::test_alpha"]}
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    assert "below min_mutation_score" not in _combined_output(result)


def test_quality_gate_passes_when_threshold_is_zero(
    pytester, mutator_fakes, monkeypatch
):
    # Score 0.0 against threshold 0.0: the gate is off (or exactly met) and
    # must never fail the run.
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    config_path = _write_config(pytester, "min_mutation_score = 0.0\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "JOIN", 0)]
    mutator_fakes.bridge.mapped_tests = {MUTANT_A: ["test_app.py::test_alpha"]}
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0


def test_baseline_failure_exits_with_code_1_not_code_2(
    pytester, mutator_fakes, monkeypatch
):
    # Exit code 2 is reserved exclusively for quality-gate breaches. A red
    # baseline is a plain test failure and must keep pytest's exit code 1.
    pytester.makepyfile(test_app="def test_alpha():\n    assert False\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "JOIN", 0)]
    mutator_fakes.bridge.mapped_tests = {MUTANT_A: ["test_app.py::test_alpha"]}
    result = _run_nested(pytester, monkeypatch, "--spark-mutate")
    assert result.ret == 1
    assert "below min_mutation_score" not in _combined_output(result)