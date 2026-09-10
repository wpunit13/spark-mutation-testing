"""Tests for the pytest plugin's baseline pass and fail-fast mutation loop.

Every nested pytest run is in-process (pytester). MutatorBridge, the
session/JVM accessors, the shim-jar filename resolution and the
importlib.resources jar resolution are all monkeypatched to in-memory fakes:
no real Spark, no real JVM, no real jar.
"""

from __future__ import annotations

import importlib.resources
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pytest_spark_mutator import plugin as plugin_module
from pytest_spark_mutator.exceptions import UnsupportedSparkVersionError

pytest_plugins = ["pytester"]

MUTANT_A = "a" * 16
MUTANT_B = "b" * 16


# ---------------------------------------------------------------------------
# In-memory fakes
# ---------------------------------------------------------------------------


class FakeBridge:
    """In-memory MutatorBridge stand-in recording every call, in order."""

    def __init__(self, events):
        self.events = events
        self.catalog = []
        self.mapped_tests = {}

    def _record(self, name, *args):
        self.events.append(("bridge", name, *args))

    def get_full_catalog(self):
        self._record("get_full_catalog")
        return [dict(entry) for entry in self.catalog]

    def get_mapped_test_ids(self, mutant_id):
        self._record("get_mapped_test_ids", mutant_id)
        return list(self.mapped_tests.get(mutant_id, []))

    def set_active_mutant(self, mutant_id):
        self._record("set_active_mutant", mutant_id)

    def clear_active_mutant(self, mutant_id):
        self._record("clear_active_mutant", mutant_id)

    def reset_session_state(self, jsparksession, since_timestamp_millis):
        self._record("reset_session_state", since_timestamp_millis)
        return {}

    def record_outcome(self, mutant_id, status, elapsed_millis, failure_detail):
        self._record("record_outcome", mutant_id, status, elapsed_millis, failure_detail)

    def finalize_reports(self):
        self._record("finalize_reports")
        return "/fake/spark-mutator-report"


class FakeSparkContext:
    def __init__(self, events):
        self.events = events

    def setJobGroup(self, group_id, description, interrupt_on_cancel):
        self.events.append(
            ("spark", "setJobGroup", group_id, description, interrupt_on_cancel)
        )

    def clearJobGroup(self):
        self.events.append(("spark", "clearJobGroup"))


class FakeSpark:
    def __init__(self, events):
        self.sparkContext = FakeSparkContext(events)
        self._jsparkSession = object()


@pytest.fixture
def mutator_fakes(monkeypatch, tmp_path):
    """Install every in-memory fake the nested pytest runs need.

    The real ``_resolve_jar_path`` runs, backed by a fake
    ``importlib.resources.files`` traversable that pretends the bundled jar
    exists; individual tests re-patch pieces they need to behave differently.
    """
    events = []
    fake = SimpleNamespace(
        events=events,
        bridge=FakeBridge(events),
        spark=FakeSpark(events),
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
    return fake


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_nested(pytester, monkeypatch, *args):
    # Load the plugin explicitly and disable entry-point autoload so the
    # plugin is registered exactly once whether or not the wheel happens to
    # be installed in the running environment.
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


def _events_named(fake, name):
    return [ev for ev in fake.events if ev[1] == name]


def _combined_output(result):
    # RunResult.stdout/.stderr are LineMatcher objects in pytest 7.x; join
    # the raw outline lists for substring assertions.
    return "\n".join(result.outlines + result.errlines)


def _index_of_event(events, name, mutant_id=None, start=0):
    for i in range(start, len(events)):
        ev = events[i]
        if ev[1] != name:
            continue
        if mutant_id is None or ev[2] == mutant_id:
            return i
    raise AssertionError(f"no {name!r} event found (mutant_id={mutant_id!r})")


# ---------------------------------------------------------------------------
# Required scenarios
# ---------------------------------------------------------------------------


def test_inactive_run_is_a_total_no_op(pytester, mutator_fakes, monkeypatch):
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    result = _run_nested(pytester, monkeypatch)
    assert result.ret == 0
    # No env var writes...
    assert "PYSPARK_SUBMIT_ARGS" not in os.environ
    # ...no bridge calls at all...
    assert mutator_fakes.events == []
    # ...and no spark-mutator output.
    assert "spark-mutator mutation summary" not in _combined_output(result)


def test_inactive_run_preserves_existing_submit_args(
    pytester, mutator_fakes, monkeypatch
):
    monkeypatch.setenv("PYSPARK_SUBMIT_ARGS", "--existing user args pyspark-shell")
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    result = _run_nested(pytester, monkeypatch)
    assert result.ret == 0
    assert os.environ["PYSPARK_SUBMIT_ARGS"] == "--existing user args pyspark-shell"


def test_baseline_failure_aborts_before_any_mutation(
    pytester, mutator_fakes, monkeypatch
):
    pytester.makepyfile(test_app="def test_alpha():\n    assert False\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "JOIN", 0)]
    mutator_fakes.bridge.mapped_tests = {MUTANT_A: ["test_app.py::test_alpha"]}
    result = _run_nested(pytester, monkeypatch, "--spark-mutate")
    assert result.ret != 0
    # The hard abort: no mutation logic ran at all.
    assert _events_named(mutator_fakes, "set_active_mutant") == []
    assert _events_named(mutator_fakes, "record_outcome") == []
    assert _events_named(mutator_fakes, "finalize_reports") == []
    assert "baseline suite failed" in _combined_output(result)


def test_baseline_success_runs_every_mutant(pytester, mutator_fakes, monkeypatch):
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    config_path = _write_config(pytester, "min_mutation_score = 0.0\n")
    mutator_fakes.bridge.catalog = [
        _entry(MUTANT_A, "JOIN", 0),
        _entry(MUTANT_B, "FILTER", 2),
    ]
    mutator_fakes.bridge.mapped_tests = {
        MUTANT_A: ["test_app.py::test_alpha"],
        MUTANT_B: ["test_app.py::test_alpha"],
    }
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    set_ids = [ev[2] for ev in _events_named(mutator_fakes, "set_active_mutant")]
    assert set_ids == [MUTANT_A, MUTANT_B]
    outcomes = _events_named(mutator_fakes, "record_outcome")
    assert len(outcomes) == 2
    assert {ev[2] for ev in outcomes} == {MUTANT_A, MUTANT_B}
    assert all(ev[3] == "SURVIVED" for ev in outcomes)
    # Jar mounting happened exactly once, with the required trailing token.
    submit_args = os.environ["PYSPARK_SUBMIT_ARGS"]
    assert str(mutator_fakes.jar_path) in submit_args
    assert (
        "--conf spark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension"
        in submit_args
    )
    assert submit_args.count("pyspark-shell") == 1
    assert submit_args.endswith("pyspark-shell")
    # Baseline-phase test-context tagging happened once per baseline test.
    tracker = mutator_fakes.jvm.io.github.wpunit13.mutator.TestContextTracker
    assert tracker.setCurrentTestId.call_count == 1
    assert tracker.clearCurrentTestId.call_count == 1


def test_submit_args_prepended_and_single_trailing_shell_token(
    pytester, mutator_fakes, monkeypatch
):
    monkeypatch.setenv(
        "PYSPARK_SUBMIT_ARGS", "--conf spark.foo=bar pyspark-shell"
    )
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
    expected = (
        f"--jars {mutator_fakes.jar_path} "
        "--conf spark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension "
        "--conf spark.foo=bar pyspark-shell"
    )
    assert os.environ["PYSPARK_SUBMIT_ARGS"] == expected


_FAILFAST_SUITE = (
    "import os\n"
    "from pathlib import Path\n"
    "\n"
    "_MARKER = Path(os.environ['SPARK_MUTATOR_MARKER_FILE'])\n"
    "\n"
    "\n"
    "def _record(name):\n"
    "    with open(_MARKER, 'a', encoding='utf-8') as fh:\n"
    "        fh.write(name + '\\n')\n"
    "\n"
    "\n"
    "def _under_mutation():\n"
    "    import pytest_spark_mutator.plugin as plugin\n"
    "\n"
    "    session = plugin._active_session\n"
    "    return session is not None and session.phase == 'mutation'\n"
    "\n"
    "\n"
    "def test_first():\n"
    "    _record('first')\n"
    "    if _under_mutation():\n"
    "        raise AssertionError('mutant killed by test_first')\n"
    "\n"
    "\n"
    "def test_second():\n"
    "    _record('second')\n"
    "\n"
    "\n"
    "def test_third():\n"
    "    _record('third')\n"
)


def test_fail_fast_stops_after_first_failing_mapped_test(
    pytester, mutator_fakes, monkeypatch, tmp_path
):
    marker_file = tmp_path / "markers.txt"
    monkeypatch.setenv("SPARK_MUTATOR_MARKER_FILE", str(marker_file))
    pytester.makepyfile(test_app=_FAILFAST_SUITE)
    config_path = _write_config(pytester, "min_mutation_score = 0.0\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "FILTER", 0)]
    mutator_fakes.bridge.mapped_tests = {
        MUTANT_A: [
            "test_app.py::test_first",
            "test_app.py::test_second",
            "test_app.py::test_third",
        ]
    }
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    outcomes = _events_named(mutator_fakes, "record_outcome")
    assert len(outcomes) == 1
    assert outcomes[0][2] == MUTANT_A
    assert outcomes[0][3] == "KILLED"
    # Baseline ran all three tests; the mutation loop ran only the first
    # (which failed) and never executed the second or third.
    markers = marker_file.read_text(encoding="utf-8").splitlines()
    assert markers == ["first", "second", "third", "first"]


_SETUP_FAILURE_SUITE = (
    "import pytest\n"
    "\n"
    "def _under_mutation():\n"
    "    import pytest_spark_mutator.plugin as plugin\n"
    "    session = plugin._active_session\n"
    "    return session is not None and session.phase == 'mutation'\n"
    "\n"
    "@pytest.fixture\n"
    "def data():\n"
    "    if _under_mutation():\n"
    "        # The mutation broke fixture setup: the test body never runs.\n"
    "        raise AssertionError('mutant broke the fixture')\n"
    "    return 42\n"
    "\n"
    "def test_uses_data(data):\n"
    "    assert data == 42\n"
)


def test_setup_phase_failure_classifies_killed(
    pytester, mutator_fakes, monkeypatch
):
    # A mutation that breaks a fixture is detected in the SETUP phase, not the
    # call phase. Before the fix this was misclassified SURVIVED; it must be
    # KILLED, exactly like a call-phase assertion failure.
    pytester.makepyfile(test_app=_SETUP_FAILURE_SUITE)
    config_path = _write_config(pytester, "min_mutation_score = 0.0\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "JOIN", 0)]
    mutator_fakes.bridge.mapped_tests = {
        MUTANT_A: ["test_app.py::test_uses_data"]
    }
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    outcomes = _events_named(mutator_fakes, "record_outcome")
    assert len(outcomes) == 1
    assert outcomes[0][2] == MUTANT_A
    assert outcomes[0][3] == "KILLED"


def test_all_mapped_tests_passing_classifies_survived(
    pytester, mutator_fakes, monkeypatch
):
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    config_path = _write_config(pytester, "min_mutation_score = 0.0\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "JOIN", 1)]
    mutator_fakes.bridge.mapped_tests = {MUTANT_A: ["test_app.py::test_alpha"]}
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    outcomes = _events_named(mutator_fakes, "record_outcome")
    assert len(outcomes) == 1
    ev = outcomes[0]
    assert ev[2] == MUTANT_A
    assert ev[3] == "SURVIVED"
    assert ev[5] is None
    assert isinstance(ev[4], int) and ev[4] >= 0


def test_excluded_mutators_are_filtered_out(pytester, mutator_fakes, monkeypatch):
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    config_path = _write_config(
        pytester,
        'excluded_mutators = ["CrossJoinMutator"]\nmin_mutation_score = 0.0\n',
    )
    mutator_fakes.bridge.catalog = [
        _entry(MUTANT_A, "JOIN", 1),  # CrossJoinMutator — excluded
        _entry(MUTANT_B, "JOIN", 0),  # JoinTypeToLeftOuterMutator — kept
    ]
    mutator_fakes.bridge.mapped_tests = {MUTANT_B: ["test_app.py::test_alpha"]}
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    set_ids = [ev[2] for ev in _events_named(mutator_fakes, "set_active_mutant")]
    assert set_ids == [MUTANT_B]
    mapped_ids = [ev[2] for ev in _events_named(mutator_fakes, "get_mapped_test_ids")]
    assert mapped_ids == [MUTANT_B]


def test_mutant_without_mapped_tests_is_skipped(pytester, mutator_fakes, monkeypatch):
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    config_path = _write_config(pytester, "min_mutation_score = 0.0\n")
    mutator_fakes.bridge.catalog = [_entry(MUTANT_A, "JOIN", 0)]
    mutator_fakes.bridge.mapped_tests = {}  # get_mapped_test_ids -> []
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    outcomes = _events_named(mutator_fakes, "record_outcome")
    assert len(outcomes) == 1
    assert outcomes[0][2] == MUTANT_A
    assert outcomes[0][3] == "SKIPPED"
    assert outcomes[0][5] == "no mapped tests"
    assert _events_named(mutator_fakes, "set_active_mutant") == []


def _setup_two_mutant_run(pytester, mutator_fakes):
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    config_path = _write_config(pytester, "min_mutation_score = 0.0\n")
    mutator_fakes.bridge.catalog = [
        _entry(MUTANT_A, "JOIN", 0),
        _entry(MUTANT_B, "FILTER", 2),
    ]
    mutator_fakes.bridge.mapped_tests = {
        MUTANT_A: ["test_app.py::test_alpha"],
        MUTANT_B: ["test_app.py::test_alpha"],
    }
    return config_path


def test_reset_sequence_runs_in_required_order(pytester, mutator_fakes, monkeypatch):
    config_path = _setup_two_mutant_run(pytester, mutator_fakes)
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    events = mutator_fakes.events
    for mutant_id in (MUTANT_A, MUTANT_B):
        idx_set = _index_of_event(events, "set_active_mutant", mutant_id)
        idx_job = _index_of_event(events, "setJobGroup", mutant_id)
        idx_clear_job = _index_of_event(events, "clearJobGroup", start=idx_job + 1)
        idx_reset = _index_of_event(events, "reset_session_state", start=idx_clear_job + 1)
        idx_clear = _index_of_event(
            events, "clear_active_mutant", mutant_id, start=idx_reset + 1
        )
        # §3.2 ordering: clearJobGroup -> reset_session_state ->
        # clear_active_mutant (strictly, in that order, per mutant).
        assert idx_set < idx_job < idx_clear_job < idx_reset < idx_clear
    # The next mutant is only activated after the previous one is cleared,
    # so a racing async job can never pick up the wrong mutant id.
    idx_clear_a = _index_of_event(events, "clear_active_mutant", MUTANT_A)
    idx_set_b = _index_of_event(events, "set_active_mutant", MUTANT_B)
    assert idx_clear_a < idx_set_b


def test_finalize_reports_called_once_after_all_mutants(
    pytester, mutator_fakes, monkeypatch
):
    config_path = _setup_two_mutant_run(pytester, mutator_fakes)
    result = _run_nested(
        pytester,
        monkeypatch,
        "--spark-mutate",
        "--spark-mutate-config",
        str(config_path),
    )
    assert result.ret == 0
    events = mutator_fakes.events
    finalize_events = _events_named(mutator_fakes, "finalize_reports")
    assert len(finalize_events) == 1
    # Called once, at the very end, after every record_outcome.
    assert events[-1] == ("bridge", "finalize_reports")
    last_outcome = max(
        i for i, ev in enumerate(events) if ev[1] == "record_outcome"
    )
    assert last_outcome < len(events) - 1


def test_ci_gate_fails_when_score_below_minimum(
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
    # One SURVIVED mutant -> score 0.0 < 100.0 -> non-zero exit status.
    assert result.ret != 0
    assert "below min_mutation_score" in _combined_output(result)


def test_ci_gate_passes_when_score_meets_minimum(
    pytester, mutator_fakes, monkeypatch
):
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


def test_unsupported_spark_version_fast_fails(pytester, mutator_fakes, monkeypatch):
    def _raise(pyspark_version=None, jars_dir=None):
        raise UnsupportedSparkVersionError(
            "Unsupported Spark/Scala combination '9.9_2.13'. "
            "Supported versions: ['3.5_2.13']"
        )

    monkeypatch.setattr(plugin_module, "resolve_shim_jar_filename", _raise)
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    result = _run_nested(pytester, monkeypatch, "--spark-mutate")
    assert result.ret != 0
    combined = _combined_output(result)
    assert "Unsupported Spark/Scala combination" in combined
    # The fast-fail guard aborts before any mutation logic runs.
    assert _events_named(mutator_fakes, "set_active_mutant") == []


def test_missing_bundled_jar_raises_usage_error(pytester, mutator_fakes, monkeypatch):
    def _raise(package):
        raise FileNotFoundError(f"no such package or file: {package}")

    monkeypatch.setattr(importlib.resources, "files", _raise)
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    result = _run_nested(pytester, monkeypatch, "--spark-mutate")
    assert result.ret != 0
    combined = _combined_output(result)
    assert "bundled shim jar" in combined
    assert "interceptor-spark-3.5_2.13.jar" in combined
    assert _events_named(mutator_fakes, "set_active_mutant") == []


def test_missing_jars_package_raises_usage_error(
    pytester, mutator_fakes, monkeypatch
):
    # The real-world absence: pytest_spark_mutator.jars does not exist at all,
    # so importlib.resources.files raises ModuleNotFoundError.
    def _raise(package):
        raise ModuleNotFoundError(f"No module named {package!r}", name=package)

    monkeypatch.setattr(importlib.resources, "files", _raise)
    pytester.makepyfile(test_app="def test_alpha():\n    assert True\n")
    result = _run_nested(pytester, monkeypatch, "--spark-mutate")
    assert result.ret != 0
    combined = _combined_output(result)
    assert "bundled shim jar" in combined
    assert _events_named(mutator_fakes, "set_active_mutant") == []