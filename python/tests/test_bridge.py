"""Tests for :mod:`pytest_spark_mutator.bridge`.

All tests run against a ``MagicMock`` JVM view — no real JVM, no Spark,
no network.
"""

from unittest.mock import MagicMock

import pytest
from py4j.protocol import Py4JJavaError

from pytest_spark_mutator.bridge import MutatorBridge
from pytest_spark_mutator.exceptions import MutatorJvmError

VALID_ID = "0123456789abcdef"

RESET_JSON = (
    '{"clearedCacheEntries": -1, "unpersistedRddCount": 0, "droppedTempViews": []}'
)


class _FakeGatewayClient:
    """Canned py4j gateway client: ``send_command`` returns a success/void
    answer so ``str(Py4JJavaError)`` resolves without a live JVM."""

    def send_command(self, command):
        return "yv"


class _FakeJavaException:
    """Stand-in for the bridged ``java.lang.Throwable``: Py4JJavaError's
    constructor reads ``_target_id`` and its ``__str__`` reads
    ``_gateway_client``."""

    def __init__(self):
        self._target_id = "o0"
        self._gateway_client = _FakeGatewayClient()


def _make_py4j_error(message="boom"):
    # The scenario text literally says `Py4JJavaError("boom", None)`, but on
    # py4j 0.10.9.9 that constructor dereferences java_exception._target_id
    # and raises AttributeError before the error is ever returned. We build a
    # valid error with a minimal fake Java exception instead; the behavior
    # under test (translation to MutatorJvmError) is identical.
    return Py4JJavaError(message, _FakeJavaException())


def _make_null_java_exception_error(message="boom"):
    # Bypass Py4JJavaError.__init__ (which dereferences _target_id) to model
    # the "null java_exception" shape that older py4j lines could construct
    # directly. This exercises bridge._translate's errmsg fallback when str()
    # cannot reach the JVM.
    err = Py4JJavaError.__new__(Py4JJavaError)
    err.args = (message, None)
    err.errmsg = message
    err.java_exception = None
    return err


@pytest.fixture()
def jvm():
    return MagicMock()


@pytest.fixture()
def bridge(jvm):
    return MutatorBridge(jvm)


# ---------------------------------------------------------------------------
# JVM call paths — one test per bridge method
# ---------------------------------------------------------------------------


def test_set_active_mutant_calls_jvm_path(bridge, jvm):
    bridge.set_active_mutant(VALID_ID)
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.setActiveMutant
    ).assert_called_once_with(VALID_ID)


def test_clear_active_mutant_calls_jvm_path(bridge, jvm):
    bridge.clear_active_mutant(VALID_ID)
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.clearActiveMutant
    ).assert_called_once_with(VALID_ID)


def test_get_active_mutant_or_none_calls_jvm_path(bridge, jvm):
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.getActiveMutantOrNull
    ).return_value = VALID_ID
    assert bridge.get_active_mutant_or_none() == VALID_ID
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.getActiveMutantOrNull
    ).assert_called_once_with()


def test_reset_calls_jvm_path(bridge, jvm):
    bridge.reset()
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.reset
    ).assert_called_once_with()


def test_get_full_catalog_calls_jvm_path_and_parses(bridge, jvm):
    (
        jvm.io.github.wpunit13.mutator.catalog.MutationCatalogAccess.getFullCatalogJson
    ).return_value = '[{"mutantId": "0123456789abcdef", "operatorType": "JOIN"}]'
    result = bridge.get_full_catalog()
    (
        jvm.io.github.wpunit13.mutator.catalog.MutationCatalogAccess.getFullCatalogJson
    ).assert_called_once_with()
    assert result == [{"mutantId": "0123456789abcdef", "operatorType": "JOIN"}]


def test_get_mapped_test_ids_calls_jvm_path_and_parses(bridge, jvm):
    (
        jvm.io.github.wpunit13.mutator.catalog.MutationCatalogAccess.getMappedTestIdsJson
    ).return_value = '["t1", "t2"]'
    result = bridge.get_mapped_test_ids(VALID_ID)
    (
        jvm.io.github.wpunit13.mutator.catalog.MutationCatalogAccess.getMappedTestIdsJson
    ).assert_called_once_with(VALID_ID)
    assert result == ["t1", "t2"]


def test_record_outcome_calls_jvm_path(bridge, jvm):
    bridge.record_outcome(VALID_ID, "KILLED", 100, None)
    (
        jvm.io.github.wpunit13.mutator.report.ReportSink.recordOutcome
    ).assert_called_once_with(VALID_ID, "KILLED", 100, None)


def test_finalize_reports_calls_jvm_path_and_returns_path(bridge, jvm):
    (
        jvm.io.github.wpunit13.mutator.report.ReportSink.finalizeAndWriteReports
    ).return_value = "/tmp/reports/mutation-report.json"
    assert bridge.finalize_reports() == "/tmp/reports/mutation-report.json"
    (
        jvm.io.github.wpunit13.mutator.report.ReportSink.finalizeAndWriteReports
    ).assert_called_once_with()


def test_reset_session_state_calls_jvm_path_and_parses(bridge, jvm):
    (
        jvm.io.github.wpunit13.mutator.reset.SessionResetFacade.resetSessionState
    ).return_value = RESET_JSON
    result = bridge.reset_session_state(MagicMock(), 12345)
    (
        jvm.io.github.wpunit13.mutator.reset.SessionResetFacade.resetSessionState
    ).assert_called_once()
    assert result == {
        "clearedCacheEntries": -1,
        "unpersistedRddCount": 0,
        "droppedTempViews": [],
    }


# ---------------------------------------------------------------------------
# get_active_mutant_or_none null handling
# ---------------------------------------------------------------------------


def test_get_active_mutant_or_none_returns_none_when_idle(bridge, jvm):
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.getActiveMutantOrNull
    ).return_value = None
    assert bridge.get_active_mutant_or_none() is None


# ---------------------------------------------------------------------------
# reset_session_state pass-through semantics
# ---------------------------------------------------------------------------


def test_reset_session_state_passes_jsparksession_untouched(bridge, jvm):
    sentinel = object()
    (
        jvm.io.github.wpunit13.mutator.reset.SessionResetFacade.resetSessionState
    ).return_value = RESET_JSON
    bridge.reset_session_state(sentinel, 987654321)
    mock_method = (
        jvm.io.github.wpunit13.mutator.reset.SessionResetFacade.resetSessionState
    )
    mock_method.assert_called_once()
    args, kwargs = mock_method.call_args
    assert args[0] is sentinel
    assert args[1] == 987654321
    assert kwargs == {}


# ---------------------------------------------------------------------------
# Exception translation
# ---------------------------------------------------------------------------


def test_py4j_error_is_translated_to_mutator_jvm_error(bridge, jvm):
    original = _make_py4j_error("boom")
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.setActiveMutant
    ).side_effect = original
    with pytest.raises(MutatorJvmError) as excinfo:
        bridge.set_active_mutant(VALID_ID)
    assert excinfo.value.java_message  # non-empty
    assert "boom" in excinfo.value.java_message


def test_py4j_error_translated_on_catalog_call(bridge, jvm):
    (
        jvm.io.github.wpunit13.mutator.catalog.MutationCatalogAccess.getFullCatalogJson
    ).side_effect = _make_py4j_error("catalog boom")
    with pytest.raises(MutatorJvmError) as excinfo:
        bridge.get_full_catalog()
    assert "catalog boom" in excinfo.value.java_message


def test_py4j_error_with_null_java_exception_uses_errmsg_fallback(bridge, jvm):
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.setActiveMutant
    ).side_effect = _make_null_java_exception_error("boom")
    with pytest.raises(MutatorJvmError) as excinfo:
        bridge.set_active_mutant(VALID_ID)
    assert "boom" in excinfo.value.java_message


# ---------------------------------------------------------------------------
# Client-side validation short-circuits the JVM
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_id", ["not-hex", "", "0123456789ABCDEF", "0123456789abcde"]
)
def test_set_active_mutant_rejects_malformed_id_without_jvm_call(bridge, jvm, bad_id):
    with pytest.raises(ValueError):
        bridge.set_active_mutant(bad_id)
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.setActiveMutant
    ).assert_not_called()


def test_clear_active_mutant_rejects_malformed_id_without_jvm_call(bridge, jvm):
    with pytest.raises(ValueError):
        bridge.clear_active_mutant("nope")
    (
        jvm.io.github.wpunit13.mutator.MutantRegistry.getInstance.return_value.clearActiveMutant
    ).assert_not_called()


def test_get_mapped_test_ids_rejects_malformed_id_without_jvm_call(bridge, jvm):
    with pytest.raises(ValueError):
        bridge.get_mapped_test_ids("nope")
    (
        jvm.io.github.wpunit13.mutator.catalog.MutationCatalogAccess.getMappedTestIdsJson
    ).assert_not_called()


@pytest.mark.parametrize(
    "bad_status", ["killed", "survived", "Killed", "TIMED-OUT", "UNKNOWN", ""]
)
def test_record_outcome_rejects_invalid_status_without_jvm_call(
    bridge, jvm, bad_status
):
    with pytest.raises(ValueError):
        bridge.record_outcome(VALID_ID, bad_status, 100, None)
    (
        jvm.io.github.wpunit13.mutator.report.ReportSink.recordOutcome
    ).assert_not_called()


def test_record_outcome_accepts_valid_status(bridge, jvm):
    bridge.record_outcome(VALID_ID, "KILLED", 100, None)
    (
        jvm.io.github.wpunit13.mutator.report.ReportSink.recordOutcome
    ).assert_called_once_with(VALID_ID, "KILLED", 100, None)


def test_record_outcome_rejects_negative_elapsed_millis(bridge, jvm):
    with pytest.raises(ValueError):
        bridge.record_outcome(VALID_ID, "KILLED", -5, None)
    (
        jvm.io.github.wpunit13.mutator.report.ReportSink.recordOutcome
    ).assert_not_called()
