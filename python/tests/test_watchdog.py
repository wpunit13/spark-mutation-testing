"""Tests for :mod:`pytest_spark_mutator.watchdog`.

Real threads with short deadlines (0.05-0.2s) for the watchdog; MagicMock
for ``spark_context`` in the escalation ladder. No Spark, no JVM.
"""

import logging
import threading
import time
from unittest.mock import MagicMock

import pytest

from pytest_spark_mutator.watchdog import (
    DriverUnresponsiveError,
    Watchdog,
    escalate_cancellation,
)


# ---------------------------------------------------------------------------
# Watchdog firing semantics
# ---------------------------------------------------------------------------


def test_fires_once_on_another_thread():
    seen_threads = []
    calls = []

    def on_timeout():
        seen_threads.append(threading.current_thread().name)
        calls.append(1)

    watchdog = Watchdog()
    watchdog.arm(0.05, on_timeout)
    time.sleep(0.3)
    assert len(calls) == 1
    assert seen_threads[0] != threading.current_thread().name


def test_arm_does_not_block_arming_thread():
    watchdog = Watchdog()
    try:
        start = time.monotonic()
        watchdog.arm(0.2, lambda: None)
        # The statement after arm() must run well under the 0.2s deadline.
        elapsed = time.monotonic() - start
        assert elapsed < 0.05
    finally:
        watchdog.disarm()


def test_disarm_prevents_firing():
    calls = []
    watchdog = Watchdog()
    watchdog.arm(0.5, lambda: calls.append(1))
    time.sleep(0.05)
    watchdog.disarm()
    time.sleep(0.6)
    assert calls == []
    assert watchdog.fired is False


def test_disarm_is_idempotent():
    watchdog = Watchdog()
    watchdog.disarm()  # never armed
    watchdog.arm(0.05, lambda: None)
    watchdog.disarm()
    watchdog.disarm()  # already disarmed
    time.sleep(0.2)
    assert watchdog.fired is False


def test_disarm_after_fired_is_idempotent():
    calls = []
    watchdog = Watchdog()
    watchdog.arm(0.05, lambda: calls.append(1))
    time.sleep(0.2)
    assert watchdog.fired is True
    watchdog.disarm()  # already fired
    watchdog.disarm()
    assert calls == [1]


def test_arm_while_already_armed_raises():
    watchdog = Watchdog()
    try:
        watchdog.arm(0.2, lambda: None)
        with pytest.raises(RuntimeError):
            watchdog.arm(0.2, lambda: None)
    finally:
        watchdog.disarm()


def test_on_timeout_exception_is_caught_logged_and_fired_stays_true(caplog):
    def boom():
        raise ValueError("watchdog callback boom")

    watchdog = Watchdog()
    watchdog.arm(0.05, boom)
    time.sleep(0.3)
    assert watchdog.fired is True
    assert any(
        record.levelno == logging.ERROR
        and "on_timeout" in record.getMessage()
        for record in caplog.records
    )
    # The watchdog survives the callback failure and can be reused.
    watchdog.arm(0.05, lambda: None)
    watchdog.disarm()


# ---------------------------------------------------------------------------
# escalate_cancellation — stages 1-3 (MagicMock spark_context)
# ---------------------------------------------------------------------------


def test_escalate_stage2_returns_timed_out_after_cancel():
    spark_context = MagicMock()
    tracker = spark_context.statusTracker.return_value
    tracker.getJobIdsForGroup.side_effect = [[7], []]

    result = escalate_cancellation(
        spark_context,
        "mutant-abc",
        grace_period_seconds=5.0,
        poll_interval_seconds=0.01,
    )

    assert result == "TIMED_OUT"
    spark_context.cancelJobGroup.assert_called_once_with("mutant-abc")
    tracker.getJobIdsForGroup.assert_called_with("mutant-abc")


def test_escalate_immediate_success_returns_quickly():
    spark_context = MagicMock()
    spark_context.statusTracker.return_value.getJobIdsForGroup.return_value = []

    start = time.monotonic()
    result = escalate_cancellation(
        spark_context,
        "mutant-abc",
        grace_period_seconds=5.0,
        poll_interval_seconds=0.01,
    )
    elapsed = time.monotonic() - start

    assert result == "TIMED_OUT"
    assert elapsed < 1.0  # well under the 5s grace period
    spark_context.cancelJobGroup.assert_called_once_with("mutant-abc")


def test_escalate_stage3_raises_driver_unresponsive():
    spark_context = MagicMock()
    spark_context.statusTracker.return_value.getJobIdsForGroup.return_value = [42]

    with pytest.raises(DriverUnresponsiveError) as excinfo:
        escalate_cancellation(
            spark_context,
            "mutant-xyz",
            grace_period_seconds=0.05,
            poll_interval_seconds=0.01,
            hard_ceiling_seconds=0.15,
        )

    message = str(excinfo.value)
    assert "mutant-xyz" in message
    assert "42" in message
    spark_context.cancelJobGroup.assert_called_once_with("mutant-xyz")


def test_escalate_without_ceiling_skips_stage3_polling():
    spark_context = MagicMock()
    spark_context.statusTracker.return_value.getJobIdsForGroup.return_value = [42]

    start = time.monotonic()
    with pytest.raises(DriverUnresponsiveError):
        escalate_cancellation(
            spark_context,
            "mutant-xyz",
            grace_period_seconds=0.2,
            poll_interval_seconds=0.01,
            hard_ceiling_seconds=None,
        )
    elapsed = time.monotonic() - start

    # No stage-3 polling beyond the grace period itself.
    assert elapsed < 1.0


def test_escalate_uses_public_status_tracker_path():
    spark_context = MagicMock()
    spark_context.statusTracker.return_value.getJobIdsForGroup.return_value = []

    escalate_cancellation(spark_context, "mutant-abc", poll_interval_seconds=0.01)

    spark_context.statusTracker.assert_called_once_with()
