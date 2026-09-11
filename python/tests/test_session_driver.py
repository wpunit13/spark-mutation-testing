"""Unit tests for :mod:`pytest_spark_mutator.session_driver`.

All tests use fakes and mocks; no real Spark session or JVM is started.
"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest
from py4j.protocol import Py4JNetworkError

from pytest_spark_mutator.config import SparkMutatorConfig
from pytest_spark_mutator.session_driver import (
    DRIVER_CRASH,
    DRIVER_CRASH_PENDING_REQUEUE,
    DRIVER_WATCHDOG_FORCE_KILL,
    GATEWAY_UNRESPONSIVE,
    DriverFailure,
    GatewayUnresponsiveError,
    SessionDriver,
    handle_breaker,
    read_sentinel_file,
    with_control_timeout,
)
from pytest_spark_mutator.watchdog import DriverUnresponsiveError


class FakeSparkContext:
    def __init__(self, events: list):
        self.events = events
        self.stopped = False

    def stop(self):
        self.events.append("sparkContext.stop")
        self.stopped = True


class FakeSparkSession:
    def __init__(self, name: str, events: list):
        self.name = name
        self.events = events
        self.sparkContext = FakeSparkContext(events)
        self._jvm = SimpleNamespace(name=f"jvm-{name}")


class FakeBridge:
    def __init__(self, events: list):
        self.events = events
        self.outcomes: list[tuple] = []
        self.finalize_calls = 0

    def record_outcome(self, mutant_id, status, elapsed_millis, failure_detail):
        self.events.append(
            ("record_outcome", mutant_id, status, elapsed_millis, failure_detail)
        )
        self.outcomes.append((mutant_id, status, elapsed_millis, failure_detail))

    def finalize_reports(self):
        self.events.append("finalize_reports")
        self.finalize_calls += 1
        return "/fake/reports/mutator.sarif"


# ---------------------------------------------------------------------------
# Required Scenario 1: Memoization and Epoch
# ---------------------------------------------------------------------------


def test_session_driver_memoizes_within_epoch_and_increments_after_restart(monkeypatch):
    """SessionDriver.get_session memoizes within an epoch and returns a different
    object after restart(), with epoch incremented."""
    events = []
    session_counter = 0

    def fake_creator(config):
        nonlocal session_counter
        session_counter += 1
        return FakeSparkSession(f"session-{session_counter}", events)

    config = SparkMutatorConfig()
    driver = SessionDriver(config, session_creator=fake_creator)

    assert driver.epoch == 0
    s1 = driver.get_session()
    assert s1.name == "session-1"
    # Repeated calls return the identical memoized object within the epoch
    assert driver.get_session() is s1
    assert driver.epoch == 0

    # Teardown and restart
    driver.restart()

    assert driver.epoch == 1
    s2 = driver.get_session()
    assert s2.name == "session-2"
    assert s2 is not s1
    assert driver.get_session() is s2
    assert driver.epoch == 1


# ---------------------------------------------------------------------------
# Required Scenario 2: Teardown Order
# ---------------------------------------------------------------------------


def test_restart_teardown_order(monkeypatch):
    """restart() tears down (calls sparkContext.stop) before recreating, in that
    order, and calls session_manager.reset_for_testing() between them."""
    events = []
    session_counter = 0

    def fake_creator(config):
        nonlocal session_counter
        session_counter += 1
        sess = FakeSparkSession(f"session-{session_counter}", events)
        events.append(f"create_session_{session_counter}")
        return sess

    def fake_reset():
        events.append("session_manager.reset_for_testing")

    monkeypatch.setattr(
        "pytest_spark_mutator.session_manager.reset_for_testing", fake_reset
    )

    config = SparkMutatorConfig()
    driver = SessionDriver(config, session_creator=fake_creator)
    driver.get_session()
    assert events == ["create_session_1"]

    events.clear()
    driver.restart()

    assert events == [
        "sparkContext.stop",
        "session_manager.reset_for_testing",
        "create_session_2",
    ]


# ---------------------------------------------------------------------------
# Required Scenario 3: Trip Idempotency and State
# ---------------------------------------------------------------------------


def test_trip_marks_unhealthy_and_is_idempotent():
    """trip() marks the driver unhealthy and stores the DriverFailure;
    a second trip() is idempotent."""
    config = SparkMutatorConfig()
    driver = SessionDriver(config)
    assert driver.healthy() is True
    assert driver.failure is None

    failure1 = DriverFailure(
        reason_code=DRIVER_WATCHDOG_FORCE_KILL,
        trigger="stage3_timeout",
        mutant_id="0123456789abcdef",
        detail="job group active past ceiling",
    )
    driver.trip(failure1)

    assert driver.healthy() is False
    assert driver.failure is not None
    assert driver.failure.reason_code == DRIVER_WATCHDOG_FORCE_KILL
    assert driver.failure.trigger == "stage3_timeout"
    assert driver.failure.mutant_id == "0123456789abcdef"
    assert driver.failure.traceback != ""  # Auto-populated

    first_failure = driver.failure

    # Second trip call must be idempotent
    failure2 = DriverFailure(
        reason_code=GATEWAY_UNRESPONSIVE,
        trigger="control_channel_timeout",
        mutant_id="fedcba9876543210",
        detail="different failure",
    )
    driver.trip(failure2)

    assert driver.healthy() is False
    assert driver.failure is first_failure
    assert driver.failure.mutant_id == "0123456789abcdef"


# ---------------------------------------------------------------------------
# Required Scenario 4: with_control_timeout
# ---------------------------------------------------------------------------


def test_with_control_timeout_fast_call():
    """with_control_timeout returns the value for a fast call."""
    res = with_control_timeout(lambda: 42, timeout_seconds=1.0)
    assert res == 42


def test_with_control_timeout_sleep_raises_gateway_unresponsive():
    """with_control_timeout raises GatewayUnresponsiveError for a call that
    sleeps past the timeout."""
    def slow_call():
        time.sleep(0.5)
        return "too-late"

    with pytest.raises(GatewayUnresponsiveError) as exc_info:
        with_control_timeout(slow_call, timeout_seconds=0.05)
    assert "0.05" in str(exc_info.value)


def test_with_control_timeout_propagates_exception():
    """with_control_timeout propagates an exception raised inside the call."""
    def failing_call():
        raise ValueError("inner problem")

    with pytest.raises(ValueError, match="inner problem"):
        with_control_timeout(failing_call, timeout_seconds=1.0)


# ---------------------------------------------------------------------------
# Required Scenario 5: Abort Accounting (requeue_on_crash = False)
# ---------------------------------------------------------------------------


def test_abort_accounting_on_crash():
    """Abort accounting (requeue_on_crash=False): given a mutant list and a
    trip at index k, the crashing mutant is ERRORED with its trigger reason,
    every mutant after k is ERRORED with driver_crash_pending_requeue,
    finalize_reports() is called exactly once, and a non-zero exit flag is set."""
    events = []
    bridge = FakeBridge(events)
    config = SparkMutatorConfig(requeue_on_crash=False)
    driver = SessionDriver(config)

    mutants = [f"mutant_{i:02d}" for i in range(5)]
    k = 2
    crashing_mutant = mutants[k]
    remaining = mutants[k + 1 :]

    failure = DriverFailure(
        reason_code=DRIVER_WATCHDOG_FORCE_KILL,
        trigger="stage3_timeout",
        mutant_id=crashing_mutant,
        detail="timeout details",
    )

    results = {}
    aborted = handle_breaker(
        driver=driver,
        config=config,
        mutant=crashing_mutant,
        failure=failure,
        bridge=bridge,
        remaining=remaining,
        results=results,
    )

    assert aborted is True
    assert driver.exit_nonzero is True
    assert driver.healthy() is False
    assert bridge.finalize_calls == 1

    # Check outcomes
    # mutant_02 (crashing) -> ERRORED with trigger reason
    # mutant_03, mutant_04 -> ERRORED with driver_crash_pending_requeue
    assert len(bridge.outcomes) == 3
    assert bridge.outcomes[0][0] == crashing_mutant
    assert bridge.outcomes[0][1] == "ERRORED"
    assert DRIVER_WATCHDOG_FORCE_KILL in bridge.outcomes[0][3]

    for rem_outcome in bridge.outcomes[1:]:
        assert rem_outcome[1] == "ERRORED"
        assert rem_outcome[3] == DRIVER_CRASH_PENDING_REQUEUE

    assert results["ERRORED"] == 3


# ---------------------------------------------------------------------------
# Required Scenario 6: Requeue Accounting (requeue_on_crash = True)
# ---------------------------------------------------------------------------


def test_requeue_accounting_on_crash(monkeypatch):
    """Requeue accounting (requeue_on_crash=True): the crashing mutant is
    ERRORED, restart() is called exactly once, and the loop continues with
    the remaining mutants."""
    events = []
    bridge = FakeBridge(events)
    config = SparkMutatorConfig(requeue_on_crash=True)

    session_counter = 0

    def fake_creator(cfg):
        nonlocal session_counter
        session_counter += 1
        return FakeSparkSession(f"session-{session_counter}", events)

    driver = SessionDriver(config, session_creator=fake_creator)
    driver.get_session()
    assert driver.epoch == 0

    mutants = ["m0", "m1", "m2", "m3"]
    k = 1
    crashing_mutant = mutants[k]
    remaining = mutants[k + 1 :]

    failure = DriverFailure(
        reason_code=DRIVER_CRASH,
        trigger="network_drop",
        mutant_id=crashing_mutant,
        detail="py4j dropped connection",
    )

    results = {}
    aborted = handle_breaker(
        driver=driver,
        config=config,
        mutant=crashing_mutant,
        failure=failure,
        bridge=bridge,
        remaining=remaining,
        results=results,
    )

    # Should not abort; restart() should have been called
    assert aborted is False
    assert driver.exit_nonzero is False
    assert driver.epoch == 1
    assert driver.healthy() is True
    assert bridge.finalize_calls == 0  # Not finalized on requeue

    assert len(bridge.outcomes) == 1
    assert bridge.outcomes[0][0] == crashing_mutant
    assert bridge.outcomes[0][1] == "ERRORED"
    assert DRIVER_CRASH in bridge.outcomes[0][3]
    assert results["ERRORED"] == 1


# ---------------------------------------------------------------------------
# Additional Resilience and Diagnostic Tests
# ---------------------------------------------------------------------------


def test_read_sentinel_file(tmp_path):
    """Sentinel file reading and cleanup contract."""
    sentinel = tmp_path / "sentinel.json"
    sentinel.write_text(
        json.dumps(
            {
                "reason": "OutOfMemoryError",
                "message": "Java heap space",
                "epochMillis": 123456789,
            }
        ),
        encoding="utf-8",
    )

    data = read_sentinel_file(str(sentinel))
    assert data is not None
    assert data["reason"] == "OutOfMemoryError"
    assert data["message"] == "Java heap space"
    # Unlinked after read
    assert not sentinel.exists()


def test_read_sentinel_file_nonexistent():
    assert read_sentinel_file("/path/does/not/exist.json") is None


def test_force_kill_child_process_on_hung_stop():
    """If sparkContext.stop() hangs or raises, gateway.proc.kill() is called."""
    events = []
    mock_proc = MagicMock()
    mock_proc.pid = 99999

    class HungSparkContext:
        def __init__(self):
            self._gateway = SimpleNamespace(proc=mock_proc)

        def stop(self):
            events.append("hung_stop")
            time.sleep(0.5)

    sess = SimpleNamespace(sparkContext=HungSparkContext())
    config = SparkMutatorConfig(control_channel_timeout_seconds=0.05)
    driver = SessionDriver(config, session_creator=lambda cfg: sess)
    driver._session = sess

    driver.restart()

    assert "hung_stop" in events
    mock_proc.kill.assert_called_once()


def test_config_new_fields_and_validation(tmp_path):
    """Verify requeue_on_crash and control_channel_timeout_seconds config."""
    cfg_file = tmp_path / "pyproject.toml"
    cfg_file.write_text(
        """
[tool.spark-mutator]
requeue_on_crash = true
control_channel_timeout_seconds = 10.0
""",
        encoding="utf-8",
    )
    cfg = SparkMutatorConfig.from_toml(cfg_file)
    assert cfg.requeue_on_crash is True
    assert cfg.control_channel_timeout_seconds == 10.0

    # Test invalid control_channel_timeout_seconds
    bad_cfg = tmp_path / "bad.toml"
    bad_cfg.write_text(
        """
[tool.spark-mutator]
control_channel_timeout_seconds = 0
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="control_channel_timeout_seconds"):
        SparkMutatorConfig.from_toml(bad_cfg)
