"""Driver-resilience circuit breaker (Escalation Stage 4).

Owns the session epoch (generation counter), failure classification, and
recovery sequence wrapping :mod:`pytest_spark_mutator.session_manager`.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
import json
import logging
import os
import threading
import time
import traceback

from . import session_manager
from .config import SparkMutatorConfig

logger = logging.getLogger(__name__)

# Reason taxonomy constants
DRIVER_WATCHDOG_FORCE_KILL = "driver_watchdog_force_kill"
GATEWAY_UNRESPONSIVE = "gateway_unresponsive"
DRIVER_CRASH = "driver_crash"
DRIVER_CRASH_PENDING_REQUEUE = "driver_crash_pending_requeue"

# Exact lowercase aliases matching the specification taxonomy
driver_watchdog_force_kill = DRIVER_WATCHDOG_FORCE_KILL
gateway_unresponsive = GATEWAY_UNRESPONSIVE
driver_crash = DRIVER_CRASH
driver_crash_pending_requeue = DRIVER_CRASH_PENDING_REQUEUE


@dataclass(frozen=True)
class DriverFailure:
    reason_code: str          # "driver_watchdog_force_kill" | "gateway_unresponsive" | "driver_crash"
    trigger: str              # "stage3_timeout" | "control_channel_timeout" | "network_drop" | "sentinel"
    mutant_id: str
    detail: str               # human-readable, includes job_ids / timeout / exception message
    traceback: str = ""


class GatewayUnresponsiveError(Exception):
    """Raised when a control-channel call exceeds its deadline."""


def with_control_timeout(call, timeout_seconds: float):
    """Run ``call()`` on a daemon thread and join with ``timeout_seconds``.

    Return the result; if the call raises, propagate that exception; if the
    thread is still alive after the timeout, raise :class:`GatewayUnresponsiveError`
    naming the timeout.
    """
    result = []
    error = []

    def target():
        try:
            result.append(call())
        except BaseException as exc:
            error.append(exc)

    thread = threading.Thread(
        target=target, daemon=True, name="spark-mutator-control-call"
    )
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        raise GatewayUnresponsiveError(
            f"control-channel call exceeded timeout of {timeout_seconds}s"
        )
    if error:
        raise error[0]
    return result[0] if result else None


class SessionDriver:
    """Manages the SparkSession generation (epoch) and recovery circuit breaker."""

    def __init__(
        self,
        config: SparkMutatorConfig,
        session_creator=None,
        jvm_accessor=None,
    ):
        self.config = config
        self._session_creator = (
            session_creator or session_manager.get_or_create_session
        )
        self._jvm_accessor = jvm_accessor or session_manager.get_jvm
        self._epoch = 0
        self._healthy = True
        self._session = None
        self._failure: DriverFailure | None = None
        self._lock = threading.Lock()
        self.exit_nonzero = False

    @property
    def epoch(self) -> int:
        return self._epoch

    def healthy(self) -> bool:
        with self._lock:
            return self._healthy

    @property
    def failure(self) -> DriverFailure | None:
        with self._lock:
            return self._failure

    def get_session(self):
        """Return the active SparkSession for the current epoch.

        Creates on first use; restarts if previously tripped.
        """
        with self._lock:
            if not self._healthy:
                self._restart_locked()
            if self._session is None:
                self._session = self._session_creator(self.config)
            return self._session

    def get_jvm(self):
        """Single audit point for spark._jvm."""
        return self._jvm_accessor(self.get_session())

    def trip(self, failure: DriverFailure) -> None:
        """Mark the driver unhealthy and store the failure.

        Idempotent and non-blocking. Does not stop the session; restart()
        handles teardown.
        """
        with self._lock:
            if not self._healthy:
                return  # Idempotent: already tripped
            self._healthy = False
            tb = failure.traceback
            if not tb:
                current_tb = traceback.format_exc()
                if current_tb and current_tb != "NoneType: None\n":
                    tb = current_tb
                else:
                    tb = "".join(traceback.format_stack())
            self._failure = dataclasses.replace(failure, traceback=tb)
            logger.warning(
                "circuit breaker tripped for mutant %s (reason: %s, trigger: %s): %s",
                failure.mutant_id,
                failure.reason_code,
                failure.trigger,
                failure.detail,
            )

    def restart(self) -> None:
        """Run the recovery sequence: teardown, reset, fresh session, epoch += 1."""
        with self._lock:
            self._restart_locked()

    def _restart_locked(self) -> None:
        old_session = self._session
        # 1. Teardown: spark.sparkContext.stop() with short timeout.
        # If it hangs or raises, force-kill the gateway child process.
        if old_session is not None:
            sc = getattr(old_session, "sparkContext", None)
            if sc is not None and hasattr(sc, "stop"):
                timeout = min(
                    getattr(self.config, "control_channel_timeout_seconds", 5.0),
                    5.0,
                )
                try:
                    with_control_timeout(sc.stop, timeout_seconds=timeout)
                except Exception:
                    logger.warning(
                        "sparkContext.stop() timed out or failed; force-killing gateway",
                        exc_info=True,
                    )
                    self._force_kill_child_process(sc)

        # 2. Reset the singleton
        session_manager.reset_for_testing()

        # 3. Create fresh session for new epoch
        try:
            self._session = self._session_creator(self.config)
            self._epoch += 1
            self._healthy = True
        except Exception as exc:
            logger.exception("failed to recreate SparkSession after driver restart")
            raise RuntimeError(
                f"failed to recreate SparkSession for epoch {self._epoch + 1}: {exc}"
            ) from exc

    def _force_kill_child_process(self, sc) -> None:
        """Force-kill the JVM child process if available (best-effort).

        In PySpark, the Java gateway process is launched by
        `pyspark.java_gateway.launch_gateway()`, and its `subprocess.Popen`
        handle is stored at `spark.sparkContext._gateway.proc`. When
        sparkContext.stop() hangs or raises, we locate `proc` and invoke
        `proc.kill()`.
        (Note: In the Maven-plugin Mojo architecture, the forked JVM is
        managed via Process.destroyForcibly()).
        """
        try:
            gateway = getattr(sc, "_gateway", None)
            proc = getattr(gateway, "proc", None) if gateway is not None else None
            if proc is not None and hasattr(proc, "kill"):
                logger.info(
                    "force-killing PySpark gateway process (pid=%s)",
                    getattr(proc, "pid", None),
                )
                proc.kill()
        except Exception:
            logger.warning("could not force-kill gateway child process", exc_info=True)


def read_sentinel_file(path: str | None = None) -> dict | None:
    """Read and return JSON content from the JVM fatal sentinel file, if present.

    Cross-cutting contract (with JVM DriverFatalShutdownHook):
    When the JVM experiences a fatal error (OutOfMemoryError, StackOverflowError, LinkageError),
    DriverFatalShutdownHook writes a sentinel JSON file at the path configured by
    System property "spark.mutator.sentinel.path" (or env var SPARK_MUTATOR_SENTINEL_PATH).
    On Py4JNetworkError, Python reads this file to enrich the driver_crash failure detail
    with the JVM's actual error class and message.
    """
    if path is None:
        path = os.environ.get("SPARK_MUTATOR_SENTINEL_PATH")
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        try:
            os.remove(path)
        except OSError:
            pass
        return data
    except Exception:
        logger.warning("failed to read sentinel file at %s", path, exc_info=True)
        return None


def handle_breaker(
    driver: SessionDriver,
    config: SparkMutatorConfig,
    mutant: str,
    failure: DriverFailure,
    bridge=None,
    remaining: list[str] | None = None,
    results: dict[str, int] | None = None,
) -> bool:
    """Central accounting function when a breaker trigger occurs.

    - Record the crashing mutant ERRORED with DriverFailure.reason_code and detail.
    - driver.trip(failure).
    - If config.requeue_on_crash:
      driver.restart() and return False (do not abort; continue loop).
    - Else (abort):
      mark every remaining mutant ERRORED with reason driver_crash_pending_requeue.
      run finalize_reports() (via bridge).
      set non-zero exit flag on driver.
      return True (abort loop).
    """
    # 1. Record the crashing mutant ERRORED
    failure_detail = (
        f"{failure.reason_code}: {failure.detail}"
        if failure.detail
        else failure.reason_code
    )
    if bridge is not None:
        try:
            bridge.record_outcome(mutant, "ERRORED", 0, failure_detail)
        except Exception:
            logger.warning(
                "could not record outcome on bridge for crashing mutant %s",
                mutant,
                exc_info=True,
            )
    if results is not None:
        results["ERRORED"] = results.get("ERRORED", 0) + 1

    # 2. Trip the driver
    driver.trip(failure)

    # 3. Requeue or abort
    if config.requeue_on_crash:
        driver.restart()
        return False  # continue loop
    else:
        driver.exit_nonzero = True
        if remaining:
            for rem_mutant in remaining:
                if bridge is not None:
                    try:
                        bridge.record_outcome(
                            rem_mutant, "ERRORED", 0, DRIVER_CRASH_PENDING_REQUEUE
                        )
                    except Exception:
                        logger.warning(
                            "could not record outcome on bridge for aborted mutant %s",
                            rem_mutant,
                            exc_info=True,
                        )
                if results is not None:
                    results["ERRORED"] = results.get("ERRORED", 0) + 1
        if bridge is not None:
            try:
                bridge.finalize_reports()
            except Exception:
                logger.warning(
                    "could not finalize reports on bridge during abort",
                    exc_info=True,
                )
        return True  # abort loop


_handle_breaker = handle_breaker
