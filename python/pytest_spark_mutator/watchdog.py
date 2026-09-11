"""Timeout watchdog and cancellation escalation ladder.

Two-channel model (docs/ARCHITECTURE.md §2.2): the watchdog's ``on_timeout``
callback executes on the timer's own thread, never on the thread that armed
it. That is what lets the escalation ladder use the control channel
(``cancelJobGroup``, statusTracker polls) while the execution channel is
still blocked inside the hung Spark job — queueing the callback back to the
arming thread would reintroduce the exact starvation the two-channel split
exists to avoid.
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)


class DriverUnresponsiveError(Exception):
    """Raised when a job group ignores cooperative cancellation past the
    hard ceiling (escalation stage 3).

    Callers should classify the mutant as ERRORED. The stage-4 process-level
    circuit breaker (force-kill of the child JVM) is not implemented here;
    it is owned by a future task.
    """


class Watchdog:
    """One-shot deadline timer whose callback runs on the timer thread.

    :param grace_period_seconds: grace period for the escalation ladder
        (stage 2); exposed so the ``on_timeout`` wiring can forward it to
        :func:`escalate_cancellation`.
    :param poll_interval_seconds: poll interval for the escalation ladder.
    """

    def __init__(
        self, grace_period_seconds: float = 5.0, poll_interval_seconds: float = 0.5
    ):
        self.grace_period_seconds = grace_period_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._armed = False
        self._fired = False

    def arm(self, deadline_seconds: float, on_timeout) -> None:
        """Start a one-shot timer that invokes ``on_timeout()`` after
        ``deadline_seconds``.

        :raises RuntimeError: if the watchdog is already armed. The mutation
            loop is strictly sequential, so overlapping arms indicate a bug.
        """
        with self._lock:
            if self._armed:
                raise RuntimeError(
                    "watchdog is already armed; disarm it before re-arming"
                )
            timer = threading.Timer(
                deadline_seconds, self._on_deadline, args=(on_timeout,)
            )
            timer.daemon = True  # never block interpreter exit on a hung callback
            timer.name = "spark-mutator-watchdog"
            self._timer = timer
            self._armed = True
            self._fired = False
        timer.start()

    def _on_deadline(self, on_timeout) -> None:
        # Runs on the timer thread. Mark fired first, then invoke on_timeout
        # directly on this thread — never queued, signalled, or serviced by
        # the arming thread (that would reintroduce the starvation the
        # two-channel model exists to avoid). Once the deadline has elapsed
        # the timer is no longer pending, so the watchdog is unarmed again;
        # fired stays True until the next arm().
        with self._lock:
            self._fired = True
            self._armed = False
            self._timer = None
        # Lock released before on_timeout runs (§2.5: never hold a monitor
        # across a potentially blocking call).
        try:
            on_timeout()
        except Exception:
            # A watchdog that dies silently is worse than one that logs.
            logger.exception("watchdog on_timeout callback raised")

    def disarm(self) -> None:
        """Cancel the pending timer, if any, and mark the watchdog unarmed.

        Idempotent: safe to call when never armed, already fired, or already
        disarmed. Never blocks on a running ``on_timeout`` (no join).
        """
        with self._lock:
            timer = self._timer
            self._timer = None
            self._armed = False
        if timer is not None:
            timer.cancel()

    @property
    def fired(self) -> bool:
        """Whether the deadline elapsed for the current arm cycle."""
        with self._lock:
            return self._fired


def _active_job_ids(spark_context, job_group_id: str) -> list:
    # Public Python path: SparkContext.statusTracker() is a plain method on
    # pyspark's Python SparkContext (verified in pyspark 3.5.x
    # python/pyspark/context.py), and getJobIdsForGroup returns the ids of
    # the jobs currently registered for the group. Structured so a
    # MagicMock can stand in for spark_context in tests.
    return list(spark_context.statusTracker().getJobIdsForGroup(job_group_id))


def escalate_cancellation(
    spark_context,
    job_group_id: str,
    grace_period_seconds: float = 5.0,
    poll_interval_seconds: float = 0.5,
    hard_ceiling_seconds: float | None = None,
) -> str:
    """Run escalation ladder stages 1-3 (docs/ARCHITECTURE.md §6.2).

    Called by an ``on_timeout`` callback (the pytest plugin supplies that
    wiring); this function does not touch the watchdog itself.

    - Stage 1: ``spark_context.cancelJobGroup(job_group_id)`` — soft cancel
      via the control channel.
    - Stage 2: poll ``statusTracker().getJobIdsForGroup`` every
      ``poll_interval_seconds`` for up to ``grace_period_seconds``; an empty
      list means stage 1 succeeded and ``"TIMED_OUT"`` is returned.
    - Stage 3: still non-empty after the grace period — keep polling until
      ``hard_ceiling_seconds`` total (measured from the start of this call;
      when ``None``, skip straight to escalation), then raise
      :class:`DriverUnresponsiveError`.

    :returns: ``"TIMED_OUT"`` once the job group is no longer active.
    :raises DriverUnresponsiveError: when the group is still active past the
        hard ceiling (or no ceiling was given). Callers should treat this as
        ERRORED.
    """
    start = time.monotonic()

    # Stage 1 — soft cancel.
    spark_context.cancelJobGroup(job_group_id)

    # Stage 2 — confirm within the grace period.
    job_ids = _active_job_ids(spark_context, job_group_id)
    while job_ids and time.monotonic() - start < grace_period_seconds:
        time.sleep(poll_interval_seconds)
        job_ids = _active_job_ids(spark_context, job_group_id)
    if not job_ids:
        return "TIMED_OUT"

    # Stage 3 — unresponsive to cooperative cancellation; poll until the
    # hard ceiling, measured from the start of the escalation.
    if hard_ceiling_seconds is not None:
        while job_ids and time.monotonic() - start < hard_ceiling_seconds:
            time.sleep(poll_interval_seconds)
            job_ids = _active_job_ids(spark_context, job_group_id)
        if not job_ids:
            return "TIMED_OUT"

    # Stage 4 (driver-resilience circuit breaker) is implemented in
    # session_driver.SessionDriver; callers should treat DriverUnresponsiveError
    # as the trigger that trips the breaker, which classifies the mutant ERRORED.
    raise DriverUnresponsiveError(
        f"job group {job_group_id!r} remained active past the hard ceiling; "
        f"still-active job ids: {job_ids}"
    )
