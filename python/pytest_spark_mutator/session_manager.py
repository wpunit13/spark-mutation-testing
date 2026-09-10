"""Single-session SparkSession management (project specification §2).

The mutation harness reuses one SparkSession across all mutant executions to
eliminate the latency of restarting the JVM driver context. The session is
memoized in a module-level singleton; :func:`reset_for_testing` drops that
singleton without stopping the session (test-only).
"""

from __future__ import annotations

import logging
import threading

from .config import SparkMutatorConfig

logger = logging.getLogger(__name__)

_session = None
_session_lock = threading.Lock()


def get_or_create_session(config: SparkMutatorConfig):
    """Return the memoized :class:`pyspark.sql.SparkSession`.

    Repeated calls return the identical object (Single-Session Reuse). The
    session is built via ``SparkSession.builder.getOrCreate()``; this module
    deliberately does not set ``spark.jars`` or ``spark.sql.extensions`` —
    those arrive via ``PYSPARK_SUBMIT_ARGS``, which the pytest plugin (the
    next work package's configuration hook) sets before this function is
    ever called.

    :param config: the resolved spark-mutator configuration. It does not
        influence how the session is constructed today; it is accepted to
        keep the call site stable and is logged at debug level.
    """
    global _session
    if _session is not None:
        return _session
    with _session_lock:
        if _session is not None:
            return _session
        # Lazy import: keeps this module importable (and inspectable) in
        # environments without pyspark, matching version_detect.py.
        from pyspark.sql import SparkSession

        # Py4J gateway flags (architecture requirement: auto_convert=False,
        # auto_field=False) — verified findings:
        #
        # * pyspark 3.5.x python/pyspark/java_gateway.py, launch_gateway():
        #   PySpark constructs its gateway inline and hardcodes
        #   auto_convert=True in BOTH branches —
        #     ClientServer(java_parameters=JavaParameters(port=...,
        #         auth_token=..., auto_convert=True), ...)
        #         when PYSPARK_PIN_THREAD is true (the default), or
        #     JavaGateway(gateway_parameters=GatewayParameters(
        #         port=..., auth_token=..., auto_convert=True))
        #   It never passes auto_field, so auto_field falls back to py4j's
        #   default of False (py4j 0.10.9.9 GatewayParameters.__init__ and
        #   JavaParameters.__init__: auto_field=False, auto_convert=False).
        #
        # * Net result: auto_field=False is satisfied (py4j default);
        #   auto_convert=False is NOT — PySpark forces auto_convert=True and
        #   exposes no public knob (env var or SparkConf key) to change it.
        #
        # Decision: auto_convert=False is unachievable without monkeypatching
        # pyspark.java_gateway.launch_gateway (or the py4j parameter classes),
        # which is out of bounds. We accept auto_convert=True: it only affects
        # how Python collections (list/dict/set) bridge to Java, and the mutator
        # bridge (bridge.py) never passes a Python collection — it crosses only
        # str/int/bool/None plus JSON strings. auto_convert=True therefore has
        # no behavioral effect on any current call, and auto_field=False still
        # holds. This is a deliberate, verified acceptance of the architecture's
        # auto_convert=False requirement.
        logger.debug("creating shared SparkSession (config=%r)", config)
        session = SparkSession.builder.getOrCreate()
        _session = session
        return session


def get_jvm(spark):
    """Return the py4j JVM handle for ``spark``.

    Isolated into one function so the private ``spark._jvm`` attribute
    access has a single audit point.
    """
    return spark._jvm


def reset_for_testing() -> None:
    """Drop the memoized SparkSession singleton WITHOUT stopping the session.

    Test-only: production code must never call this. It exists so unit tests
    can exercise :func:`get_or_create_session` against a fresh singleton.
    """
    global _session
    with _session_lock:
        _session = None
