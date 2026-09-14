"""Pytest plugin driving the spark-mutator baseline pass and mutation loop.

Hook layout:

* ``pytest_addoption`` / ``pytest_configure`` — option parsing, shim-jar
  mounting via ``PYSPARK_SUBMIT_ARGS``, and construction of the per-run
  :class:`_MutationSession` (stored in ``config.stash``).
* ``pytest_runtest_protocol`` (hookwrapper) — baseline-phase timing and JVM
  test-context tagging. A no-op in the mutation phase (the mutation loop
  drives test execution itself).
* ``pytest_runtest_logreport`` — per-test failure detection for both phases.
* ``pytest_collection_modifyitems`` — captures the collected items so the
  mutation loop can map impact-map nodeids back to runnable items.
* ``pytest_sessionfinish`` — the baseline gate and the fail-fast mutation
  execution loop itself.

When ``--spark-mutate`` is absent the plugin is a total no-op: no env var
writes, no session creation, no protocol interception, no output.
"""

from __future__ import annotations

import importlib.resources
import logging
import os
import time

import pytest

from py4j.protocol import Py4JNetworkError

from .bridge import MutatorBridge
from .config import SparkMutatorConfig
from .session_driver import (
    DriverFailure,
    GatewayUnresponsiveError,
    SessionDriver,
    handle_breaker,
    read_sentinel_file,
    with_control_timeout,
)
from .session_manager import get_jvm, get_or_create_session
from .version_detect import resolve_shim_jar_filename
from .watchdog import DriverUnresponsiveError, Watchdog, escalate_cancellation

logger = logging.getLogger(__name__)

_EXTENSION_CONF_ARG = (
    "--conf spark.sql.extensions=io.github.wpunit13.mutator.MutatorSparkExtension"
)
_SHELL_TOKEN = "pyspark-shell"

# Duplicated from the JVM-side SARIF writer (SarifReportWriter.MUTATOR_NAMES,
# key format "<operatorType>|<mutationIndex>"); must stay in sync with it.
# Deliberately not fetched over the bridge. Unlisted combinations fall back
# to UnknownMutator on the JVM side (SarifReportWriter's documented fallback).
_MUTATOR_NAMES = {
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
_UNKNOWN_MUTATOR = "UnknownMutator"

# Fork-directive / user-config keys (developer-guide §3.1/§3.2): one key
# across both surfaces. The plugin propagates the TOML config into the driver
# JVM through these properties so the engine-side filters and the report's
# config echo see the same values the Python loop enforces.
_PROP_TARGET_MODULES = "spark.mutator.targetModules"
_PROP_EXCLUDED_MUTATORS = "spark.mutator.excludedMutators"
_PROP_TIMEOUT_MULTIPLIER = "spark.mutator.timeoutMultiplier"
_PROP_MIN_MUTATION_SCORE = "spark.mutator.minMutationScore"

# Stash key under which the active _MutationSession is stored on the pytest
# config; the single source of truth for "is the plugin active".
_SESSION_KEY = pytest.StashKey()

# The one authorized module-level session reference: pytest_runtest_logreport
# receives no config object, so it resolves the active session through this
# reference. Set in pytest_configure, cleared in pytest_unconfigure.
_active_session: _MutationSession | None = None

# User-facing limitation (docs/ARCHITECTURE.md §5.3): the shim jar and the
# MutatorSparkExtension are injected through PYSPARK_SUBMIT_ARGS, which only
# takes effect when it is set before SparkSession.builder.getOrCreate() runs.
# Pipeline code under test must therefore not construct a SparkSession at
# import time (or with a config overriding spark.jars / spark.sql.extensions):
# env-var injection cannot retroactively alter an already-created session.


def pytest_addoption(parser) -> None:
    group = parser.getgroup("spark-mutator")
    group.addoption(
        "--spark-mutate",
        action="store_true",
        default=False,
        help="Run the spark-mutator baseline pass and mutation loop.",
    )
    group.addoption(
        "--spark-mutate-config",
        default="pyproject.toml",
        help="Path to the TOML file holding the [tool.spark-mutator] table.",
    )


def pytest_configure(config) -> None:
    global _active_session
    if not config.getoption("--spark-mutate"):
        # Inactive: total no-op (hard requirement). Nothing below may run.
        return
    mutator_config = SparkMutatorConfig.from_toml(
        config.getoption("--spark-mutate-config")
    )
    # Propagate output_dir to the JVM-side ReportSink, which resolves it from
    # SPARK_MUTATOR_OUTPUT_DIR (then the spark.mutator.outputDirectory system
    # property, then a default). Set it before the gateway JVM is lazily
    # launched, so the child process inherits it. Without this, the config's
    # output_dir was effectively ignored.
    os.environ["SPARK_MUTATOR_OUTPUT_DIR"] = mutator_config.output_dir
    # UnsupportedSparkVersionError deliberately propagates: pytest aborts with
    # that message, which is exactly the required fast-fail guard.
    jar_filename = resolve_shim_jar_filename()
    jar_path = _resolve_jar_path(jar_filename)
    _inject_submit_args(jar_path)
    session = _MutationSession(mutator_config)
    config.stash[_SESSION_KEY] = session
    _active_session = session


def pytest_unconfigure(config) -> None:
    global _active_session
    if _session_for_config(config) is not None:
        _active_session = None


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    session = _session_for_config(item.config)
    if session is None:
        yield
        return
    if session.phase != "baseline":
        # Mutation phase: the loop in pytest_sessionfinish drives test
        # execution itself; the protocol hook must not add behavior here.
        yield
        return
    start = time.perf_counter()
    try:
        session._apply_engine_config()
        session._set_test_context(item.nodeid)
        session._set_file_path_hint(item.nodeid)
        yield
    finally:
        session._clear_test_context()
        session.baseline_elapsed[item.nodeid] = time.perf_counter() - start


def pytest_runtest_logreport(report) -> None:
    session = _active_session
    if session is None or not report.failed:
        return
    if session.phase == "baseline":
        # A failed call — or a failed/erroring setup — fails the baseline
        # suite and triggers the hard abort in pytest_sessionfinish.
        if report.when in ("call", "setup"):
            session.baseline_failed = True
    elif session.phase == "mutation" and report.when in ("call", "setup"):
        # Per-mutant fail-fast collector: the loop breaks on the first call-
        # or setup-phase failure it observes. A mutation that breaks a fixture
        # (setup) is as much a kill as one that breaks an assertion (call):
        # both prove the mutant changed observable behavior. Classifying the
        # former as SURVIVED would deflate the mutation score.
        session.mutation_call_failed = True
        session.mutation_failed_nodeid = report.nodeid


def pytest_collection_modifyitems(session, config, items) -> None:
    mut_session = _session_for_config(config)
    if mut_session is None:
        return
    mut_session.items_by_nodeid = {item.nodeid: item for item in items}


def pytest_sessionfinish(session, exitstatus) -> None:
    mut_session = _session_for_config(session.config)
    if mut_session is None:
        return
    if mut_session.baseline_failed:
        # Spec's hard abort: never run mutation logic on a red baseline.
        print(
            "spark-mutator: baseline suite failed; mutation testing skipped. "
            "Fix the failing tests before running mutation testing."
        )
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
        return
    if exitstatus != 0:
        # The baseline phase did not complete cleanly (collection error,
        # interrupt, ...). The mutation phase requires a completed green
        # baseline; skipping here strengthens, never weakens, the baseline
        # abort above.
        print(
            "spark-mutator: baseline phase did not complete cleanly "
            f"(exit status {int(exitstatus)}); mutation testing skipped."
        )
        return
    mut_session.phase = "mutation"
    try:
        score = mut_session.run_mutation_loop()
    except Exception as exc:
        logger.exception("spark-mutator: mutation loop failed")
        print(f"spark-mutator: mutation loop failed: {type(exc).__name__}: {exc}")
        session.exitstatus = pytest.ExitCode.INTERNAL_ERROR
        return
    if getattr(mut_session.driver, "exit_nonzero", False):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
        return
    if score < mut_session.config.min_mutation_score:
        print(
            f"spark-mutator: mutation score {score:.1f}% is below "
            f"min_mutation_score {mut_session.config.min_mutation_score:.1f}%; "
            "failing the run."
        )
        # WP-16 governance: a quality-gate breach is exit code 2, deliberately
        # distinct from test assertion failures (1) so CI can route the two
        # differently. Caveat: pytest itself uses 2 for "interrupted by user";
        # the spec accepts the collision because the message above and the
        # written reports disambiguate.
        session.exitstatus = 2
        return


# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------


def _session_for_config(config):
    return config.stash.get(_SESSION_KEY, None)


def _resolve_jar_path(filename: str) -> str:
    """Resolve the bundled shim jar to an absolute path (§5.3)."""
    try:
        jar_path = importlib.resources.files("pytest_spark_mutator.jars").joinpath(
            filename
        )
    except (ModuleNotFoundError, FileNotFoundError):
        jar_path = None
    if jar_path is None or not jar_path.is_file():
        raise pytest.UsageError(
            f"spark-mutator: the bundled shim jar {filename!r} is missing from "
            f"the installed package (expected pytest_spark_mutator/jars/{filename}). "
            "The wheel was most likely built without running the jar bundling "
            "step; rebuild the wheel so the interceptor fat jar is bundled "
            "before running with --spark-mutate."
        )
    return str(jar_path)


def _inject_submit_args(jar_path: str) -> None:
    """Prepend the shim jar and extension conf to ``PYSPARK_SUBMIT_ARGS``.

    User-supplied submit args are preserved after ours, and exactly one
    trailing ``pyspark-shell`` token is kept (required by PySpark's
    submit-args parsing).
    """
    ours = f"--jars {jar_path} {_EXTENSION_CONF_ARG}"
    tokens = os.environ.get("PYSPARK_SUBMIT_ARGS", "").split()
    if tokens and tokens[-1] == _SHELL_TOKEN:
        tokens = tokens[:-1]
    user_args = " ".join(tokens)
    if user_args:
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"{ours} {user_args} {_SHELL_TOKEN}"
    else:
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"{ours} {_SHELL_TOKEN}"


def _mutator_name_for(entry: dict) -> str:
    return _MUTATOR_NAMES.get(
        f"{entry['operatorType']}|{entry['mutationIndex']}", _UNKNOWN_MUTATOR
    )


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------


class _MutationSession:
    """All state for one active spark-mutator run (baseline + mutation)."""

    def __init__(self, config: SparkMutatorConfig):
        self.config = config
        self.driver = SessionDriver(
            config,
            session_creator=lambda cfg: get_or_create_session(cfg),
            jvm_accessor=lambda spk: get_jvm(spk),
        )
        self._bridge = None
        self._bridge_epoch = -1
        self.baseline_elapsed: dict[str, float] = {}
        self.baseline_failed = False
        self.phase = "baseline"
        self.items_by_nodeid = {}
        self.results: dict[str, int] = {}
        self.watchdog = Watchdog()
        self.driver_unresponsive_error: Exception | None = None
        self.mutation_call_failed = False
        self.mutation_failed_nodeid: str | None = None
        self._engine_config_applied = False
        # Epoch-millis lower bound for the §3.2 diff-and-drop reset: state
        # created before the baseline started counts as pre-existing.
        self.baseline_start_epoch_ms = int(time.time() * 1000)

    # -- lazy accessors (no JVM work happens until first use) --------------

    @property
    def spark(self):
        return self.driver.get_session()

    @property
    def jvm(self):
        return self.driver.get_jvm()

    @property
    def bridge(self) -> MutatorBridge:
        if self._bridge is None or self._bridge_epoch != self.driver.epoch:
            self._bridge = MutatorBridge(self.jvm)
            self._bridge_epoch = self.driver.epoch
        return self._bridge

    # -- raw JVM access ----------------------------------------------------
    # Deliberate, narrow exception to the "go through MutatorBridge" rule:
    # MutatorBridge does not expose TestContextTracker, the system-property
    # channel, or CatalystMutationRule and must not be modified. These methods
    # are the only raw-JVM access in this plugin; they are baseline-phase
    # bookkeeping (test-context tagging, WP-19 config propagation and
    # file-path hints), not general-purpose bridge operations.

    def _set_test_context(self, nodeid: str) -> None:
        self.jvm.io.github.wpunit13.mutator.TestContextTracker.setCurrentTestId(nodeid)

    def _clear_test_context(self) -> None:
        self.jvm.io.github.wpunit13.mutator.TestContextTracker.clearCurrentTestId()

    def _apply_engine_config(self) -> None:
        """One-time propagation of the TOML config into the driver JVM (WP-19).

        The properties must be set before the first analysis so the engine-side
        filters (CatalystMutationRule reads them per invocation) and the
        report's config echo (ReportWriter resolves them at finalize time) see
        them. ``excluded_mutators`` is propagated verbatim: canonical
        OperatorType names are enforced engine-side, legacy mutator display
        names are ignored there (with a one-time warning) and enforced by the
        Python loop below, which understands both key shapes.
        """
        if self._engine_config_applied:
            return
        system = self.jvm.java.lang.System
        excluded = ",".join(self.config.excluded_mutators)
        targets = ",".join(self.config.target_modules)
        if excluded:
            system.setProperty(_PROP_EXCLUDED_MUTATORS, excluded)
        if targets:
            system.setProperty(_PROP_TARGET_MODULES, targets)
        system.setProperty(_PROP_TIMEOUT_MULTIPLIER, str(self.config.timeout_multiplier))
        system.setProperty(_PROP_MIN_MUTATION_SCORE, str(self.config.min_mutation_score))
        self._engine_config_applied = True

    def _set_file_path_hint(self, nodeid: str) -> None:
        """Best-effort test-file-granularity hint for targetModules (WP-19).

        Wired ONLY when target_modules is configured: the hint is a direct
        input to the MutantID formula, so changing it unconditionally would
        churn every mutantId (and break cross-run mutantId comparisons such as
        the weak-vs-hardened e2e proof). With target_modules empty the hint
        stays at its default "unknown" and mutantIds stay stable.
        """
        if not self.config.target_modules:
            return
        test_file = nodeid.split("::", 1)[0]
        self.jvm.io.github.wpunit13.mutator.CatalystMutationRule.setCurrentFilePathHint(
            test_file
        )

    # -- mutation loop ------------------------------------------------------

    def run_mutation_loop(self) -> float:
        """Run every non-excluded mutant; returns the mutation score."""
        bridge = self.bridge
        catalog = bridge.get_full_catalog()
        driver = self.driver
        config = self.config

        # WP-19 exclusion keys: canonical OperatorType names (case-
        # insensitive) plus the legacy mutator display names (exact match,
        # backward compatible). Canonical names are additionally enforced
        # engine-side via the spark.mutator.excludedMutators system property
        # (see _apply_engine_config), so excluded operators typically never
        # reach this catalog at all; the check below remains as the second
        # layer and the only enforcement point for legacy names.
        excluded_operator_types = {
            name.strip().upper() for name in config.excluded_mutators
        }

        survivors = []
        for entry in catalog:
            mutator_name = _mutator_name_for(entry)
            operator_type = str(entry.get("operatorType", "")).strip().upper()
            if operator_type in excluded_operator_types or (
                mutator_name in config.excluded_mutators
            ):
                logger.debug(
                    "spark-mutator: excluding %s (mutant %s, operator %s)",
                    mutator_name,
                    entry["mutantId"],
                    entry.get("operatorType"),
                )
                continue
            survivors.append((entry["mutantId"], mutator_name))

        def _handle_breaker(driver, config, mutant, failure):
            return handle_breaker(
                driver,
                config,
                mutant,
                failure,
                bridge=self.bridge,
                remaining=remaining_mutants,
                results=self.results,
            )

        for idx, (mutant, mutator_name) in enumerate(survivors):
            remaining_mutants = [m[0] for m in survivors[idx + 1:]]
            try:
                self._evaluate_mutant(mutant, mutator_name)
            except DriverUnresponsiveError as e:          # signal A (stage 3)
                aborted = _handle_breaker(driver, config, mutant, DriverFailure(
                    "driver_watchdog_force_kill", "stage3_timeout", mutant, str(e)))
                if aborted:
                    break
            except GatewayUnresponsiveError as e:         # signal B
                aborted = _handle_breaker(driver, config, mutant, DriverFailure(
                    "gateway_unresponsive", "control_channel_timeout", mutant, str(e)))
                if aborted:
                    break
            except Py4JNetworkError as e:                 # signal C (network drop)
                trigger = "network_drop"
                detail = str(e)
                sentinel = read_sentinel_file()
                if sentinel:
                    trigger = "sentinel"
                    detail = f"{detail}; fatal JVM error: {sentinel.get('reason')}: {sentinel.get('message')}"
                aborted = _handle_breaker(driver, config, mutant, DriverFailure(
                    "driver_crash", trigger, mutant, detail))
                if aborted:
                    break

        if not driver.exit_nonzero:
            report_path = self.bridge.finalize_reports()
        else:
            report_path = getattr(driver, "report_path", None) or "target/spark-mutator-reports"
        return self._print_summary(report_path)

    def _evaluate_mutant(self, mutant_id: str, mutator_name: str) -> None:
        start = time.perf_counter()
        status = "ERRORED"
        failure_detail = None
        needs_reset = False
        reset_error = None
        try:
            mapped = self.bridge.get_mapped_test_ids(mutant_id)
            runnable = [nid for nid in mapped if nid in self.items_by_nodeid]
            if not mapped:
                # Zero tests can neither kill nor let a mutant survive;
                # counting it either way would corrupt the score.
                status, failure_detail = "SKIPPED", "no mapped tests"
            elif not runnable:
                # TODO(spec-gap): the spec does not define behavior when the
                # impact map references nodeids that were not collected in
                # this session; skip the mutant rather than miscounting it.
                logger.warning(
                    "spark-mutator: none of the mapped tests for mutant %s were "
                    "collected in this session; skipping: %s",
                    mutant_id,
                    mapped,
                )
                status, failure_detail = (
                    "SKIPPED",
                    "no mapped tests runnable in this session",
                )
            else:
                needs_reset = True
                status, failure_detail = self._run_mutant(mutant_id, mapped)
        except (DriverUnresponsiveError, GatewayUnresponsiveError, Py4JNetworkError):
            needs_reset = False
            raise
        except Exception as exc:
            # One bad mutant must not abort the run.
            status = "ERRORED"
            failure_detail = f"{type(exc).__name__}: {exc}"
            logger.exception("spark-mutator: mutant %s (%s) errored", mutant_id, mutator_name)
        finally:
            if needs_reset:
                try:
                    reset_error = self._run_reset_sequence(mutant_id)
                except (DriverUnresponsiveError, GatewayUnresponsiveError, Py4JNetworkError):
                    raise
        elapsed = time.perf_counter() - start
        if needs_reset and reset_error is not None and status != "TIMED_OUT":
            status = "ERRORED"
            failure_detail = f"reset sequence failed: {reset_error}"
        self.bridge.record_outcome(
            mutant_id, status, int(elapsed * 1000), failure_detail
        )
        self.results[status] = self.results.get(status, 0) + 1

    def _run_mutant(self, mutant_id: str, mapped: list) -> tuple:
        deadline = max(
            1.0,
            self.config.timeout_multiplier
            * sum(self.baseline_elapsed.get(nid, 0.0) for nid in mapped),
        )
        self.mutation_call_failed = False
        self.mutation_failed_nodeid = None
        self.driver_unresponsive_error = None
        self.bridge.set_active_mutant(mutant_id)
        spark = self.spark
        # §6.1 job group tagging; interruptOnCancel=True is mandatory.
        spark.sparkContext.setJobGroup(
            mutant_id, f"spark-mutator mutant {mutant_id}", True
        )
        self.watchdog.arm(
            deadline, self._make_timeout_closure(mutant_id, spark.sparkContext)
        )
        try:
            for nodeid in mapped:
                item = self.items_by_nodeid.get(nodeid)
                if item is None:
                    logger.warning(
                        "spark-mutator: mapped test %r not collected; skipping",
                        nodeid,
                    )
                    continue
                item.ihook.pytest_runtest_protocol(item=item, nextitem=None)
                if self.mutation_call_failed:
                    # Fail-fast: the first call-phase failure halts the
                    # remaining mapped tests for this mutant.
                    break
        finally:
            self.watchdog.disarm()
        if self.driver_unresponsive_error is not None:
            raise self.driver_unresponsive_error
        return self._classify_mutant(deadline)

    def _classify_mutant(self, deadline: float) -> tuple:
        # Precedence per §2.6. An escaped MutatorJvmError / unexpected
        # exception is handled by the caller's broad except (ERRORED).
        if self.watchdog.fired and self.driver_unresponsive_error is None:
            return "TIMED_OUT", f"deadline {deadline:.1f}s exceeded"
        if self.driver_unresponsive_error is not None:
            return "ERRORED", f"driver unresponsive: {self.driver_unresponsive_error}"
        if self.mutation_call_failed:
            return (
                "KILLED",
                f"test failed under mutation: {self.mutation_failed_nodeid}",
            )
        return "SURVIVED", None

    def _make_timeout_closure(self, mutant_id: str, spark_context):
        def on_timeout():
            # Runs on the watchdog timer thread; must never raise out of it.
            try:
                escalate_cancellation(
                    spark_context,
                    mutant_id,
                    grace_period_seconds=self.watchdog.grace_period_seconds,
                    poll_interval_seconds=self.watchdog.poll_interval_seconds,
                )
            except DriverUnresponsiveError as exc:
                # Recorded for the classifier; never re-raised on this thread.
                self.driver_unresponsive_error = exc

        return on_timeout

    def _run_reset_sequence(self, mutant_id: str) -> str | None:
        """Run the §3.2 reset sequence in the exact required order.

        Steps 1-4 (cache, persistent RDDs, catalog invalidation, temp-view
        diff-and-drop) run inside ``SessionResetFacade.resetSessionState``;
        ``clearActiveMutant`` must be LAST so a lingering asynchronous job
        cannot pick up the next mutant's id by racing the reset. All failures
        are logged and returned, never raised.
        """
        error = None
        timeout = getattr(self.config, "control_channel_timeout_seconds", 5.0)
        try:
            with_control_timeout(
                self.spark.sparkContext.clearJobGroup, timeout_seconds=timeout
            )
        except (DriverUnresponsiveError, GatewayUnresponsiveError, Py4JNetworkError):
            raise
        except Exception:
            logger.exception(
                "spark-mutator: clearJobGroup failed for mutant %s", mutant_id
            )
        try:
            with_control_timeout(
                lambda: self.bridge.reset_session_state(
                    self.spark._jsparkSession, self.baseline_start_epoch_ms
                ),
                timeout_seconds=timeout,
            )
        except (DriverUnresponsiveError, GatewayUnresponsiveError, Py4JNetworkError):
            raise
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            logger.exception(
                "spark-mutator: reset_session_state failed for mutant %s", mutant_id
            )
        try:
            # Must be LAST (§3.2).
            with_control_timeout(
                lambda: self.bridge.clear_active_mutant(mutant_id),
                timeout_seconds=timeout,
            )
        except (DriverUnresponsiveError, GatewayUnresponsiveError, Py4JNetworkError):
            raise
        except Exception:
            logger.exception(
                "spark-mutator: clear_active_mutant failed for mutant %s", mutant_id
            )
        return error

    def _print_summary(self, report_path: str) -> float:
        killed = self.results.get("KILLED", 0)
        survived = self.results.get("SURVIVED", 0)
        timed_out = self.results.get("TIMED_OUT", 0)
        errored = self.results.get("ERRORED", 0)
        skipped = self.results.get("SKIPPED", 0)
        denominator = killed + timed_out + survived
        score = ((killed + timed_out) / denominator * 100.0) if denominator else 0.0
        print("spark-mutator mutation summary")
        print(f"  KILLED: {killed}")
        print(f"  SURVIVED: {survived}")
        print(f"  TIMED_OUT: {timed_out}")
        print(f"  ERRORED: {errored}")
        print(f"  SKIPPED: {skipped}")
        print(f"  mutation score: {score:.1f}%")
        print(f"  report: {report_path}")
        return score