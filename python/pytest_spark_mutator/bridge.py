"""Py4J facade over the spark-mutator JVM entry points.

Every JVM call is wrapped so that no :class:`py4j.protocol.Py4JJavaError`
ever escapes this class; callers see :class:`MutatorJvmError` instead, with
the Java-side message available on ``.java_message``.
"""

from __future__ import annotations

import json
import re

from py4j.protocol import Py4JJavaError

from .exceptions import MutatorJvmError

_MUTANT_ID_PATTERN = re.compile(r"^[0-9a-f]{16}$")

_VALID_STATUSES = frozenset({"KILLED", "SURVIVED", "TIMED_OUT", "ERRORED", "SKIPPED"})


class MutatorBridge:
    """Thin facade over the mutator's JVM-side static entry points.

    The caller is responsible for having constructed the underlying gateway
    with ``auto_convert=False`` and ``auto_field=False``; this class does not
    construct the gateway itself. All JVM access uses exact static-method
    call syntax (e.g. ``MutantRegistry.getInstance().setActiveMutant(id)``),
    and only cleanly bridged types (String, boolean, long, int) plus JSON
    strings cross the boundary.
    """

    def __init__(self, jvm):
        self._jvm = jvm

    @property
    def _mutator(self):
        """The ``io.github.wpunit13.mutator`` package root on the JVM view."""
        return self._jvm.io.github.wpunit13.mutator

    @staticmethod
    def _validate_mutant_id(mutant_id) -> None:
        if not isinstance(mutant_id, str) or not _MUTANT_ID_PATTERN.match(mutant_id):
            raise ValueError(
                "mutant_id must be a 16-character lowercase hex string, "
                f"got {mutant_id!r}"
            )

    @staticmethod
    def _translate(jvm_call: str, original: Py4JJavaError) -> MutatorJvmError:
        # str() on a Py4JJavaError round-trips to the JVM to render the Java
        # stack trace. That call itself can fail when the gateway connection
        # is already gone, or when the error carries a null java_exception
        # (older py4j lines could construct one even though 0.10.x cannot).
        # Never let message extraction mask the MutatorJvmError: fall back to
        # the locally stored errmsg, then to repr, in that order.
        try:
            java_message = str(original)
        except Exception:
            java_message = getattr(original, "errmsg", "") or repr(original)
        return MutatorJvmError(f"JVM call failed: {jvm_call}", java_message)

    # ------------------------------------------------------------------
    # MutantRegistry
    # ------------------------------------------------------------------

    def set_active_mutant(self, mutant_id: str) -> None:
        self._validate_mutant_id(mutant_id)
        try:
            self._mutator.MutantRegistry.getInstance().setActiveMutant(mutant_id)
        except Py4JJavaError as original:
            raise self._translate(
                "MutantRegistry.getInstance().setActiveMutant", original
            ) from original

    def clear_active_mutant(self, mutant_id: str) -> None:
        self._validate_mutant_id(mutant_id)
        try:
            self._mutator.MutantRegistry.getInstance().clearActiveMutant(mutant_id)
        except Py4JJavaError as original:
            raise self._translate(
                "MutantRegistry.getInstance().clearActiveMutant", original
            ) from original

    def get_active_mutant_or_none(self) -> str | None:
        try:
            return self._mutator.MutantRegistry.getInstance().getActiveMutantOrNull()
        except Py4JJavaError as original:
            raise self._translate(
                "MutantRegistry.getInstance().getActiveMutantOrNull", original
            ) from original

    def reset(self) -> None:
        try:
            self._mutator.MutantRegistry.getInstance().reset()
        except Py4JJavaError as original:
            raise self._translate(
                "MutantRegistry.getInstance().reset", original
            ) from original

    # ------------------------------------------------------------------
    # MutationCatalogAccess
    # ------------------------------------------------------------------

    def get_full_catalog(self) -> list[dict]:
        try:
            raw = self._mutator.catalog.MutationCatalogAccess.getFullCatalogJson()
        except Py4JJavaError as original:
            raise self._translate(
                "MutationCatalogAccess.getFullCatalogJson", original
            ) from original
        return json.loads(raw)

    def get_mapped_test_ids(self, mutant_id: str) -> list[str]:
        self._validate_mutant_id(mutant_id)
        try:
            raw = self._mutator.catalog.MutationCatalogAccess.getMappedTestIdsJson(
                mutant_id
            )
        except Py4JJavaError as original:
            raise self._translate(
                "MutationCatalogAccess.getMappedTestIdsJson", original
            ) from original
        return json.loads(raw)

    # ------------------------------------------------------------------
    # ReportSink
    # ------------------------------------------------------------------

    def record_outcome(
        self, mutant_id: str, status: str, elapsed_millis: int, failure_detail: str | None
    ) -> None:
        self._validate_mutant_id(mutant_id)
        if status not in _VALID_STATUSES:
            raise ValueError(
                f"Invalid status {status!r}: must be one of {sorted(_VALID_STATUSES)}"
            )
        if (
            not isinstance(elapsed_millis, int)
            or isinstance(elapsed_millis, bool)
            or elapsed_millis < 0
        ):
            raise ValueError(
                f"elapsed_millis must be a non-negative int, got {elapsed_millis!r}"
            )
        try:
            self._mutator.report.ReportSink.recordOutcome(
                mutant_id, status, elapsed_millis, failure_detail
            )
        except Py4JJavaError as original:
            raise self._translate("ReportSink.recordOutcome", original) from original

    def finalize_reports(self) -> str:
        try:
            return self._mutator.report.ReportSink.finalizeAndWriteReports()
        except Py4JJavaError as original:
            raise self._translate(
                "ReportSink.finalizeAndWriteReports", original
            ) from original

    # ------------------------------------------------------------------
    # SessionResetFacade
    # ------------------------------------------------------------------

    def reset_session_state(self, jsparksession, since_timestamp_millis: int) -> dict:
        try:
            raw = self._mutator.reset.SessionResetFacade.resetSessionState(
                jsparksession, since_timestamp_millis
            )
        except Py4JJavaError as original:
            raise self._translate(
                "SessionResetFacade.resetSessionState", original
            ) from original
        return json.loads(raw)
