"""Configuration loading for spark-mutator.

Reads the ``[tool.spark-mutator]`` table from a TOML file (normally
``pyproject.toml``). A missing file or a missing table is not an error: the
plugin must work zero-config, so an all-defaults instance is returned.
Unknown keys are ignored silently (forward compatibility) but logged at
debug level.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

try:
    import tomllib  # Python 3.11+
except ImportError:  # pragma: no cover - exercised only on Python < 3.11
    import tomli as tomllib

logger = logging.getLogger(__name__)

_KNOWN_KEYS = frozenset(
    {
        "target_modules",
        "excluded_mutators",
        "timeout_multiplier",
        "min_mutation_score",
        "max_errored_count",
        "max_not_applied_ratio",
        "output_dir",
        "requeue_on_crash",
        "control_channel_timeout_seconds",
        "per_test_attribution",
    }
)


@dataclass(frozen=True)
class SparkMutatorConfig:
    """User-facing spark-mutator settings (project specification §8)."""

    target_modules: list[str] = field(default_factory=list)
    excluded_mutators: list[str] = field(default_factory=list)
    timeout_multiplier: float = 2.0
    min_mutation_score: float = 80.0
    max_errored_count: int = 0
    max_not_applied_ratio: float = 0.2
    output_dir: str = "target/spark-mutator-reports"
    requeue_on_crash: bool = False
    control_channel_timeout_seconds: float = 5.0
    per_test_attribution: bool = False

    @classmethod
    def from_toml(cls, path) -> "SparkMutatorConfig":
        """Load settings from the ``[tool.spark-mutator]`` table of a TOML file.

        A missing file or a missing ``[tool.spark-mutator]`` table yields an
        all-defaults instance so the plugin works zero-config. Unknown keys
        are ignored (logged at debug level). Values are coerced to the
        declared field types; an uncoercible value or an out-of-range
        ``timeout_multiplier`` / ``min_mutation_score`` raises
        :class:`ValueError` naming the offending key.
        """
        try:
            with open(path, "rb") as handle:
                data = tomllib.load(handle)
        except FileNotFoundError:
            logger.debug(
                "spark-mutator config file %s not found; using defaults", path
            )
            return cls()

        tool = data.get("tool")
        table = tool.get("spark-mutator") if isinstance(tool, dict) else None
        if not isinstance(table, dict):
            logger.debug(
                "no [tool.spark-mutator] table in %s; using defaults", path
            )
            return cls()

        for key in sorted(set(table) - _KNOWN_KEYS):
            logger.debug(
                "ignoring unknown key %r in [tool.spark-mutator] "
                "(forward compatibility)",
                key,
            )

        kwargs = {}
        if "target_modules" in table:
            kwargs["target_modules"] = _coerce_str_list(
                "target_modules", table["target_modules"]
            )
        if "excluded_mutators" in table:
            kwargs["excluded_mutators"] = _coerce_str_list(
                "excluded_mutators", table["excluded_mutators"]
            )
        if "timeout_multiplier" in table:
            kwargs["timeout_multiplier"] = _coerce_float(
                "timeout_multiplier", table["timeout_multiplier"]
            )
        if "min_mutation_score" in table:
            kwargs["min_mutation_score"] = _coerce_float(
                "min_mutation_score", table["min_mutation_score"]
            )
        if "max_errored_count" in table:
            kwargs["max_errored_count"] = _coerce_int(
                "max_errored_count", table["max_errored_count"]
            )
        if "max_not_applied_ratio" in table:
            kwargs["max_not_applied_ratio"] = _coerce_float(
                "max_not_applied_ratio", table["max_not_applied_ratio"]
            )
        if "output_dir" in table:
            kwargs["output_dir"] = table["output_dir"]
        if "requeue_on_crash" in table:
            kwargs["requeue_on_crash"] = _coerce_bool(
                "requeue_on_crash", table["requeue_on_crash"]
            )
        if "control_channel_timeout_seconds" in table:
            kwargs["control_channel_timeout_seconds"] = _coerce_float(
                "control_channel_timeout_seconds",
                table["control_channel_timeout_seconds"],
            )
        if "per_test_attribution" in table:
            kwargs["per_test_attribution"] = _coerce_bool(
                "per_test_attribution", table["per_test_attribution"]
            )

        config = cls(**kwargs)

        if config.timeout_multiplier <= 0:
            raise ValueError(
                f"timeout_multiplier must be > 0, got {config.timeout_multiplier!r}"
            )
        if not 0 <= config.min_mutation_score <= 100:
            raise ValueError(
                f"min_mutation_score must be within [0, 100], "
                f"got {config.min_mutation_score!r}"
            )
        if config.control_channel_timeout_seconds <= 0:
            raise ValueError(
                f"control_channel_timeout_seconds must be > 0, "
                f"got {config.control_channel_timeout_seconds!r}"
            )
        return config


def _coerce_bool(key: str, value) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(
        f"invalid value for {key!r} in [tool.spark-mutator]: "
        f"expected a boolean, got {type(value).__name__}"
    )


def _coerce_int(key: str, value) -> int:
    if isinstance(value, bool):
        raise ValueError(
            f"invalid value for {key!r} in [tool.spark-mutator]: "
            f"expected an integer, got a boolean"
        )
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"invalid value for {key!r} in [tool.spark-mutator]: "
            f"{value!r} cannot be coerced to int"
        ) from None


def _coerce_float(key: str, value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"invalid value for {key!r} in [tool.spark-mutator]: "
            f"{value!r} cannot be coerced to float"
        ) from None


def _coerce_str_list(key: str, value) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(
            f"invalid value for {key!r} in [tool.spark-mutator]: "
            f"expected a list of strings, got {type(value).__name__}"
        )
    return [str(item) for item in value]
