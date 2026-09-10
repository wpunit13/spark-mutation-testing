"""Tests for :mod:`pytest_spark_mutator.config`.

Writes real TOML files into pytest's ``tmp_path``; no Spark, no JVM.
"""

import logging

import pytest

from pytest_spark_mutator.config import SparkMutatorConfig


def _write(tmp_path, text):
    path = tmp_path / "pyproject.toml"
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_full_table_loads_every_field(tmp_path):
    path = _write(
        tmp_path,
        """\
[tool.spark-mutator]
target_modules = ["my_pipeline.transforms"]
excluded_mutators = ["CrossJoinMutator"]
timeout_multiplier = 3.5
min_mutation_score = 90.0
output_dir = "target/mutator-out"
""",
    )
    config = SparkMutatorConfig.from_toml(path)
    assert config.target_modules == ["my_pipeline.transforms"]
    assert config.excluded_mutators == ["CrossJoinMutator"]
    assert config.timeout_multiplier == 3.5
    assert config.min_mutation_score == 90.0
    assert config.output_dir == "target/mutator-out"


def test_missing_table_returns_all_defaults(tmp_path):
    path = _write(tmp_path, '[project]\nname = "example"\n')
    config = SparkMutatorConfig.from_toml(path)
    assert config == SparkMutatorConfig()
    assert config.timeout_multiplier == 2.0
    assert config.min_mutation_score == 80.0
    assert config.output_dir == "target/spark-mutator-reports"
    assert config.target_modules == []
    assert config.excluded_mutators == []


def test_nonexistent_path_returns_all_defaults(tmp_path):
    config = SparkMutatorConfig.from_toml(tmp_path / "does-not-exist.toml")
    assert config == SparkMutatorConfig()


def test_unknown_key_is_ignored_and_logged_at_debug(tmp_path, caplog):
    path = _write(
        tmp_path,
        '[tool.spark-mutator]\nsome_future_key = "x"\n',
    )
    with caplog.at_level(logging.DEBUG, logger="pytest_spark_mutator.config"):
        config = SparkMutatorConfig.from_toml(path)
    assert config == SparkMutatorConfig()
    assert any(
        "some_future_key" in record.getMessage() for record in caplog.records
    )


# ---------------------------------------------------------------------------
# Coercion and validation failures
# ---------------------------------------------------------------------------


def test_timeout_multiplier_uncoercible_string_raises(tmp_path):
    path = _write(tmp_path, '[tool.spark-mutator]\ntimeout_multiplier = "fast"\n')
    with pytest.raises(ValueError, match="timeout_multiplier"):
        SparkMutatorConfig.from_toml(path)


def test_timeout_multiplier_zero_raises(tmp_path):
    path = _write(tmp_path, "[tool.spark-mutator]\ntimeout_multiplier = 0\n")
    with pytest.raises(ValueError, match="timeout_multiplier"):
        SparkMutatorConfig.from_toml(path)


def test_min_mutation_score_above_100_raises(tmp_path):
    path = _write(tmp_path, "[tool.spark-mutator]\nmin_mutation_score = 101\n")
    with pytest.raises(ValueError, match="min_mutation_score"):
        SparkMutatorConfig.from_toml(path)


def test_min_mutation_score_zero_is_accepted(tmp_path):
    path = _write(tmp_path, "[tool.spark-mutator]\nmin_mutation_score = 0\n")
    config = SparkMutatorConfig.from_toml(path)
    assert config.min_mutation_score == 0.0


# ---------------------------------------------------------------------------
# Additional coercion pinning
# ---------------------------------------------------------------------------


def test_timeout_multiplier_int_is_coerced_to_float(tmp_path):
    path = _write(tmp_path, "[tool.spark-mutator]\ntimeout_multiplier = 2\n")
    config = SparkMutatorConfig.from_toml(path)
    assert config.timeout_multiplier == 2.0
    assert isinstance(config.timeout_multiplier, float)


def test_min_mutation_score_100_is_accepted(tmp_path):
    path = _write(tmp_path, "[tool.spark-mutator]\nmin_mutation_score = 100\n")
    config = SparkMutatorConfig.from_toml(path)
    assert config.min_mutation_score == 100.0


def test_negative_timeout_multiplier_raises(tmp_path):
    path = _write(tmp_path, "[tool.spark-mutator]\ntimeout_multiplier = -1.0\n")
    with pytest.raises(ValueError, match="timeout_multiplier"):
        SparkMutatorConfig.from_toml(path)


def test_list_field_with_bare_string_raises(tmp_path):
    path = _write(tmp_path, '[tool.spark-mutator]\ntarget_modules = "oops"\n')
    with pytest.raises(ValueError, match="target_modules"):
        SparkMutatorConfig.from_toml(path)
