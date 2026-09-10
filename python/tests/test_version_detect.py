"""Tests for :mod:`pytest_spark_mutator.version_detect`.

Uses ``tmp_path``-built fake jars directories; PySpark itself is never
imported (every function receives injectable overrides).
"""

import pytest

from pytest_spark_mutator.exceptions import UnsupportedSparkVersionError
from pytest_spark_mutator.version_detect import (
    VERSION_MATRIX,
    detect_scala_binary,
    detect_spark_minor,
    resolve_shim_jar_filename,
)


def _make_jars_dir(tmp_path, *filenames):
    jars_dir = tmp_path / "jars"
    jars_dir.mkdir()
    for filename in filenames:
        (jars_dir / filename).touch()
    return jars_dir


# ---------------------------------------------------------------------------
# VERSION_MATRIX
# ---------------------------------------------------------------------------


def test_version_matrix_contains_exactly_the_built_shim():
    assert VERSION_MATRIX == {"3.5_2.13": "interceptor-spark-3.5_2.13.jar"}


# ---------------------------------------------------------------------------
# detect_scala_binary
# ---------------------------------------------------------------------------


def test_detect_scala_binary_returns_213(tmp_path):
    jars_dir = _make_jars_dir(
        tmp_path, "spark-core_2.13-3.5.1.jar", "scala-library.jar"
    )
    assert detect_scala_binary(jars_dir=jars_dir) == "2.13"


def test_detect_scala_binary_returns_212(tmp_path):
    jars_dir = _make_jars_dir(tmp_path, "spark-core_2.12-3.4.2.jar")
    assert detect_scala_binary(jars_dir=jars_dir) == "2.12"


def test_detect_scala_binary_is_deterministic_with_multiple_matches(tmp_path):
    jars_dir = _make_jars_dir(
        tmp_path, "spark-core_2.13-3.5.1.jar", "spark-core_2.12-3.5.1.jar"
    )
    assert detect_scala_binary(jars_dir=jars_dir) == "2.12"


def test_detect_scala_binary_empty_dir_raises(tmp_path):
    jars_dir = _make_jars_dir(tmp_path)
    with pytest.raises(
        UnsupportedSparkVersionError,
        match="could not detect Scala binary version from pyspark installation",
    ):
        detect_scala_binary(jars_dir=jars_dir)


def test_detect_scala_binary_no_matching_filename_raises(tmp_path):
    jars_dir = _make_jars_dir(tmp_path, "hadoop-client.jar")
    with pytest.raises(UnsupportedSparkVersionError):
        detect_scala_binary(jars_dir=jars_dir)


def test_detect_scala_binary_missing_dir_raises(tmp_path):
    with pytest.raises(UnsupportedSparkVersionError):
        detect_scala_binary(jars_dir=tmp_path / "does-not-exist")


# ---------------------------------------------------------------------------
# detect_spark_minor
# ---------------------------------------------------------------------------


def test_detect_spark_minor_351():
    assert detect_spark_minor(pyspark_version="3.5.1") == "3.5"


def test_detect_spark_minor_400():
    assert detect_spark_minor(pyspark_version="4.0.0") == "4.0"


def test_detect_spark_minor_broken_version_raises():
    with pytest.raises(UnsupportedSparkVersionError):
        detect_spark_minor(pyspark_version="broken")


# ---------------------------------------------------------------------------
# resolve_shim_jar_filename
# ---------------------------------------------------------------------------


def test_resolve_shim_jar_filename_happy_path(tmp_path):
    jars_dir = _make_jars_dir(tmp_path, "spark-core_2.13-3.5.1.jar")
    assert (
        resolve_shim_jar_filename(pyspark_version="3.5.1", jars_dir=jars_dir)
        == "interceptor-spark-3.5_2.13.jar"
    )


def test_resolve_shim_jar_filename_unsupported_spark_fast_fails(tmp_path):
    jars_dir = _make_jars_dir(tmp_path, "spark-core_2.13-4.0.0.jar")
    with pytest.raises(UnsupportedSparkVersionError) as excinfo:
        resolve_shim_jar_filename(pyspark_version="4.0.0", jars_dir=jars_dir)
    message = str(excinfo.value)
    assert "4.0_2.13" in message
    assert "3.5_2.13" in message


def test_resolve_shim_jar_filename_right_spark_wrong_scala_raises(tmp_path):
    jars_dir = _make_jars_dir(tmp_path, "spark-core_2.12-3.5.1.jar")
    with pytest.raises(UnsupportedSparkVersionError) as excinfo:
        resolve_shim_jar_filename(pyspark_version="3.5.1", jars_dir=jars_dir)
    assert "3.5_2.12" in str(excinfo.value)
