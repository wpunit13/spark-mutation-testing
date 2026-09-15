"""Tests for :mod:`pytest_spark_mutator.version_detect`.

Uses ``tmp_path``-built fake jars directories; PySpark itself is never
imported (every function receives injectable overrides).
"""

import re
from pathlib import Path

import pytest

from pytest_spark_mutator.exceptions import UnsupportedSparkVersionError
from pytest_spark_mutator.version_detect import (
    VERSION_MATRIX,
    detect_scala_binary,
    detect_spark_minor,
    resolve_shim_jar_filename,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _make_jars_dir(tmp_path, *filenames):
    jars_dir = tmp_path / "jars"
    jars_dir.mkdir()
    for filename in filenames:
        (jars_dir / filename).touch()
    return jars_dir


# ---------------------------------------------------------------------------
# VERSION_MATRIX
# ---------------------------------------------------------------------------


def test_version_matrix_covers_the_supported_combos():
    # WP-20 matrix: latest LTS line (3.5, both Scala binaries) plus the newest
    # GA minor (4.2, Scala 2.13-only). Parity with the JVM side is asserted in
    # test_supported_versions_match_the_jvm_side below.
    assert VERSION_MATRIX == {
        "3.5_2.13": "interceptor-spark-3.5_2.13.jar",
        "3.5_2.12": "interceptor-spark-3.5_2.12.jar",
        "4.2_2.13": "interceptor-spark-4.2_2.13.jar",
    }


def test_supported_versions_match_the_jvm_side():
    # WP-20 parity guard: SUPPORTED_VERSIONS (SparkVersionDetector.java, the
    # Maven-plugin detection surface) and VERSION_MATRIX keys (this wheel's
    # detection surface) must be identical sets. A combo present on one side
    # only means one path fast-fails on a version the other path would serve.
    java_source = (REPO_ROOT / "maven-plugin" / "src" / "main" / "java" / "io"
                   / "github" / "wpunit13" / "mutator" / "maven"
                   / "SparkVersionDetector.java").read_text(encoding="utf-8")
    supported = re.search(
        r"SUPPORTED_VERSIONS\s*=\s*[^;]*Set\.of\(([^)]*)\)", java_source
    )
    assert supported is not None, "SUPPORTED_VERSIONS declaration not found"
    jvm_keys = {token.strip().strip('"') for token in supported.group(1).split(",")}
    assert jvm_keys == {"3.5_2.12", "3.5_2.13", "4.2_2.13"}, (
        f"SparkVersionDetector.SUPPORTED_VERSIONS drifted: {sorted(jvm_keys)}")
    assert set(VERSION_MATRIX) == jvm_keys, (
        "VERSION_MATRIX keys must equal SparkVersionDetector.SUPPORTED_VERSIONS; "
        f"python={sorted(VERSION_MATRIX)} jvm={sorted(jvm_keys)}")


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


@pytest.mark.parametrize(
    ("scala_binary", "expected_jar"),
    [
        ("2.13", "interceptor-spark-3.5_2.13.jar"),
        ("2.12", "interceptor-spark-3.5_2.12.jar"),
    ],
    ids=["scala-2.13", "scala-2.12"],
)
def test_resolve_shim_jar_filename_covers_35_matrix_entries(
    tmp_path, scala_binary, expected_jar
):
    # WP-19: shim resolution covers every 3.5 matrix entry.
    jars_dir = _make_jars_dir(tmp_path, f"spark-core_{scala_binary}-3.5.1.jar")
    assert (
        resolve_shim_jar_filename(pyspark_version="3.5.1", jars_dir=jars_dir)
        == expected_jar
    )


def test_resolve_shim_jar_filename_covers_the_42_entry(tmp_path):
    # WP-20: a Spark 4.2 pyspark distribution resolves its own shim.
    jars_dir = _make_jars_dir(tmp_path, "spark-core_2.13-4.2.0.jar")
    assert (
        resolve_shim_jar_filename(pyspark_version="4.2.0", jars_dir=jars_dir)
        == "interceptor-spark-4.2_2.13.jar"
    )


def test_resolve_shim_jar_filename_unsupported_spark_fast_fails(tmp_path):
    jars_dir = _make_jars_dir(tmp_path, "spark-core_2.13-4.0.0.jar")
    with pytest.raises(UnsupportedSparkVersionError) as excinfo:
        resolve_shim_jar_filename(pyspark_version="4.0.0", jars_dir=jars_dir)
    message = str(excinfo.value)
    assert "4.0_2.13" in message
    assert "3.5_2.13" in message
    assert "3.5_2.12" in message


def test_resolve_shim_jar_filename_right_spark_wrong_scala_raises(tmp_path):
    # Spark 3.4 has no shim for either Scala binary; the error must name the
    # offending combination and list the supported keys.
    jars_dir = _make_jars_dir(tmp_path, "spark-core_2.13-3.4.2.jar")
    with pytest.raises(UnsupportedSparkVersionError) as excinfo:
        resolve_shim_jar_filename(pyspark_version="3.4.2", jars_dir=jars_dir)
    message = str(excinfo.value)
    assert "3.4_2.13" in message
    assert "3.5_2.13" in message
    assert "3.5_2.12" in message
