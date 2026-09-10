"""Detection of the running Spark/Scala combination and shim jar selection.

Every pyspark import is lazy (inside the function bodies) so this module can
be imported — and unit-tested — in environments where PySpark is not
installed.
"""

from __future__ import annotations

import re
from pathlib import Path

from .exceptions import UnsupportedSparkVersionError

VERSION_MATRIX: dict[str, str] = {"3.5_2.13": "interceptor-spark-3.5_2.13.jar"}

_SCALA_CORE_PATTERN = re.compile(r"^spark-core_(2\.12|2\.13)-.*\.jar$")


def detect_scala_binary(jars_dir=None) -> str:
    """Return the Scala binary version (``"2.12"`` or ``"2.13"``) in use.

    :param jars_dir: directory containing the pyspark distribution's bundled
        jars; when ``None`` it is resolved as ``Path(pyspark.__file__).parent
        / "jars"``.
    :raises UnsupportedSparkVersionError: if the directory is missing or no
        filename matches the spark-core Scala-suffix pattern.
    """
    if jars_dir is None:
        import pyspark

        jars_dir = Path(pyspark.__file__).parent / "jars"
    jars_dir = Path(jars_dir)
    try:
        # Sorted so the returned match is deterministic when an installation
        # somehow carries core jars for more than one Scala binary.
        filenames = sorted(entry.name for entry in jars_dir.iterdir())
    except (FileNotFoundError, NotADirectoryError):
        filenames = []
    for filename in filenames:
        match = _SCALA_CORE_PATTERN.match(filename)
        if match:
            return match.group(1)
    raise UnsupportedSparkVersionError(
        "could not detect Scala binary version from pyspark installation"
    )


def detect_spark_minor(pyspark_version=None) -> str:
    """Return the ``"<major>.<minor>"`` prefix of the running Spark version.

    :param pyspark_version: the ``pyspark.__version__`` string; when ``None``
        it is read from the installed pyspark package.
    :raises UnsupportedSparkVersionError: if the version string has fewer
        than two dot-separated components.
    """
    if pyspark_version is None:
        import pyspark

        pyspark_version = pyspark.__version__
    components = pyspark_version.split(".")
    if len(components) < 2:
        raise UnsupportedSparkVersionError(
            f"could not parse Spark version string '{pyspark_version}': "
            "expected at least '<major>.<minor>'"
        )
    return f"{components[0]}.{components[1]}"


def resolve_shim_jar_filename(pyspark_version=None, jars_dir=None) -> str:
    """Return the shim jar filename for the detected Spark/Scala combination.

    :raises UnsupportedSparkVersionError: if the combination is not present
        in :data:`VERSION_MATRIX`, or if either detection step fails.
    """
    key = f"{detect_spark_minor(pyspark_version)}_{detect_scala_binary(jars_dir)}"
    try:
        return VERSION_MATRIX[key]
    except KeyError:
        raise UnsupportedSparkVersionError(
            f"Unsupported Spark/Scala combination '{key}'. "
            f"Supported versions: {sorted(VERSION_MATRIX.keys())}"
        ) from None
