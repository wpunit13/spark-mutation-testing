"""WP-22: wheel packaging tests.

Asserts (a) bundle_jars output is deterministic across runs and (b) the
built wheel embeds every VERSION_MATRIX jar (both combo jars post-WP-19).
"""

import ast
import zipfile

import pytest

from build_hooks.bundle_jars import bundle, find_repo_root


def _load_version_matrix():
    """VERSION_MATRIX from the installed package, else AST-parse the source."""
    try:
        from pytest_spark_mutator.version_detect import VERSION_MATRIX

        return dict(VERSION_MATRIX)
    except ImportError:
        pass
    source = find_repo_root() / "python" / "pytest_spark_mutator" / "version_detect.py"
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "VERSION_MATRIX":
                    return ast.literal_eval(node.value)
    raise RuntimeError(f"VERSION_MATRIX not found in {source}")


MATRIX = _load_version_matrix()

REPO_ROOT = find_repo_root()
JARS_DIR = REPO_ROOT / "python" / "pytest_spark_mutator" / "jars"


def test_bundle_is_deterministic(tmp_path):
    """Two consecutive bundle() runs copy byte-identical files."""
    if not list((REPO_ROOT / "catalyst-interceptor").glob("*/target/*.jar")):
        pytest.skip("Maven build output absent (run mvn -B clean install first)")
    first = {str(p): p.read_bytes() for p in bundle(REPO_ROOT)}
    second = {str(p): p.read_bytes() for p in bundle(REPO_ROOT)}
    assert set(first) == set(second)
    assert first == second  # paths AND contents identical


@pytest.mark.skipif(
    not list(JARS_DIR.glob("*.jar")),
    reason="jars not bundled yet (run bundle_jars.py or build the wheel)",
)
def test_wheel_embeds_every_matrix_jar():
    """The wheel (if present in python/dist) embeds one jar per combo."""
    wheels = sorted((REPO_ROOT / "python" / "dist").glob("*.whl"))
    if not wheels:
        pytest.skip("no wheel built in python/dist")
    with zipfile.ZipFile(wheels[-1]) as wheel:
        names = wheel.namelist()
    for target in MATRIX.values():
        assert f"pytest_spark_mutator/jars/{target}" in names, (
            f"{target} missing from wheel"
        )
