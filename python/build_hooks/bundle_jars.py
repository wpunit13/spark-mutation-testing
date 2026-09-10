"""Bundle Maven build artifacts into pytest_spark_mutator package data.

This script assumes that ``mvn -B clean install`` (or at least
``mvn -pl catalyst-interceptor/interceptor-bundle -am package``) has already run,
and that it must never invoke Maven itself — the two toolchains stay decoupled
and CI sequences them.
"""

from __future__ import annotations

import ast
from pathlib import Path
import shutil
import sys

_LAST_COPIES: list[tuple[Path, Path]] = []


def find_repo_root(start=None) -> Path:
    """Walk upward from ``start`` (or ``__file__``) to locate the repo root.

    The repository root is identified as the nearest ancestor directory containing
    both a ``pom.xml`` file and a ``python/`` subdirectory.

    :raises RuntimeError: if no matching directory is found before the filesystem root.
    """
    current = (
        Path(start).resolve() if start is not None else Path(__file__).resolve().parent
    )
    for candidate in [current, *current.parents]:
        if (candidate / "pom.xml").is_file() and (candidate / "python").is_dir():
            return candidate
    raise RuntimeError(
        f"Repository root containing pom.xml and python/ subdirectory could not be found "
        f"starting from {current}."
    )


def _load_version_matrix(repo_root: Path) -> dict[str, str]:
    """Retrieve VERSION_MATRIX dynamically from pytest_spark_mutator.version_detect."""
    # 1. Attempt normal import
    try:
        from pytest_spark_mutator.version_detect import VERSION_MATRIX

        return dict(VERSION_MATRIX)
    except (ImportError, ModuleNotFoundError):
        pass

    # 2. Attempt import with python/ on sys.path
    python_dir = repo_root / "python"
    if str(python_dir) not in sys.path:
        sys.path.insert(0, str(python_dir))
    try:
        from pytest_spark_mutator.version_detect import VERSION_MATRIX

        return dict(VERSION_MATRIX)
    except (ImportError, ModuleNotFoundError):
        pass

    # 3. Fall back to AST literal parsing to avoid dependency on uninstalled packages
    version_detect_file = python_dir / "pytest_spark_mutator" / "version_detect.py"
    if not version_detect_file.is_file():
        raise RuntimeError(
            f"Could not locate version_detect.py at {version_detect_file}"
        )

    with open(version_detect_file, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=str(version_detect_file))

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "VERSION_MATRIX":
                    return ast.literal_eval(node.value)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "VERSION_MATRIX":
                if node.value is not None:
                    return ast.literal_eval(node.value)

    raise RuntimeError(f"Could not extract VERSION_MATRIX from {version_detect_file}")


def bundle(repo_root=None) -> list[Path]:
    """Locate and copy each jar in VERSION_MATRIX into pytest_spark_mutator/jars/.

    :param repo_root: root directory of the repository; discovered automatically if None.
    :return: list of destination Path objects copied.
    :raises RuntimeError: if zero matches or multiple matches are found for any jar.
    """
    global _LAST_COPIES
    _LAST_COPIES = []

    if repo_root is None:
        root = find_repo_root()
    else:
        root = Path(repo_root).resolve()

    version_matrix = _load_version_matrix(root)
    destinations: list[Path] = []
    dest_dir = root / "python" / "pytest_spark_mutator" / "jars"

    for key, target_filename in version_matrix.items():
        pattern = f"catalyst-interceptor/*/target/{target_filename}"
        matches = sorted(root.glob(pattern))

        if len(matches) == 0:
            raise RuntimeError(
                f"No jar matching glob pattern '{pattern}' was found. "
                f"mvn -B clean install must run first."
            )
        if len(matches) > 1:
            match_list = [str(m) for m in matches]
            raise RuntimeError(
                f"Multiple matches found for glob pattern '{pattern}': {match_list}. "
                f"Stale build output from a renamed module cannot be resolved automatically."
            )

        match = matches[0]
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / target_filename

        shutil.copy2(match, dest_file)
        _LAST_COPIES.append((match, dest_file))
        destinations.append(dest_file)

    return destinations


def main() -> int:
    """CLI entry point: bundle jars and print copy operations."""
    try:
        destinations = bundle()
    except RuntimeError as err:
        print(f"Error: {err}", file=sys.stderr)
        return 1

    for src, dst in _LAST_COPIES:
        print(f"{src} -> {dst}")
    print(f"Successfully bundled {len(destinations)} jar(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
