"""Setuptools build hook shim.

Overrides build_py to bundle the catalyst interceptor fat jar before
packaging the wheel.
"""

from __future__ import annotations

from pathlib import Path
import sys

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

try:
    from build_hooks.bundle_jars import bundle
except ImportError:
    bundle = None


class CustomBuildPy(build_py):
    """Custom build_py that bundles jars before packaging."""

    def run(self):
        if bundle is not None:
            bundle()
        super().run()


class CustomSdist(sdist):
    """Custom sdist that bundles jars before packaging."""

    def run(self):
        if bundle is not None:
            bundle()
        super().run()


setup(
    cmdclass={
        "build_py": CustomBuildPy,
        "sdist": CustomSdist,
    },
)
