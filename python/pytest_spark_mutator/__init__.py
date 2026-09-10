"""Python-side driver tooling for the spark-mutator JVM agent.

Currently provides:

- :mod:`pytest_spark_mutator.bridge` -- Py4J facade over the mutator's
  JVM-side static entry points.
- :mod:`pytest_spark_mutator.version_detect` -- Spark/Scala version
  detection and shim jar selection.

The pytest plugin hooks are delivered by a later work package.
"""

__version__ = "0.1.0"
