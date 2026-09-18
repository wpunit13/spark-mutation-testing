#!/usr/bin/env bash
set -euo pipefail

# One-command regression net (docs/TEST_STRATEGY.md §2). Answers
# "did I break anything?": runs every layer of the verification pyramid.
#
#   scripts/verify_all.sh            # offline Maven (-o), the normal case
#   scripts/verify_all.sh --online   # let Maven reach repositories
#
# Requires: JDK 17 (JAVA_HOME), and a Python 3 env with pyspark + pytest —
# point PYTHON_BIN at it if `python3` on PATH is not that env. PYSPARK_PYTHON
# defaults to the same interpreter (Spark 4.x requires the worker to match
# the driver's minor version).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

OFFLINE=1
for arg in "$@"; do
    case "$arg" in
        --online) OFFLINE=0 ;;
        *) echo "usage: $0 [--online]" >&2; exit 2 ;;
    esac
done
MVN_FLAG=""
[ "$OFFLINE" -eq 1 ] && MVN_FLAG="-o"

# The Python that runs the wheel suite and the e2e scripts must be the one
# with pyspark + pytest installed. Override with PYTHON_BIN=... if python3 on
# PATH is not that env (PYSPARK_PYTHON defaults to the same interpreter).
PYTHON_BIN="${PYTHON_BIN:-python3}"

# --- environment guards ------------------------------------------------------

# Check the Java Maven will actually use: $JAVA_HOME/bin/java when set, else PATH.
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/java}"
JAVA_BIN="${JAVA_BIN:-java}"
java_version=$("${JAVA_BIN}" -version 2>&1 | head -1)
echo "Java: ${java_version}"
case "${java_version}" in
    *'"17.'*) : ;;   # JDK 17 — required by every supported Spark line
    *)
        if [ "${ALLOW_NON_JDK17:-0}" != "1" ]; then
            echo "ERROR: JDK 17 is required (found: ${java_version})." >&2
            echo "Set JAVA_HOME to a JDK 17 install, or ALLOW_NON_JDK17=1 to proceed anyway." >&2
            exit 1
        fi
        ;;
esac

step() { echo; echo "=== [$1/${2}] ${3} ==="; }
TOTAL=5

# --- [1/5] Reactor tests: L1 engine units + L2 per-shim contracts + L3 in-process ---

step 1 $TOTAL "Reactor tests (L1 engine units, L2 shim goldens, L3 in-process)"
mvn ${MVN_FLAG} clean test

# --- [2/5] Install reactor artifacts for the e2e steps -----------------------

step 2 $TOTAL "Install reactor artifacts (plugin, mutator-junit5, bundles)"
mvn ${MVN_FLAG} install -DskipTests

# --- [3/5] Bundle every supported combo's jar into the wheel -----------------

step 3 $TOTAL "Bundle shim jars into the wheel"
"${PYTHON_BIN}" python/build_hooks/bundle_jars.py

# --- [4/5] Python suite: detection, wheel layout, JVM/Python matrix parity ---

step 4 $TOTAL "Python test suite"
"${PYTHON_BIN}" -m pytest python/tests/ -q

# --- [5/5] L4 end-to-end: both orchestration paths ---------------------------

step 5 $TOTAL "End-to-end proofs (PySpark path, then JVM path)"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$("${PYTHON_BIN}" -c 'import sys; print(sys.executable)')}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON}}"
echo "PYSPARK_PYTHON=${PYSPARK_PYTHON}"
"${PYTHON_BIN}" scripts/verify_e2e.py
"${PYTHON_BIN}" scripts/verify_e2e_jvm.py

echo
echo "ALL GREEN: engine units, shim goldens, in-process engine, wheel packaging,"
echo "detection parity, and both end-to-end survival/kill proofs."
