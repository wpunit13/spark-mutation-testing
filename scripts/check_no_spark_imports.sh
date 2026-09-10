#!/usr/bin/env bash
set -euo pipefail

# Resolve repository root relative to script directory so it can be invoked from anywhere
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${REPO_ROOT}/mutator-core/src/main/java"

# We search specifically for import statements anchored to the start of the line.
# CRITICAL: Do NOT grep for arbitrary occurrences of 'org.apache.spark'.
# mutator-core/src/main/java/io/github/wpunit13/mutator/reset/SessionResetFacade.java
# interacts with Spark entirely through java.lang.reflect and legitimately contains
# string literals like "org.apache.spark.sql.SparkSession" and Javadoc mentioning Spark,
# but contains NO Spark import statements. Grepping for unanchored 'org.apache.spark'
# would produce a false positive on SessionResetFacade.java.
PATTERN='^[[:space:]]*import[[:space:]]+org\.apache\.spark'

# Execute grep across mutator-core/src/main/java
if matches=$(grep -rnE "${PATTERN}" "${TARGET_DIR}" 2>/dev/null); then
    echo "ERROR: Forbidden Spark import(s) detected in mutator-core:" >&2
    echo "${matches}" >&2
    echo "Invariant violation: mutator-core must have zero compile-time dependencies on Spark; catalyst imports belong only in catalyst-interceptor/." >&2
    exit 1
else
    echo "OK: No forbidden Spark imports found in mutator-core."
    exit 0
fi
