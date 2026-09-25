#!/usr/bin/env bash
# verify-release.sh — assert every consumer-facing coordinate of a release
# resolves from the target repository BEFORE announcing the release.
#
# Motivation (the 1.0.0 incident): the reactor built cleanly and the release
# published a SUBSET of the modules — the maven-plugin resolved a coordinate
# that was never published, so every downstream consumer's CI-grade path died
# on first use while all in-repo tests stayed green. In-repo tests run against
# the reactor; only a coordinate-set check against the target repository
# catches a partial publish.
#
# Usage:
#   verify-release.sh <version>                # check against Maven Central
#   verify-release.sh <version> --repo <id::url>  # check against a specific repository
#
# Exit codes: 0 = all coordinates resolve; 1 = at least one missing.
#
# The list below is the CONTRACT with consumers: the quick-start coordinates
# (parent, core, mutator-junit5, maven-plugin), the aggregator POM, and the
# three self-contained interceptor bundles the plugin and the Python wheel
# resolve. Extend it whenever a new consumer-facing module is added.
set -u

VERSION="${1:?usage: verify-release.sh <version> [--repo <id::url>]}"
REPO_URL=""
if [ "${2:-}" = "--repo" ]; then
  REPO_URL="${3:?--repo requires a repository url (id::url)}"
fi

GROUP="io.github.wpunit13"
ARTIFACTS=(
  spark-mutation-testing-parent
  spark-mutation-testing-core
  mutator-junit5
  spark-mutation-testing-maven-plugin
  catalyst-interceptor
  interceptor-bundle-spark-3.5_2.12
  interceptor-bundle-spark-3.5_2.13
  interceptor-bundle-spark-4.2_2.13
)

missing=()
for artifact in "${ARTIFACTS[@]}"; do
  args=(-B -q dependency:get -Dartifact="$GROUP:$artifact:$VERSION")
  if [ -n "$REPO_URL" ]; then
    args+=(-DremoteRepositories="$REPO_URL")
  fi
  if ! mvn "${args[@]}"; then
    missing+=("$GROUP:$artifact:$VERSION")
  fi
done

if [ "${#missing[@]}" -gt 0 ]; then
  echo "ERROR: ${#missing[@]} of ${#ARTIFACTS[@]} release coordinates NOT resolvable:" >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

echo "All ${#ARTIFACTS[@]} release coordinates resolve from ${REPO_URL:-Maven Central}."
