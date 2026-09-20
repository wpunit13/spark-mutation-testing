#!/usr/bin/env bash
set -euo pipefail

# WP-22 shade-integrity gate (packet DESIGN §6).
#
# The Py4J bridge calls io.github.wpunit13.mutator.* by exact FQCN, so the
# shaded bundles MUST keep first-party classes under their exact packages.
# This gate fails if any bundle:
#   - is missing a required first-party class at its exact path, or
#   - carries a duplicate (relocated) copy of a first-party class, or
#   - has a PlanMutatorShim services file naming a non-first-party impl.
# Relocation of THIRD-party classes is allowed and not checked.
#
# Usage: scripts/check_shade_integrity.sh   (after mvn package/install)

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fail=0
checked=0

required_classes=(
  io/github/wpunit13/mutator/MutantRegistry.class
  io/github/wpunit13/mutator/MutatorSparkExtension.class
  io/github/wpunit13/mutator/dispatch/ShimDispatcher.class
  io/github/wpunit13/mutator/api/PlanMutatorShim.class
)
services_file="META-INF/services/io.github.wpunit13.mutator.api.PlanMutatorShim"

shopt -s nullglob
# BUNDLE jars only — the thin shim artifacts (interceptor-spark-*_<version>.jar)
# legitimately contain none of the first-party classes; the bundles shade them all.
jars=("$repo_root"/catalyst-interceptor/interceptor-bundle*/target/interceptor-spark-*.jar)
shopt -u nullglob

if [ ${#jars[@]} -eq 0 ]; then
  echo "FAIL: no bundle jars found under catalyst-interceptor/*/target/. Run mvn -B clean install first." >&2
  exit 1
fi

for jar in "${jars[@]}"; do
  case "$jar" in
    *-javadoc.jar|*-sources.jar) continue ;;
  esac
  checked=$((checked + 1))

  name="$(basename "$jar")"
  echo "== $name"
  entries="$(unzip -Z1 "$jar")"

  for cls in "${required_classes[@]}"; do
    count="$(grep -c "^${cls}$" <<<"$entries" || true)"
    if [ "$count" -ne 1 ]; then
      echo "FAIL: $cls present $count time(s) — must be exactly once at its exact first-party path (no relocation)" >&2
      fail=1
    fi
  done

  if ! grep -q "^${services_file}$" <<<"$entries"; then
    echo "FAIL: missing $services_file" >&2
    fail=1
  else
    content="$(unzip -p "$jar" "$services_file" 2>/dev/null || true)"
    if [ -z "$content" ]; then
      echo "FAIL: $services_file is empty" >&2
      fail=1
    fi
    while IFS= read -r line; do
      [ -z "$line" ] && continue
      case "$line" in
        io.github.wpunit13.mutator.*) ;;
        *)
          echo "FAIL: services file names a non-first-party (relocated?) impl: $line" >&2
          fail=1
          ;;
      esac
    done <<<"$content"
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "shade-integrity: FAILED" >&2
  exit 1
fi
echo "shade-integrity: OK ($checked bundle(s) checked)"
