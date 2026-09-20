#!/usr/bin/env bash
set -euo pipefail

# WP-19 version-hygiene merge guard (docs/RELEASING.md Rule 1).
#
# Runs as a PR-triggered CI step. Two checks, both fail with a message citing
# Rule 1 ("versions change only in the release workflow, stamped from the
# tag"):
#
#   a) STATE: the resolved project version on the PR head ends in -SNAPSHOT.
#      main is a permanent dev marker; nothing in a PR should change it.
#
#   b) DIFF: no <version> line in any pom.xml that already exists at the
#      merge-base, nor the version field in python/pyproject.toml, added or
#      removed relative to the merge-base. Newly ADDED pom files (new modules)
#      are exempt — they legitimately declare their own 1.0.0-SNAPSHOT at
#      birth. Lines whose entire version content is a single ${...} property
#      reference (e.g. <version>${project.version}</version> in
#      dependencyManagement) are exempt too: they pin no literal version, so
#      they are not a hand edit under Rule 1. Escape hatch: commits whose
#      subject contains [version-bump] are exempt (legitimate dependency
#      upgrades).
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

BASE_REF="${BASE_REF:-origin/main}"
RULE1_MSG="docs/RELEASING.md Rule 1: versions change only in the release workflow, stamped from the tag"

# --- resolve the merge base -------------------------------------------------

if ! git rev-parse --verify --quiet "$BASE_REF" >/dev/null 2>&1; then
    # CI fork-PR checkouts may not carry the base branch locally; try once.
    branch="${BASE_REF#origin/}"
    git fetch origin "$branch" >/dev/null 2>&1 || true
fi
if ! git rev-parse --verify --quiet "$BASE_REF" >/dev/null 2>&1; then
    echo "ERROR: base ref '$BASE_REF' not found; cannot compute the merge base." >&2
    exit 1
fi
MERGE_BASE="$(git merge-base HEAD "$BASE_REF")"
echo "Version-hygiene guard: HEAD=$(git rev-parse --short HEAD), base=$BASE_REF, merge-base=$MERGE_BASE"

# --- escape hatch -----------------------------------------------------------

DIFF_EXEMPT=0
if git log --format=%s "$MERGE_BASE"..HEAD | grep -q '\[version-bump\]'; then
    echo "OK: [version-bump] commit present in the PR history; the diff check is exempt (legitimate dependency upgrades)."
    DIFF_EXEMPT=1
fi

# --- (a) STATE: the head's project version must be a SNAPSHOT ---------------

pom_version="$(awk '
    /<artifactId>spark-mutation-testing-parent<\/artifactId>/ {found=1; next}
    found && /<version>/ {
        sub(/.*<version>/, ""); sub(/<\/version>.*/, ""); print; exit
    }' pom.xml)"
if [ -z "$pom_version" ]; then
    echo "ERROR: could not resolve the project version from pom.xml." >&2
    exit 1
fi

case "$pom_version" in
    *-SNAPSHOT)
        echo "OK: project version '$pom_version' is a -SNAPSHOT."
        ;;
    *)
        echo "ERROR: the PR head's project version is '$pom_version', not a -SNAPSHOT." >&2
        echo "Rule 1: $RULE1_MSG" >&2
        echo "main's version is a permanent dev marker; PRs never change it." >&2
        exit 1
        ;;
esac

# --- (b) DIFF: version lines in pre-existing poms + pyproject version -------

fail=0

while IFS= read -r pom; do
    [ -n "$pom" ] || continue
    if ! git cat-file -e "$MERGE_BASE:$pom" 2>/dev/null; then
        # Newly ADDED pom files (new modules) legitimately declare their own
        # 1.0.0-SNAPSHOT at birth — exempt.
        echo "SKIP (new module, exempt): $pom"
        continue
    fi
    version_lines="$(git diff "$MERGE_BASE" -- "$pom" | grep -E '^[+-].*<version>' || true)"
    # Exempt pure property references: <version>${...}</version> pins no
    # literal version, so adding/removing one is not a hand edit under Rule 1.
    literal_lines="$(printf '%s\n' "$version_lines" | \
        grep -vE '^[+-].*<version>[[:space:]]*\$\{[^}]*\}[[:space:]]*</version>' || true)"
    if [ -n "$literal_lines" ]; then
        echo "ERROR: <version> line(s) changed in $pom (exists at the merge base):" >&2
        echo "$literal_lines" >&2
        fail=1
    fi
done < <(git diff --name-only "$MERGE_BASE" | grep -E '(^|/)pom\.xml$' || true)

if git cat-file -e "$MERGE_BASE:python/pyproject.toml" 2>/dev/null; then
    pyproject_lines="$(git diff "$MERGE_BASE" -- python/pyproject.toml | grep -E '^[+-][[:space:]]*version[[:space:]]*=' || true)"
    if [ -n "$pyproject_lines" ]; then
        echo "ERROR: version field changed in python/pyproject.toml (exists at the merge base):" >&2
        echo "$pyproject_lines" >&2
        fail=1
    fi
fi

if [ "$fail" -ne 0 ]; then
    if [ "$DIFF_EXEMPT" -eq 1 ]; then
        echo "OK: version-line changes are exempt by a [version-bump] commit."
        exit 0
    fi
    echo "Rule 1: $RULE1_MSG" >&2
    exit 1
fi

echo "OK: no version lines changed in pre-existing poms or python/pyproject.toml."
exit 0