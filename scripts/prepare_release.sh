#!/usr/bin/env bash
set -euo pipefail

# WP-22 release choreography (docs/RELEASING.md Rule 1: the ONLY place
# versions change). Creates the release commit at <version>, then bumps the
# tree to the next patch SNAPSHOT and commits that too. Prints the exact
# tag+push commands; tagging/pushing stays a human action.

usage() { echo "usage: $0 <version>   # e.g. 1.0.1" >&2; exit 1; }

[ $# -eq 1 ] || usage
VERSION="$1"
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || { echo "ERROR: version must be X.Y.Z (numeric), got '$VERSION'" >&2; usage; }

# --- guard rails ------------------------------------------------------------
branch="$(git rev-parse --abbrev-ref HEAD)"
[ "$branch" = "main" ] || { echo "ERROR: release only from main (on '$branch')" >&2; exit 1; }
[ -z "$(git status --porcelain)" ] || { echo "ERROR: dirty working tree — commit or stash first" >&2; exit 1; }
if git rev-parse -q --verify "refs/tags/v$VERSION" >/dev/null; then
  echo "ERROR: tag v$VERSION already exists — Central is immutable, never re-publish" >&2
  exit 1
fi

MAJOR="${VERSION%%.*}"; rest="${VERSION#*.}"
MINOR="${rest%%.*}"; PATCH="${rest#*.}"
NEXT="$MAJOR.$MINOR.$((PATCH + 1))"

# --- release commit ---------------------------------------------------------
JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17)}" mvn -B -q versions:set -DnewVersion="$VERSION" -DgenerateBackupPoms=false

sed -i.bak -E 's/^version = "[^"]*"/version = "'"$VERSION"'"/' python/pyproject.toml && rm -f python/pyproject.toml.bak
sed -i.bak 's/1\.0\.0-SNAPSHOT/'"$VERSION"'/g' docs/developer-guide.md && rm -f docs/developer-guide.md.bak

git add pom.xml '**/pom.xml' python/pyproject.toml docs/developer-guide.md
git commit -m "release: v$VERSION"
RELEASE_SHA="$(git rev-parse HEAD)"

# --- post-release bump ------------------------------------------------------
mvn -B -q versions:set -DnewVersion="$NEXT-SNAPSHOT" -DgenerateBackupPoms=false
sed -i.bak -E 's/^version = "[^"]*"/version = "'"$NEXT"'"/' python/pyproject.toml && rm -f python/pyproject.toml.bak

git add pom.xml '**/pom.xml' python/pyproject.toml
git commit -m "post-release bump: ${NEXT}-SNAPSHOT"

echo
echo "Release commit: $RELEASE_SHA"
echo "Now run:"
echo "  git tag v$VERSION $RELEASE_SHA"
echo "  git push origin main v$VERSION"
