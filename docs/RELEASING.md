# Releasing spark-mutator

The runbook for cutting a release. If you remember only one rule, make it
**Rule 1** below.

> **Status (WP-22):** the release tooling now exists — `scripts/prepare_release.sh`
> and `.github/workflows/release.yml` implement this document. This document
> remains the normative process the tooling implements. The human one-time
> prerequisites (§7) are needed before the first release regardless.

---

## 1. Base Rule

**Do not edit POM or `pyproject.toml` versions by hand. Ever.**

The only version-changing action in this repository is running
`scripts/prepare_release.sh <version>` on a green `main`. Every manual
`versions:set`, hand-edited `<version>`, or ad-hoc `git tag` is a way to
publish the wrong coordinates to immutable repositories.

---

## 2. The version model (what lives where)

| Place | Version | Who changes it |
|---|---|---|
| `main` (day to day) | `X.Y.Z-SNAPSHOT` | Nobody. Frozen between releases. |
| The **release commit** | exact `X.Y.Z` | `prepare_release.sh`, once per release |
| The **tag** `vX.Y.Z` | points at the release commit | You, right after running the script |
| Maven Central / PyPI | `X.Y.Z` | The workflow, derived **from the tag** |

The published version is derived from the tag, and a guard verifies the
tagged commit's POM agrees — a tag/POM mismatch **fails the build** instead of
publishing wrong coordinates. Tag version and POM version are two views of one
decision, never independent inputs.

---

## 3. The cycle

```
v1.0.0 published
   │
   ▼
main = 1.0.1-SNAPSHOT          ← the script bumped it for you (post-release step)
   │
   ├── start new development   ← branch (or commit on main): NO version edits
   ├── merge PRs               ← main stays 1.0.1-SNAPSHOT
   │
   ▼  "I'm ready to release"
prepare_release.sh 1.0.1       ← the ONLY time versions change (release commit)
git tag v1.0.1 && push         ← publish; script bumps main to 1.0.2-SNAPSHOT
   │
   ▼
repeat
```

- **Starting new development after a release:** touch nothing — the
  post-release bump already set the next SNAPSHOT.
- **Long-lived feature branch** open during a release: merge as usual
  afterwards. Feature branches never edit POMs, so the merge has no version
  conflict; main's SNAPSHOT simply continues.

---

## 4. The ritual (copy-paste)

```bash
git checkout main && git pull          # release ALWAYS from green main
scripts/prepare_release.sh 1.0.1      # versions:set + pyproject sync + release commit
                                      #   + post-release SNAPSHOT bump (both committed)
git tag v1.0.1
git push origin main v1.0.1           # tag push triggers publish
```

The script creates TWO commits: the release commit (`release: v1.0.1`, exact
version) and the post-release bump (`1.0.2-SNAPSHOT`). It prints the exact
tag+push commands; tagging and pushing stay human actions.

Then watch the `release` workflow: **guard → publish-maven → publish-pypi →
smoke-test**, all green before announcing anything. The workflow derives the
version FROM the tag and a guard step fails the build on a tag/POM mismatch
instead of publishing wrong coordinates.

Choosing the number (`<version>`):

| Change since last release | Bump | Example |
|---|---|---|
| Bug fixes only | patch | `1.0.0 → 1.0.1` |
| New mutator family, new Spark/Scala shim combo, new config surface (additive) | minor | `1.0.0 → 1.1.0` |
| Removed/changed public behavior or coordinates | major | `1.0.0 → 2.0.0` |

The JVM reactor and the Python wheel always release **in lockstep** — the
wheel embeds the bundles, so a PyPI version without its matching JVM release
must never exist.

---

## 5. What the tag triggers

1. **Guard** — `GITHUB_REF_NAME` must match `^v[0-9]+\.[0-9]+\.[0-9]+$`.
   Non-matching tags skip silently. (The glob `v*.*.*` alone is NOT numeric:
   `v1.0.0-rc1` matches it, and Central is immutable.)
2. **publish-maven** — signs with GPG and deploys the public surface to Maven
   Central via the Central Portal token.
3. **publish-pypi** — rebuilds the wheel from the same run's jars, publishes
   to PyPI via OIDC trusted publishing.
4. **smoke-test** — installs the wheel from PyPI and resolves the plugin +
   bundle from Central by coordinate.

First release only: the Maven job lands in the Central Portal for **manual
review** (`autoPublish=false`, set in the root POM's `release` profile) —
approve it in the Portal UI. The `smoke-test` job polls Central for up to 30
minutes so the review delay does not fail the run. Flip `autoPublish` to
`true` in a LATER change, never in the packet/change that first publishes.

---

## 6. Guard rails — things that would hurt

- **Central is immutable.** A bad release is fixed by a *new* version, never
  by re-publishing. Never move or delete a pushed `vX.Y.Z` tag.
- **Never release from a branch** or from a non-green main.
- **Never publish pre-release tags** (`v1.0.0-rc1` is rejected by the guard).
  If pre-releases are ever wanted, that is a deliberate workflow change, not a
  tag-time improvisation.
- **Secrets never enter the repo** — they live in GitHub repository settings
  (see §7).

---

## 7. One-time human prerequisites (before the very first release)

1. **Maven Central Portal** account with the verified namespace
   `io.github.wpunit13` (verification via a temporary verification key in the
   GitHub repo or DNS TXT — the Portal UI walks through it).
2. **Four GitHub repository secrets** (Settings → Secrets and variables →
   Actions): `CENTRAL_USERNAME`, `CENTRAL_PASSWORD` (Central Portal token),
   `GPG_PRIVATE_KEY` (armored), `GPG_PASSPHRASE`.
3. **PyPI trusted publisher** configured for this repo + workflow + environment
   (OIDC — no API token).
4. Published artifact surface (fixed): `spark-mutation-testing-parent` (POM),
   `spark-mutation-testing-core`, `mutator-junit5`,
   `spark-mutation-testing-maven-plugin`, and one
   `interceptor-bundle-spark-<ver>_<scala>` per supported combo. Everything
   else stays internal.
