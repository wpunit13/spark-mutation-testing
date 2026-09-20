# Releasing spark-mutator

Runbook for cutting a release. The model: **the tag is the only version
decision; the workflow stamps it at build time.** If you remember only one
rule, make it **Rule 1** below.

> **Status (WP-22 v2):** tag-stamped releases. The release workflow derives
> the published version from the pushed tag and stamps the POM + pyproject
> versions in the build workspace (never committed). The human one-time
> prerequisites (§6) are needed before the first release regardless.

---

## 1. Base Rule

**Never hand-edit POM or `pyproject.toml` versions. Ever.**

Versions change only inside the release workflow, derived from the pushed
tag. `main` stays at its dev-marker version (`1.0.0-SNAPSHOT`, pyproject
`0.1.0`) forever — it is never published. The version-hygiene CI guard fails
any PR that touches version lines (escape hatch: a `[version-bump]` commit
subject for legitimate dependency upgrades).

---

## 2. The version model (what lives where)

| Place | Version | Who sets it |
|---|---|---|
| `main` (day to day) | dev marker: `1.0.0-SNAPSHOT` / pyproject `0.1.0` | Nobody — frozen forever |
| The tag `vX.Y.Z` | exact `X.Y.Z` | You, at release time |
| Maven Central / PyPI | `X.Y.Z` | The workflow, stamped from the tag |

One decision (the tag), one derivation (the stamp). No release commits, no
post-release bumps, no POM/tag reconciliation — the tag cannot diverge from
the published version because the published version IS the tag.

---

## 3. The ritual (copy-paste)

1. Confirm CI is green on the `main` commit you want to release.
2. Create the tag on `main`:
   - GitHub UI → Releases → Draft a new release → new tag `v1.0.0` → target
     `main` → Publish; or
   - `git tag v1.0.0 && git push origin v1.0.0`.
3. Watch the `Release` workflow: **guard → publish-maven → publish-pypi →
   smoke-test**, all green before announcing anything.
4. First release only: approve the deployment in the Central Portal UI
   (`autoPublish=false`); the smoke-test job polls Central for up to 30
   minutes so the review delay does not fail the run.

That is the whole release. No local commits, no branch, no PR — releases
never push to `main`.

Choosing the number (`X.Y.Z`):

| Change since last release | Bump | Example |
|---|---|---|
| Bug fixes only | patch | `1.0.0 → 1.0.1` |
| New mutator family, new Spark/Scala shim combo, new config surface (additive) | minor | `1.0.0 → 1.1.0` |
| Removed/changed public behavior or coordinates | major | `1.0.0 → 2.0.0` |

The JVM reactor and the Python wheel always release **in lockstep** — the
wheel embeds the bundles, so a PyPI version without its matching JVM release
must never exist.

---

## 4. What the tag triggers

1. **Guard** — `GITHUB_REF_NAME` must match `^v[0-9]+\.[0-9]+\.[0-9]+$`.
   Non-matching tags skip silently. (The glob `v*.*.*` alone is NOT numeric:
   `v1.0.0-rc1` matches it, and Central is immutable.)
2. **On-main guard** — the tagged commit must be an ancestor of `origin/main`
   (releases are cut from green `main` only).
3. **publish-maven** — stamps the POM version from the tag in the workspace
   (`mvn versions:set`, never committed), signs with GPG, deploys the public
   surface to Maven Central via the Central Portal token.
4. **publish-pypi** — stamps the pyproject version, rebuilds the wheel from
   the same run's jars, publishes to PyPI via OIDC trusted publishing.
5. **smoke-test** — installs the wheel from PyPI and resolves the plugin +
   bundle from Central by coordinate.

First release only: the Maven job lands in the Central Portal for **manual
review** (`autoPublish=false`, set in the root POM's `release` profile) —
approve it in the Portal UI. Flip `autoPublish` to `true` in a LATER change,
never in the change that first publishes.

---

## 5. Guard rails — things that would hurt

- **Central is immutable.** A bad release is fixed by a *new* version, never
  by re-publishing. Never move or delete a pushed `vX.Y.Z` tag.
- **Never release from a non-green main.** The workflow's on-main guard
  enforces the tag's commit is on `main`; greenness is your judgment call.
- **Never publish pre-release tags** (`v1.0.0-rc1` is rejected by the guard).
  If pre-releases are ever wanted, that is a deliberate workflow change, not a
  tag-time improvisation.
- **Secrets never enter the repo** — they live in GitHub repository settings
  (see §6).

---

## 6. One-time human prerequisites (before the very first release)

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

---

## 7. Design note — why there are no release commits

WP-22 v1 created a release commit (exact version) plus a post-release bump on
`main`, so the POM at the tagged commit equaled the tag. That required direct
pushes to `main` and fought branch protection; every release needed a local
script, a release branch, a PR, and a SHA-targeted tag.

V2 dissolves the constraint those commits served: the tag is the single
version decision and the workflow stamps everything else in the build
workspace. `main`'s version is a dev marker that never changes, so the
version-hygiene guard reduces to a pure hand-edit tripwire, and the release
ritual is tag-only.
