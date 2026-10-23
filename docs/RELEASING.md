# Releasing

How to cut a release. The mechanics live in `scripts/release.sh`; the changelog is built from the fragments in
`changelog/unreleased/` by `scripts/changelog.sh` (see `changelog/README.md`).

## Version

Aqueduct is `0.x`. Breaking changes bump the minor version, so the first release since the `0.1.0` placeholder is
**`0.2.0`**: it carries breaking changes (see `changelog/unreleased/breaking/`). Later releases with only fixes bump the
patch version. The tag is `v<version>`, the crate version in `Cargo.toml` is `<version>`, the image tag is `<version>`
(no `v`).

The image goes to **`ghcr.io/yarncraft/aqueduct`**. Override it with `AQUEDUCT_IMAGE` for a test registry (add `AQUEDUCT_REGISTRY_ARGS=--tls-verify=false` for a plain-HTTP
one).

## Steps

Needs: Podman, `gh`, and for the push a token with `write:packages`
(`podman login ghcr.io`).

1. **Preview the notes.** `scripts/changelog.sh` prints what will be released. Every breaking change must have an
   upgrade step.
2. **Prepare the release PR.** On a branch (not `main`):

   ```sh
   scripts/release.sh prepare 0.2.0
   ```

   This folds the fragments into `CHANGELOG.md` as `## 0.2.0 (<date>)` and deletes them, sets the workspace version in
   `Cargo.toml`, and refreshes the workspace entries in `Cargo.lock` (with `cargo` if installed, else in a
   `rust:1-bookworm` container). Commit it as `chore: release 0.2.0` and open the PR. Review the notes in the diff.
3. **Merge the PR** after the usual review and green CI on the exact head.
4. **Check the tree** on the merged `main`:

   ```sh
   git switch main && git pull --ff-only
   scripts/release.sh check 0.2.0
   ```

   It fails if the version or changelog section is missing, fragments remain, the tree is dirty, `HEAD` is not
   `origin/main`, or the tag exists.
5. **Build and publish the image** from that commit:

   ```sh
   scripts/release.sh image 0.2.0 --push
   ```

   With `--push` it first runs `check` (so only `origin/main` after the release PR can be published) and refuses if
   `ghcr.io/yarncraft/aqueduct:0.2.0` already exists in the registry. The existence probe fails safe: only a
   "manifest unknown" / "name unknown" / "not found" answer allows the push; a login, network or rate-limit error stops
   it with the message printed. ghcr may answer `denied` for a package that was never published; if so, check your login,
   then rerun once with `AQUEDUCT_FIRST_PUSH=1`, which accepts `denied` as "not published yet" (never use it afterwards). The build context is `git archive HEAD`, so
   git-ignored files in your checkout cannot reach the image. The image carries the
   `org.opencontainers.image.{version,revision,source}` labels and is built for the **host architecture only**
   (linux/amd64 on the usual release machine); there is no arm64 build. The digest is printed and saved in
   `dist/aqueduct-0.2.0.digest` (git-ignored). Deploy by that digest (`ghcr.io/yarncraft/aqueduct@sha256:...`), not by tag.
6. **Smoke-test the published image:**

   ```sh
   scripts/release.sh smoke 0.2.0
   ```

   It removes the local build, pulls the image by its recorded digest, runs it with Postgres 16 and Kafka in a
   throwaway pod, and waits for the image's own health probe (`aqueduct --healthcheck`, a GET of `/healthz` that must
   return 200). Then follow the upgrade guide.
7. **Tag and release.** Only now:

   ```sh
   git tag -a v0.2.0 -m "Aqueduct 0.2.0" && git push origin v0.2.0
   scripts/release.sh notes 0.2.0 > dist/notes-0.2.0.md
   gh release create v0.2.0 --title "Aqueduct 0.2.0" --notes-file dist/notes-0.2.0.md
   ```

   `notes` prints the changelog section for that version plus a **Container image** block with the digest recorded in
   step 5.

If step 5 or 6 fails, fix forward with a new patch version. Do not move a published tag or overwrite a pushed image tag.
