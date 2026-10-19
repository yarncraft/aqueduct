# Changelog fragments

Each unreleased change is one Markdown file, so two PRs never edit the same lines of `CHANGELOG.md`.

## Adding an entry

Create `changelog/unreleased/<section>/<issue>-<slug>.md` in the same PR as the change.

- `<section>` is one of:
  - `breaking`: breaking changes and upgrade notes. Every breaking change needs one, with the upgrade step.
  - `added`
  - `changed`
  - `fixed`
- `<issue>` is the GitHub issue number. `<slug>` is a few lowercase words joined by hyphens, e.g.
  `breaking/56-kafka-topic-replication-factor.md`. If one issue needs two entries, give each its own slug.
- The file holds exactly one bullet, in the same style as the existing ones:

  ```markdown
  - **What changed, in plain words** (#56). What the user sees, why, and what to do on upgrade.
  ```

  Wrap at about 120 columns. Indent continuation lines, nested bullets and code blocks by two spaces.

To fix an entry, edit its file. Don't add a second one. The script refuses to run if a fragment sits outside the four
section folders, so a misfiled entry can't be skipped and deleted at release.

## Previewing and releasing

```sh
scripts/changelog.sh                   # print the Unreleased section, highest issue number first
scripts/changelog.sh --release 0.2.0   # write it into CHANGELOG.md as "## 0.2.0 (<date>)" and delete the fragments
```
