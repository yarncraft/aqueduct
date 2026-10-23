- **The container image is published to GitHub Packages** (#244). `ghcr.io/yarncraft/aqueduct` is pushed on every
  commit to `main` (`:latest`, `:main`, `:sha-<short>`) and on `v*` tags (`:<version>`, `:<major>.<minor>`), so
  `docker run ghcr.io/yarncraft/aqueduct:latest` works without a local build. Pin a `sha-` or version tag in production.
