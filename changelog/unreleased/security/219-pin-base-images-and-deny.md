- **Container base images are pinned by digest** (#219). The `Dockerfile` now references `rust:1-bookworm` and
  `distroless/cc-debian12:nonroot` by sha256 digest, so a rebuild cannot pick up a changed upstream tag. A new
  `deny.toml` adds `cargo deny check` (advisories, licenses, bans, sources) next to `cargo audit`.
