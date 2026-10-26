- **Build: Rust 1.99.1 is pinned and the workspace is on edition 2024** (#262). `rust-toolchain.toml` pins the
  toolchain, `rust-version` matches it, and one `[workspace.lints]` table denies `unwrap`/`expect` outside tests.
  thiserror is on 2, each crate asks only for the tokio features it uses, and `hmac`/`sha2` gave way to `ring`.
  `route/plan.rs` and `admin.rs` are split into modules. Behaviour is unchanged. No action needed.
