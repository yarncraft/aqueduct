# syntax=docker/dockerfile:1
# rust:1-bookworm, pinned by digest (multi-arch index). Bump deliberately.
FROM rust:1-bookworm@sha256:5c1a1732ac3f77635b24d397ceb67b4d9ea61de348f68c0aa54a79fd01e65c69 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
# No cache mount on /src/target: cargo trusts mtimes, so a persistent target dir can silently
# reuse a stale binary after the sources change.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --locked -p aqueduct && mkdir /state && cp target/release/aqueduct /aqueduct

# gcr.io/distroless/cc-debian12:nonroot, pinned by digest.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build /aqueduct /usr/local/bin/aqueduct
COPY config/aqueduct.example.toml /etc/aqueduct/aqueduct.toml
# Pre-create the state dir owned by the nonroot user (uid 65532) so a fresh named volume is writable.
COPY --from=build --chown=65532:65532 /state /var/lib/aqueduct
VOLUME /var/lib/aqueduct
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s \
  CMD ["/usr/local/bin/aqueduct", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/aqueduct"]
