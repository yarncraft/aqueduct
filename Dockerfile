# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
# No cache mount on /src/target: cargo trusts mtimes, so a persistent target dir can silently
# reuse a stale binary after the sources change.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release -p aqueduct && mkdir /state && cp target/release/aqueduct /aqueduct

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /aqueduct /usr/local/bin/aqueduct
COPY config/aqueduct.example.toml /etc/aqueduct/aqueduct.toml
# Pre-create the state dir owned by the nonroot user (uid 65532) so a fresh named volume is writable.
COPY --from=build --chown=65532:65532 /state /var/lib/aqueduct
VOLUME /var/lib/aqueduct
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s \
  CMD ["/usr/local/bin/aqueduct", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/aqueduct"]
