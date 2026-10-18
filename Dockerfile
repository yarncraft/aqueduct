# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p aqueduct 2>/dev/null || cargo build --release -p aqueduct; \
    cp target/release/aqueduct /aqueduct

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /aqueduct /usr/local/bin/aqueduct
COPY config/aqueduct.example.toml /etc/aqueduct/aqueduct.toml
VOLUME /var/lib/aqueduct
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s \
  CMD ["/usr/local/bin/aqueduct", "--healthcheck"]
ENTRYPOINT ["/usr/local/bin/aqueduct"]
