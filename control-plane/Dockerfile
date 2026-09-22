FROM rust:1.94.1 AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY control-plane ./control-plane
COPY gateway ./gateway

RUN cargo build -p control-plane --release --locked

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/control-plane /usr/local/bin/control-plane
EXPOSE 8000
CMD ["control-plane"]
