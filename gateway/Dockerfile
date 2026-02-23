FROM rust:latest AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY control-plane ./control-plane
COPY gateway ./gateway

RUN cargo build -p gateway --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/gateway /usr/local/bin/gateway
EXPOSE 8080
CMD ["gateway"]
