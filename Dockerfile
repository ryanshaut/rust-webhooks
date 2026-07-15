FROM rust:1-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

COPY --from=builder /app/target/release/rust-webhooks /app/rust-webhooks

EXPOSE 3000

ENTRYPOINT ["/app/rust-webhooks"]
