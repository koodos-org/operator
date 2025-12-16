FROM rust:1.87-bullseye

WORKDIR /
RUN cargo new app
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

COPY src/ src/
COPY assets/ assets/

RUN touch ./src/main.rs && cargo build --release

# Run the binary
CMD ["./target/release/ood-controller", "controller", "all"]
