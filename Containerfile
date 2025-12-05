FROM rust:1.87-bullseye

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && RUSTFLAGS='-Cllvm-args=-inline-threshold=10 -Cllvm-args=-inlinedefault-threshold=10 -Cllvm-args=-inlinehint-threshold=10' cargo build --release && rm -rf src

COPY src/ src/
COPY assets/ assets/

RUN RUSTFLAGS='-Cllvm-args=-inline-threshold=10 -Cllvm-args=-inlinedefault-threshold=10 -Cllvm-args=-inlinehint-threshold=10' cargo build --release

# Run the binary
CMD ["./target/release/ood-controller", "controller", "all"]
