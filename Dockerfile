# Install rust toolchain, don't export it, just the binary
FROM python:3.9.2
WORKDIR /root

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh && \
    chmod +x rustup.sh && \
    ./rustup.sh -y

ENV PATH="/root/.cargo/bin:${PATH}"

RUN mkdir -p /project/lt-kafka
WORKDIR /project/lt-kafka

# Install required libraries
RUN apt update && apt install libsasl2-dev

# Create blank lib.rs for build to succeed
COPY Cargo.lock .
COPY Cargo.toml .
# Build dependencies
RUN mkdir src && \
    touch src/lib.rs && \
    cargo b --release && \
    cargo clean && \
    rm -r src

COPY src src

RUN cargo b --release

FROM python:3.9.2
COPY --from=0 /project/lt-kafka/target/release/liblt_kafka.so /lib/liblt_kafka.so