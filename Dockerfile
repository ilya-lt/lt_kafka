# Install rust toolchain, don't export it, just the binary
FROM python:3.9.2
WORKDIR /root
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh
RUN chmod +x rustup.sh
RUN ./rustup.sh -y

RUN mkdir /project
WORKDIR /project
RUN mkdir lt-kafka
WORKDIR /project/lt-kafka

# Install required libraries
RUN apt update
RUN apt install libsasl2-dev

COPY Cargo.lock .
COPY Cargo.toml .
COPY src src

RUN /root/.cargo/bin/cargo b --release

FROM python:3.9.2
COPY --from=0 /project/lt-kafka/target/release/liblt_kafka.so /lib/liblt_kafka.so