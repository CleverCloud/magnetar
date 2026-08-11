# SPDX-License-Identifier: Apache-2.0
FROM rust@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e

# Installation is outside every measurement. The launcher records the actual
# derived image ID and dpkg inventory, then uses that ID for both references.
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
       clang=1:14.0-55.7~deb12u1 \
       llvm=1:14.0-55.7~deb12u1 \
       libclang-dev=1:14.0-55.7~deb12u1 \
       libkrb5-dev=1.20.1-2+deb12u5 \
       cmake=3.25.1-1 \
       golang-go=2:1.19~1 \
       golang-1.19-go=1.19.8-2 \
       time=1.9-0.2 \
       strace=6.1-0.1 \
       heaptrack=1.4.0-2 \
       valgrind=1:3.19.0-1 \
    && rm -rf /var/lib/apt/lists/*

# Suite fixtures invoke the CLI as well as the Docker API. Copy only the
# client; the runner owns the daemon and exposes its explicitly mounted socket.
COPY --from=docker@sha256:3f4743208d2338c934d7b8bcfbe1bb54c0b2355c510ad5e0f31c0c4a54bd704e /usr/local/bin/docker /usr/local/bin/docker

ENV RUSTUP_TOOLCHAIN=1.98.1-x86_64-unknown-linux-gnu \
    RUSTUP_AUTO_INSTALL=0 \
    LC_ALL=C \
    CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_RELEASE_DEBUG=1 \
    CARGO_PROFILE_RELEASE_STRIP=none \
    CC=clang CXX=clang++ ASM=clang AR=llvm-ar RANLIB=llvm-ranlib

# The build command supplies the hash of these exact Dockerfile bytes.
ARG MAGNETAR_DOCKERFILE_SHA256
LABEL org.opencontainers.image.base.digest="sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e" \
      magnetar.performance.docker-cli-source="docker@sha256:3f4743208d2338c934d7b8bcfbe1bb54c0b2355c510ad5e0f31c0c4a54bd704e" \
      magnetar.performance.dockerfile-sha256="${MAGNETAR_DOCKERFILE_SHA256}"
