FROM ubuntu:22.04

# Basic build tools and dependencies for RocksDB
RUN apt-get update && apt-get install -y \
    build-essential \
    g++ \
    cmake \
    git \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
 && rm -rf /var/lib/apt/lists/*

# Build RocksDB 7.10.2 from source
WORKDIR /opt
RUN git clone --branch v7.10.2 --depth 1 https://github.com/facebook/rocksdb.git

WORKDIR /opt/rocksdb
# Build shared library (librocksdb.so) to matches Makefile’s -lrocksdb
RUN make shared_lib -j2

# Expose RocksDB location to the build
ENV ROCKSDB_DIR=/opt/rocksdb
ENV LD_LIBRARY_PATH=${ROCKSDB_DIR}:${LD_LIBRARY_PATH}

# Copy assignment code into the container
WORKDIR /app
COPY . /app

CMD ["/bin/bash"]
