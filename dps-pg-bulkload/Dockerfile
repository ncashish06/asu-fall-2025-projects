FROM postgres:14

# Use a single RUN instruction to install build dependencies, clone,
# build, and clean up to keep the final image size minimal.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git build-essential postgresql-server-dev-14 \
        libselinux1-dev liblz4-dev libpam0g-dev libkrb5-dev libz-dev libreadline-dev ca-certificates\
    # Clone, compile, and install the pg_bulkload extension
    && git clone https://github.com/ossc-db/pg_bulkload.git /pg_bulkload \
    && cd /pg_bulkload \
    && make \
    && make install \
    # Perform cleanup in the same layer to eliminate unnecessary files
    && apt-get purge -y --auto-remove \
        git build-essential postgresql-server-dev-14 \
        libselinux1-dev liblz4-dev libpam0g-dev libkrb5-dev libz-dev libreadline-dev ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /pg_bulkload

USER postgres

EXPOSE 5432
