# Imagem do Postgres do Cidades para o CloudNativePG: a imagem padrão do CNPG
# (PostgreSQL 18, Debian 12) + pg_duckdb compilado do fonte. O commit é o mesmo
# do playbook de VM (infra/playbooks/postgres/group_vars/postgres.yaml), que
# instala a extensão 1.2.0, a mesma do homolog.
#
# O build compila o DuckDB inteiro: de 20 a 60 minutos, ~2 GB de RAM por job.
#
#   docker build -t ghcr.io/govhub-br/cidades-postgres:18-pgduckdb-ee38d3b infra/k8s/postgres

ARG CNPG_IMAGE=ghcr.io/cloudnative-pg/postgresql:18.6-202610260820-standard-bookworm

FROM ${CNPG_IMAGE} AS builder

ARG PG_MAJOR=18
ARG PG_DUCKDB_VERSION=ee38d3b540ecea1d93683ba99bdcec5632a21eaf
ARG MAKE_JOBS=4

USER root

# Lista do Dockerfile oficial do pg_duckdb, que também é Debian 12.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
       "postgresql-server-dev-${PG_MAJOR}" \
       bison build-essential ca-certificates cmake flex git libc++-dev libc++abi-dev \
       libcurl4-openssl-dev libglib2.0-dev liblz4-dev libreadline-dev libssl-dev \
       libstdc++-12-dev libtinfo5 libxml2-dev libxml2-utils libxslt-dev ninja-build \
       pkg-config xsltproc zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/duckdb/pg_duckdb.git /build \
  && cd /build \
  && git checkout "${PG_DUCKDB_VERSION}" \
  && git submodule update --init --recursive

WORKDIR /build
RUN make -j"${MAKE_JOBS}" PG_CONFIG="/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" \
  && make install DESTDIR=/out PG_CONFIG="/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config"


FROM ${CNPG_IMAGE}

USER root
# O DuckDB lê s3:// via libcurl (extensão httpfs).
RUN apt-get update \
  && apt-get install -y --no-install-recommends libcurl4 \
  && rm -rf /var/lib/apt/lists/*

# pg_duckdb.so, libduckdb.so, o .control e os .sql da extensão.
COPY --from=builder /out/ /

# Usuário postgres das imagens do CNPG.
USER 26
