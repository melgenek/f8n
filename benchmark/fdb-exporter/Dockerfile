# --- xx tool stage ---
FROM tonistiigi/xx AS xx

# --- multi-arch build stage ---
FROM golang:1.25-bookworm AS base
COPY --from=xx / /
ARG TAG
ARG TARGETOS
ARG TARGETARCH
ENV TAG=${TAG} CGO_ENABLED=1

ENV SRC_DIR=/go/src/github.com/yevhenii/myproj/f8n/benchmark/fdb-exporter
WORKDIR ${SRC_DIR}/
COPY ./go.mod ./go.sum ./main.go ./
COPY ./metrics ./metrics

ARG FDB_VERSION=7.3.69
RUN if [ "${TARGETARCH}" = "amd64" ]; then \
      FDB_ARCH="amd64"; \
    elif [ "${TARGETARCH}" = "arm64" ]; then \
      FDB_ARCH="aarch64"; \
    else \
      echo "Unsupported architecture: ${TARGETARCH}"; \
      exit 1; \
    fi \
    && wget "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_${FDB_ARCH}.deb" \
    && dpkg -i foundationdb-clients_${FDB_VERSION}-1_${FDB_ARCH}.deb

FROM base as build
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    CGO_CFLAGS="-DSQLITE_ENABLE_DBSTAT_VTAB=1 -DSQLITE_USE_ALLOCA=1" xx-go build -ldflags "-extldflags -s" -o bin/exporter

# --- multi-arch package stage ---
FROM debian:bookworm-slim AS package

COPY --from=build /go/src/github.com/yevhenii/myproj/f8n/benchmark/fdb-exporter/bin/exporter /bin/exporter
COPY --from=build /usr/lib/libfdb* /usr/lib/
COPY --from=build /usr/include/foundationdb/ /usr/include/foundationdb/

EXPOSE 2379/tcp
RUN mkdir /db && chown nobody /db
USER nobody
ENTRYPOINT ["/bin/exporter"]
