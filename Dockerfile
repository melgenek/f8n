# --- xx tool stage ---
FROM tonistiigi/xx AS xx

# --- multi-arch build stage ---
FROM golang:1.24-bookworm AS base
COPY --from=xx / /
ARG TAG
ARG TARGETOS
ARG TARGETARCH
ENV TAG=${TAG} CGO_ENABLED=1

ENV SRC_DIR=/go/src/github.com/melgenek/f8n
WORKDIR ${SRC_DIR}/
COPY ./go.mod ./go.sum ./main.go ./
COPY ./pkg ./pkg
COPY ./.git ./.git
COPY ./.golangci.json ./.golangci.json

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
    CGO_CFLAGS="-DSQLITE_ENABLE_DBSTAT_VTAB=1 -DSQLITE_USE_ALLOCA=1" xx-go build -ldflags "-extldflags -s" -o bin/f8n

# --- etcdctl stage ---
FROM debian:bookworm-slim AS etcd
ARG TARGETARCH
ARG ETCD_VERSION=3.5.13
RUN apt-get update && apt-get install -y wget tar gzip && rm -rf /var/lib/apt/lists/* \
    && if [ "${TARGETARCH}" = "amd64" ]; then \
         ARCH=amd64; \
       elif [ "${TARGETARCH}" = "arm64" ]; then \
         ARCH=arm64; \
       else \
         echo "Unsupported arch '${TARGETARCH}'"; exit 1; \
       fi \
    && wget -q https://github.com/etcd-io/etcd/releases/download/v${ETCD_VERSION}/etcd-v${ETCD_VERSION}-linux-${ARCH}.tar.gz \
    && tar xzf etcd-v${ETCD_VERSION}-linux-${ARCH}.tar.gz \
    && mv etcd-v${ETCD_VERSION}-linux-${ARCH}/etcdctl /usr/local/bin/etcdctl \
    && chmod +x /usr/local/bin/etcdctl \
    && rm -rf etcd-v${ETCD_VERSION}-linux-${ARCH}*

# --- multi-arch package stage ---
FROM debian:bookworm-slim AS package

COPY --from=build /go/src/github.com/melgenek/f8n/bin/f8n /bin/f8n
COPY --from=build /usr/lib/libfdb* /usr/lib/
COPY --from=build /usr/include/foundationdb/ /usr/include/foundationdb/
COPY --from=etcd /usr/local/bin/etcdctl /usr/local/bin/etcdctl

EXPOSE 2379/tcp
RUN mkdir /db && chown nobody /db
USER nobody
ENTRYPOINT ["/bin/f8n"]
