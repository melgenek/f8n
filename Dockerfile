# --- xx tool stage ---
FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

# --- multi-arch build stage ---
FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS build
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

RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    xx-go build -ldflags "-extldflags -s" -o bin/f8n

# --- multi-arch package stage ---
FROM debian:bookworm-slim AS package

COPY --from=build /go/src/github.com/melgenek/f8n/bin/f8n /bin/f8n
COPY --from=build /usr/lib/libfdb* /usr/lib/
COPY --from=build /usr/include/foundationdb/ /usr/include/foundationdb/
EXPOSE 2379/tcp
USER nobody
ENTRYPOINT ["/bin/f8n"]
