# Builder stage
FROM golang:1.24 as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /etcd-lease-flood

# Final stage
FROM scratch

COPY --from=builder /etcd-lease-flood /etcd-lease-flood

ENTRYPOINT ["/etcd-lease-flood"]
