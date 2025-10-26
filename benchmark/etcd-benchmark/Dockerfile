FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src

# Clone the etcd repository
RUN git clone https://github.com/etcd-io/etcd.git .

# Build the benchmark tool
WORKDIR /src/tools/benchmark
RUN go build -o /benchmark .

# Final image
FROM alpine:latest
COPY --from=builder /benchmark /benchmark
ENTRYPOINT ["/benchmark"]
