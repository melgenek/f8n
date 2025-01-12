# Kine (Kine is not etcd)
==========================

Kine is an etcdshim that translates etcd API to:
- SQLite
- Postgres
- MySQL/MariaDB
- NATS

## Features
- Can be ran standalone so any k8s (not just K3s) can use Kine
- Implements a subset of etcdAPI (not usable at all for general purpose etcd)
- Translates etcdTX calls into the desired API (Create, Update, Delete)

See an [example](/examples/minimal.md).

## Developer Documentation

A high level flow diagram and overview of code structure is available at [docs/flow.md](/docs/flow.md).


./scripts/build

docker-compose up -d
docker exec kine_fdb_1 fdbcli --exec "configure new single memory"

--endpoint=fdb://127.0.0.1



    docker container run \
        --rm --name k3s \
        --network host \
        --privileged \
        -e K3S_DEBUG=true \
        -e K3S_DATASTORE_ENDPOINT=http://127.0.0.1:2379 \
        docker.io/rancher/k3s:v1.29.4-k3s1 server \
        --kube-apiserver-arg=feature-gates=WatchList=true \
        --disable=coredns,servicelb,traefik,local-storage,metrics-server \
        --disable-network-policy




docker build \
    --label "org.foundationdb.version=7.3.56" \
    --label "org.foundationdb.build_date=$(date +"%Y-%m-%dT%H:%M:%S%z")" \
    --label "org.foundationdb.commit=abcdef" \
    --progress plain \
    --build-arg FDB_VERSION="7.3.56" \
    --build-arg FDB_LIBRARY_VERSIONS="7.3.56" \
    --build-arg FDB_WEBSITE="https://hello.world" \
    --tag "fdb-local" \
    --file Dockerfile \
    --target "foundationdb" .
