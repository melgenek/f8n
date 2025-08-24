# Kine FoundationDB backend

This is a Kine fork that uses FoundationDB to mimic ETCD API.
The implementation currently passes all `sig-api-machinery` tests for `v1.33.3`.

## Running demo
0. Install FDB https://apple.github.io/foundationdb/getting-started-mac.html
0. Get the FDB connection string
```
cat /usr/local/etc/foundationdb/fdb.cluster
```
1. Run Kine
```
go run main.go  --endpoint=fdb://VufDkgAW:O2dFQHXk@127.0.0.1:4689
```

2. Run K3s backed by Kine
```
docker container run \
    --rm --name k3s \
    --network host \
    --privileged \
    -e K3S_DEBUG=true \
    -e K3S_DATASTORE_ENDPOINT=http://127.0.0.1:2379 \
    docker.io/rancher/k3s:v1.29.4-k3s1 server \
    --kube-apiserver-arg=feature-gates=WatchList=true \
    --kube-apiserver-arg=etcd-compaction-interval=10s \
    --disable=servicelb,traefik,local-storage,metrics-server \
    --disable-network-policy \
    --flannel-iface=eth0
```

3. Run kubectl
```
$ docker exec k3s kubectl get nodes -A
NAME          STATUS   ROLES                  AGE   VERSION
lima-docker   Ready    control-plane,master   86s   v1.29.4+k3s1
```

## Implementation

The implementation is in the `pkg/drivers/fdb` directory.
