# F8N: K8S on FoundationDB 

F8N is an etcdshim that translated etcd API to FoundationDB.

# What's inside

The repo uses [Kine](https://github.com/k3s-io/kine) as a library and implements a new backend for FoundationDB.
[Kine gRPC server](https://github.com/k3s-io/kine/blob/master/docs/flow.md#flow-diagram) narrows the Etcd API surface to the minimum required by Kubernetes.

The implementation in this repo is experimental, although all the features are implemented.
Features include:
- lists, watches, compaction, etc.
- records up to 2MiB in size
- databases of arbitrary size

The implementation currently passes `sig-api-machinery` tests for K8S `v1.33.3`.

## Demo

Spin up FoundationDB, F8N, and K3s with Docker:

1. Get the FDB connection string
```
make start-k3s
```

2. Run kubectl
```
docker exec k3s kubectl get nodes -A

NAME           STATUS   ROLES                  AGE   VERSION
6669177524ca   Ready    control-plane,master   7s    v1.33.3+k3s1
```
