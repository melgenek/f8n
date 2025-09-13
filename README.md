# F8N: Kubernetes on FoundationDB 

F8N is an etcdshim that translated etcd API to FoundationDB.

# What's inside

The repo implements the Etcd API on top of the FoundationDB. 
The implementation is a backend for [Kine](https://github.com/k3s-io/kine)
[Kine gRPC server](https://github.com/k3s-io/kine/blob/master/docs/flow.md#flow-diagram) narrows the Etcd API surface to the minimum required by Kubernetes.

All the required features are implemented:
- lists, watches, ttl, compaction, etc.
- records up to 2MiB in size
- databases of arbitrary size

The implementation:
- passes `sig-api-machinery` tests for K8S `v1.33.3`
- a subset of the ETCD [K8S robustness tests](https://github.com/etcd-io/etcd/tree/main/tests/robustness) to validate the ETCD API responses and watch API guarantees

# How to use

F8N is a drop-in replacement for ETCD. 
Images are published at [ghcr.io/melgenek/f8n](https://github.com/melgenek/f8n/pkgs/container/f8n/versions?filters%5Bversion_type%5D=tagged):
```
docker pull ghcr.io/melgenek/f8n:v0.3.0
```

Run the F8N image as a container and configure your API server to connect to it instead of ETCD with `--etcd-servers`.

For a full mTLS example, see the `demo/tls` folder.
Docs are available in the [docs](docs) directory.

## Demo

Spin up FoundationDB, F8N, and K3s with Docker:

1. The demos are in the `demo` folder:
```
cd demo/simple
```

2. Start docker compose:
```
docker compose up
```

2. Run kubectl
```
docker exec k3s-demo kubectl get nodes -A

NAME           STATUS   ROLES                  AGE   VERSION
77232a04e727   Ready    control-plane,master   23s   v1.33.3+k3s1
```
