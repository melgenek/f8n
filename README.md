# Kine FoundationDB backend

This is a Kine fork that uses FoundationDB to mimic ETCD API.
The implementation currently passes all `sig-api-machinery` tests for `v1.29.4` excluding `StorageVersionAPI|Slow|Flaky`.

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
    --disable=coredns,servicelb,traefik,local-storage,metrics-server \
    --disable-network-policy
```

3. Run kubectl
```
$ docker exec k3s kubectl get nodes -A
NAME          STATUS   ROLES                  AGE   VERSION
lima-docker   Ready    control-plane,master   86s   v1.29.4+k3s1
```

## Implementation

The implementation is in the `pkg/drivers/fdb` directory.

## TODO

Here is a list of implementation details that need to be completed before starting scale testing of this implementation.
- [ ] Current write amplification is x4. Reduce it to x2 by querying the previous value instead of storing it in the record.
- [ ] Value size is limited to 100KiB. Extend the size to 10MiB (transaction size limit) https://apple.github.io/foundationdb/largeval.html
- [ ] List operation has to account for long running or large transactions. Make sure that in such case there are multiple consequitive FDB transactions. 
- [ ] Implement compaction
- [ ] get rid of `adjustRevision`



## Useful readings

- https://www.alibabacloud.com/blog/getting-started-with-kubernetes-%7C-etcd_596292
- https://forums.foundationdb.org/t/a-foundationdb-layer-for-apiserver-as-an-alternative-to-etcd/2697/3
- https://forums.foundationdb.org/t/what-is-the-most-efficient-way-to-generate-version-stamps-in-fdb/2062
- https://forums.foundationdb.org/t/possible-to-create-a-unique-increasing-8-byte-sequence-with-versionstamps/1640/4
- https://static.sched.com/hosted_files/foundationdbsummit2019/86/zookeeper_layer.pdf
- https://www.youtube.com/watch?v=2HiIgbxtx0c&ab_channel=TheLinuxFoundation
- https://etcd.io/docs/v3.6/learning/data_model/
- https://github.com/apple/foundationdb/wiki/Difference-between-Tuple.range()-and-Range.startsWith()
- https://forums.foundationdb.org/t/what-is-the-most-efficient-way-to-generate-version-stamps-in-fdb/2062
- https://forums.foundationdb.org/t/versionstamp-vs-committedversion/600/4
- https://forums.foundationdb.org/t/versionstamp-performance/705
- https://forums.foundationdb.org/t/get-current-versionstamp/586/3
- https://forums.foundationdb.org/t/versionstamp-vs-committedversion/600/5
- 
