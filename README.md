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

## TODO

Here is a list of implementation details that need to be completed before starting scale testing of this implementation.
- [ ] Watch operation has to account for long-running or large transactions. Make sure that in such case there are multiple consecutive FDB transactions. 
- [ ] put ttl events into a separate slice
- [ ] limit records to 2MiB

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
- https://forums.foundationdb.org/t/building-scalable-log-streaming-on-foundationdb/2781/10
- https://github.com/richardartoul/tsdb-layer
- https://github.com/k3s-io/kine/issues/311#issuecomment-2246257835
- https://github.com/k3s-io/kine/issues/386#issuecomment-2552695321
- https://github.com/apple/foundationdb/wiki/An-Overview-how-Watches-Work
- https://forums.foundationdb.org/t/changefeeds-watching-and-getting-updates-on-ranges-of-keys/511/8
- https://forums.foundationdb.org/t/go-lang-addreadconflictkey-addwriteconflictkey/1842/3
- https://forums.foundationdb.org/t/api-for-transaction-size/1367/6
- https://forums.foundationdb.org/t/transaction-size-limit-calculation/811/3
- https://forums.foundationdb.org/t/foundationdb-read-performance/729/4
- https://forums.foundationdb.org/t/getting-the-number-of-key-value-pairs/189/5
- https://forums.foundationdb.org/t/large-range-scans-avoid-5s-limit/292/9
- https://github.com/apple/foundationdb/blob/61fcce08cc9a894130c913ba8ca89963e93e6acc/bindings/java/src/main/com/apple/foundationdb/tuple/ByteArrayUtil.java#L364
- https://forums.foundationdb.org/t/what-does-it-mean-transaction-is-retryable-in-particular-to-handle-error-of-transaction-too-old/1451/2
- https://apple.github.io/foundationdb/developer-guide.html?highlight=retry%20loop#transaction-retry-loops
- https://forums.foundationdb.org/t/layer-for-read-write-transactions-lasting-longer-than-5-seconds/1318/10
- https://web.archive.org/web/20150325020408/http://community.foundationdb.com/questions/4118/future-version.html
- https://forums.foundationdb.org/t/read-only-transactions/3412
- https://github.com/apple/foundationdb/wiki/Difference-between-snapshot-reads-and-regular-reads
- https://forums.foundationdb.org/t/use-of-setreadversion/3696/2
- https://forums.foundationdb.org/t/changes-feed-without-hot-keys/1057/2
- https://forums.foundationdb.org/t/variable-chunk-size-for-blobs/1174/2
- https://github.com/apple/foundationdb/wiki/Transaction-size-limit
- https://forums.foundationdb.org/t/relax-consistency-guarantees/1560/19
- https://forums.foundationdb.org/t/generating-sortable-unique-id-primary-key-across-the-cluster-in-an-entity-relationship-model/3789/2
- https://forums.foundationdb.org/t/foundationdb-read-performance/729/4
- 

