```
for pod in $(kubectl get po -l foundationdb.org/fdb-cluster-name=${cluster} -o name --no-headers -A);
do
echo $pod
kubectl exec -n fdb-operator $pod -- bash -c 'pkill fdbserver && rm -f /var/fdb/data/fdb.cluster && pkill fdbserver'
done
```
