/*
Copyright 2025 Benjamin Chess

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

type KV struct {
	key             string
	serializedValue string
	lastRevision    int64
}

func main() {
	var (
		endpoints  = flag.String("endpoints", "localhost:2379", "comma-separated etcd endpoints")
		numKeys    = flag.Int("num-keys", 1000, "number of Lease keys to create and flood")
		namespace  = flag.String("namespace", "default", "Kubernetes namespace for Lease keys")
		keyPrefix  = flag.String("key-prefix", "", "etcd key prefix")
		numWorkers = flag.Int("workers", 100, "number of concurrent worker goroutines")
	)
	flag.Parse()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*endpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// Create serializer for Kubernetes objects
	scheme := runtime.NewScheme()
	coordv1.AddToScheme(scheme)
	serializer := serializer.NewCodecFactory(scheme).LegacyCodec(schema.GroupVersion{Group: "coordination.k8s.io", Version: "v1"})

	// Create initial keys
	log.Printf("Creating %d initial Lease keys...", *numKeys)
	kvs := make([]KV, *numKeys)
	for i := 0; i < *numKeys; i++ {
		leaseName := fmt.Sprintf("%slease-%d", *keyPrefix, i)
		key := fmt.Sprintf("/registry/leases/%s/%s", *namespace, leaseName)

		lease := createLease(leaseName, *namespace)
		data, err := runtime.Encode(serializer, &lease)
		if err != nil {
			log.Printf("Failed to encode lease %d: %v", i, err)
			continue
		}
		kvs[i] = KV{key: key, serializedValue: string(data)}

		rev, err := optimisticPut(cli, context.Background(), key, string(data), 0)
		if err != nil {
			log.Printf("Failed to create key %s: %v", key, err)
		} else {
			kvs[i].lastRevision = rev
		}
	}
	log.Printf("Created %d initial keys", *numKeys)

	// Metrics tracking
	var putCount int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start metrics goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count := atomic.LoadInt64(&putCount)
				fmt.Printf("Puts/sec: %d\n", count)
				atomic.StoreInt64(&putCount, 0)
			}
		}
	}()

	// Create worker pool
	var wg sync.WaitGroup
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := (len(kvs) / *numWorkers) * workerID
			end := start + (len(kvs) / *numWorkers)
			worker(cli, kvs[start:end], &putCount, workerID)
		}(i)
	}

	log.Printf("Started %d workers, flooding etcd with Lease updates...", *numWorkers)
	wg.Wait()
}

func worker(cli *clientv3.Client, kvs []KV, putCount *int64, workerID int) {
	idx := 0
	for {
		kv := kvs[idx]

		rev, err := optimisticPut(cli, context.Background(), kv.key, kv.serializedValue, kv.lastRevision)
		if err != nil {
			log.Printf("Worker %d: Failed to update key %s: %v", workerID, kv.key, err)
		} else {
			kvs[idx].lastRevision = rev
			atomic.AddInt64(putCount, 1)
		}

		idx++
		if idx >= len(kvs) {
			idx = 0
		}
	}
}

func optimisticPut(k *clientv3.Client, ctx context.Context, key string, value string, expectedRevision int64) (int64, error) {
	txn := k.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), "=", expectedRevision),
	).Then(
		clientv3.OpPut(key, value, clientv3.WithLease(0)),
	).Else(clientv3.OpGet(key))

	txnResp, err := txn.Commit()
	if err != nil {
		return -1, err
	}

	if txnResp.Succeeded {
		//log.Printf("Successs to update key %v", txnResp)
		return txnResp.Responses[0].Response.(*etcdserverpb.ResponseOp_ResponsePut).ResponsePut.Header.Revision, nil
	} else {
		log.Printf("Failed to update key %v", txnResp)
		return txnResp.Responses[0].Response.(*etcdserverpb.ResponseOp_ResponseRange).ResponseRange.Kvs[0].ModRevision, nil
	}
	//return 1, nil
}

func createLease(name, namespace string) coordv1.Lease {
	now := metav1.NowMicro()
	leaseDurationSeconds := int32(15)

	return coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"kwok.x-k8s.io/kwok-controller": "kwok-controller-0",
			},
		},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &name,
			LeaseDurationSeconds: &leaseDurationSeconds,
			RenewTime:            &now,
		},
	}
}
