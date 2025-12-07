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
	var (
		putCount      int64
		totalDuration int64
		watchTotalLag int64
		watchCount    int64
		watchLastRev  int64
	)
	revisionMap := &sync.Map{}
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
				pCount := atomic.SwapInt64(&putCount, 0)
				pDuration := atomic.SwapInt64(&totalDuration, 0)

				avgPDuration := float64(0)
				if pCount > 0 {
					avgPDuration = float64(pDuration) / float64(pCount)
				}

				var lastPutRev int64
				revisionMap.Range(func(key, value any) bool {
					if key.(int64) > lastPutRev {
						lastPutRev = key.(int64)
					}
					return true
				})

				wCount := atomic.SwapInt64(&watchCount, 0)
				wTotalLag := atomic.SwapInt64(&watchTotalLag, 0)
				wLastRev := atomic.LoadInt64(&watchLastRev)

				avgWLag := float64(0)
				if wCount > 0 {
					avgWLag = float64(wTotalLag) / float64(wCount)
				}

				log.Printf("Rev: %d. Puts/sec: %d. Avg duration: %.2fms. Watch rev: %d. Watch batch size: %d. Avg watch lag: %.2fms\n",
					lastPutRev, pCount, avgPDuration/1000000,
					wLastRev, wCount, avgWLag/1000000)
			}
		}
	}()

	// Start compaction goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				resp, err := cli.Get(ctx, kvs[0].key)
				if err != nil {
					log.Printf("Failed to get latest revision for compaction: %v", err)
					continue
				}
				rev := resp.Header.Revision
				log.Printf("Compacting etcd at revision %d", rev)
				start := time.Now()
				_, err = cli.Compact(ctx, rev)
				duration := time.Since(start)
				if err != nil {
					log.Printf("Compact completed in %v, but failed with: %v", duration, err)
				} else {
					log.Printf("Compact completed in %v at revision %d", duration, rev)
				}
			}
		}
	}()

	// Start watch goroutine
	go func() {
		watchCh := cli.Watch(ctx, "/", clientv3.WithPrefix(), clientv3.WithRev(1))
		for {
			select {
			case <-ctx.Done():
				return
			case watchResp := <-watchCh:
				for _, ev := range watchResp.Events {
					if val, ok := revisionMap.LoadAndDelete(ev.Kv.ModRevision); ok {
						writeTime := val.(time.Time)
						lag := time.Since(writeTime)
						atomic.AddInt64(&watchTotalLag, int64(lag))
						atomic.AddInt64(&watchCount, 1)
					}
				}
				if len(watchResp.Events) > 0 {
					atomic.StoreInt64(&watchLastRev, watchResp.Events[len(watchResp.Events)-1].Kv.ModRevision)
				}
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
			worker(cli, kvs[start:end], &putCount, &totalDuration, revisionMap, workerID)
		}(i)
	}

	log.Printf("Started %d workers, flooding etcd with Lease updates...", *numWorkers)
	wg.Wait()
}

func worker(cli *clientv3.Client, kvs []KV, putCount *int64, totalDuration *int64, revisionMap *sync.Map, workerID int) {
	idx := 0
	for {
		kv := kvs[idx]

		start := time.Now()
		rev, err := optimisticPut(cli, context.Background(), kv.key, kv.serializedValue, kv.lastRevision)
		duration := time.Since(start)

		if err != nil {
			log.Printf("Worker %d: Failed to update key %s: %v", workerID, kv.key, err)
		} else {
			kvs[idx].lastRevision = rev
			revisionMap.Store(rev, time.Now())
			atomic.AddInt64(putCount, 1)
			atomic.AddInt64(totalDuration, duration.Nanoseconds())
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
		return txnResp.Responses[0].Response.(*etcdserverpb.ResponseOp_ResponsePut).ResponsePut.Header.Revision, nil
	} else {
		log.Printf("Failed to update key %v", txnResp)
		return txnResp.Responses[0].Response.(*etcdserverpb.ResponseOp_ResponseRange).ResponseRange.Kvs[0].ModRevision, nil
	}
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
