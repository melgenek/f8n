// Copyright 2022 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package traffic

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/tests/v3/robustness/traffic"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"go.etcd.io/etcd/tests/v3/framework/e2e"
	"go.etcd.io/etcd/tests/v3/robustness/client"
	"go.etcd.io/etcd/tests/v3/robustness/identity"
	"go.etcd.io/etcd/tests/v3/robustness/report"
)

func SimulateTraffic(ctx context.Context, t *testing.T, lg *zap.Logger, clus *e2e.EtcdProcessCluster, profile traffic.Profile, tf traffic.Traffic, failpointInjected <-chan report.FailpointInjection, baseTime time.Time, ids identity.Provider) []report.ClientReport {
	mux := sync.Mutex{}
	endpoints := clus.EndpointsGRPC()

	lm := identity.NewLeaseIDStorage()
	// Use the highest MaximalQPS of all traffic profiles as burst otherwise actual traffic may be accidentally limited
	limiter := rate.NewLimiter(rate.Limit(profile.MaximalQPS), profile.BurstableQPS)

	r, err := traffic.CheckEmptyDatabaseAtStart(ctx, lg, endpoints, ids, baseTime)
	require.NoError(t, err)
	reports := []report.ClientReport{r}

	wg := sync.WaitGroup{}
	nonUniqueWriteLimiter := traffic.NewConcurrencyLimiter(profile.MaxNonUniqueRequestConcurrency)
	finish := make(chan struct{})

	keyStore := traffic.NewKeyStore(10, "key")

	lg.Info("Start traffic")
	startTime := time.Since(baseTime)
	for i := 0; i < profile.ClientCount; i++ {
		wg.Add(1)
		c, nerr := client.NewRecordingClient([]string{endpoints[i%len(endpoints)]}, ids, baseTime)
		require.NoError(t, nerr)
		go func(c *client.RecordingClient) {
			defer wg.Done()
			defer c.Close()

			tf.RunTrafficLoop(ctx, c, limiter, ids, lm, nonUniqueWriteLimiter, keyStore, finish)
			mux.Lock()
			reports = append(reports, c.Report())
			mux.Unlock()
		}(c)
	}
	if !profile.ForbidCompaction {
		wg.Add(1)
		c, nerr := client.NewRecordingClient(endpoints, ids, baseTime)
		if nerr != nil {
			t.Fatal(nerr)
		}
		go func(c *client.RecordingClient) {
			defer wg.Done()
			defer c.Close()

			compactionPeriod := traffic.DefaultCompactionPeriod
			if profile.CompactPeriod != time.Duration(0) {
				compactionPeriod = profile.CompactPeriod
			}

			tf.RunCompactLoop(ctx, c, compactionPeriod, finish)
			mux.Lock()
			reports = append(reports, c.Report())
			mux.Unlock()
		}(c)
	}
	var fr *report.FailpointInjection
	select {
	case frp, ok := <-failpointInjected:
		require.Truef(t, ok, "Failed to collect failpoint report")
		fr = &frp
	case <-ctx.Done():
		t.Fatalf("Traffic finished before failure was injected: %s", ctx.Err())
	}
	close(finish)
	wg.Wait()
	lg.Info("Finished traffic")
	endTime := time.Since(baseTime)

	time.Sleep(time.Second)
	// Ensure that last operation succeeds
	cc, err := client.NewRecordingClient(endpoints, ids, baseTime)
	require.NoError(t, err)
	defer cc.Close()

	kc := kubernetes.Client{Client: &clientv3.Client{KV: cc}}
	// Kine does not support a regular Put
	//_, err = cc.Put(ctx, "tombstone", "true")
	_, err = kc.OptimisticPut(ctx, "/tombstone", []byte("true"), 0, kubernetes.PutOptions{})
	require.NoErrorf(t, err, "First operation failed, validation requires first operation to succeed")

	reports = append(reports, cc.Report())

	totalStats := traffic.CalculateStats(reports, startTime, endTime)
	beforeFailpointStats := traffic.CalculateStats(reports, startTime, fr.Start)
	duringFailpointStats := traffic.CalculateStats(reports, fr.Start, fr.End)
	afterFailpointStats := traffic.CalculateStats(reports, fr.End, endTime)

	lg.Info("Reporting complete traffic", zap.Int("successes", totalStats.Successes), zap.Int("failures", totalStats.Failures), zap.Float64("successRate", totalStats.SuccessRate()), zap.Duration("period", totalStats.Period), zap.Float64("qps", totalStats.QPS()))
	lg.Info("Reporting traffic before failure injection", zap.Int("successes", beforeFailpointStats.Successes), zap.Int("failures", beforeFailpointStats.Failures), zap.Float64("successRate", beforeFailpointStats.SuccessRate()), zap.Duration("period", beforeFailpointStats.Period), zap.Float64("qps", beforeFailpointStats.QPS()))
	lg.Info("Reporting traffic during failure injection", zap.Int("successes", duringFailpointStats.Successes), zap.Int("failures", duringFailpointStats.Failures), zap.Float64("successRate", duringFailpointStats.SuccessRate()), zap.Duration("period", duringFailpointStats.Period), zap.Float64("qps", duringFailpointStats.QPS()))
	lg.Info("Reporting traffic after failure injection", zap.Int("successes", afterFailpointStats.Successes), zap.Int("failures", afterFailpointStats.Failures), zap.Float64("successRate", afterFailpointStats.SuccessRate()), zap.Duration("period", afterFailpointStats.Period), zap.Float64("qps", afterFailpointStats.QPS()))

	if beforeFailpointStats.QPS() < profile.MinimalQPS {
		t.Errorf("Requiring minimal %f qps before failpoint injection for test results to be reliable, got %f qps", profile.MinimalQPS, beforeFailpointStats.QPS())
	}
	// TODO: Validate QPS post failpoint injection to ensure the that we sufficiently cover period when cluster recovers.
	return reports
}
