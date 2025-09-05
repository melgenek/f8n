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

package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	forkedFailpoint "github.com/melgenek/f8n/pkg/api_test/failpoint"
	etcdFDB "github.com/melgenek/f8n/pkg/api_test/fdb"
	forkedTraffic "github.com/melgenek/f8n/pkg/api_test/traffic"
	forkedValidate "github.com/melgenek/f8n/pkg/api_test/validate"
	"github.com/melgenek/f8n/pkg/drivers/fdb"
	_ "github.com/melgenek/f8n/pkg/drivers/fdb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
	"go.etcd.io/etcd/tests/v3/robustness/client"
	"go.etcd.io/etcd/tests/v3/robustness/failpoint"
	"go.etcd.io/etcd/tests/v3/robustness/identity"
	"go.etcd.io/etcd/tests/v3/robustness/model"
	"go.etcd.io/etcd/tests/v3/robustness/report"
	"go.etcd.io/etcd/tests/v3/robustness/scenarios"
	"go.etcd.io/etcd/tests/v3/robustness/traffic"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	WaitBeforeFailpoint = time.Second
	WaitJitter          = traffic.DefaultCompactionPeriod
	WaitAfterFailpoint  = time.Second
)

var tsc = []scenarios.TestScenario{
	{
		Name:    "KubernetesSingleClient",
		Traffic: traffic.Kubernetes,
		Profile: traffic.Profile{
			MinimalQPS:                     100,
			MaximalQPS:                     200,
			BurstableQPS:                   1000,
			ClientCount:                    1,
			MaxNonUniqueRequestConcurrency: 3,
			ForbidCompaction:               true,
		},
		Watch: client.WatchConfig{
			RequestProgress: true,
		},
		Failpoint: forkedFailpoint.DummyFailpoint{},
	},
}

func TestRobustnessExploratory(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)
	fdb.APITest = true
	fdb.UseSequentialId = true
	fdb.CleanDirOnStart = true
	for _, scenario := range tsc {
		t.Run(scenario.Name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			scenario.Cluster.Logger = logger
			fdbProcess, err := etcdFDB.NewFDBEtcdProcess()
			require.NoError(t, err)

			cluster := &e2e.EtcdProcessCluster{
				Cfg: &e2e.EtcdProcessClusterConfig{
					ClusterSize: 1,
					ServerConfig: embed.Config{
						WatchProgressNotifyInterval: etcdFDB.NotifyInterval,
					},
				},
				Procs: []e2e.EtcdProcess{fdbProcess},
			}
			defer forcestopCluster(cluster)
			testRobustness(t.Context(), t, logger, scenario, cluster)
		})
	}
}

func testRobustness(ctx context.Context, t *testing.T, lg *zap.Logger, s scenarios.TestScenario, c *e2e.EtcdProcessCluster) {
	serverDataPaths := report.ServerDataPaths(c)
	r := report.TestReport{
		Logger:          lg,
		ServersDataPath: serverDataPaths,
		Traffic:         &report.TrafficDetail{ExpectUniqueRevision: s.Traffic.ExpectUniqueRevision()},
	}
	var persistedRequests []model.EtcdRequest
	var walToEtcdRequestsMapping []etcdFDB.WalDump
	var err error
	// t.Failed() returns false during panicking. We need to forcibly
	// save data on panicking.
	// Refer to: https://github.com/golang/go/issues/49929
	panicked := true
	defer func() {
		_, persistResults := os.LookupEnv("PERSIST_RESULTS")
		shouldReport := t.Failed() || panicked || persistResults
		path := testResultsDirectory(t)
		if shouldReport {
			r.ServersDataPath = map[string]string{}
			if err := r.Report(path); err != nil {
				t.Error(err)
			}
			persistWAL(t, path, walToEtcdRequestsMapping)
		}
	}()
	r.Client = runScenario(ctx, t, s, lg, c)
	persistedRequests, walToEtcdRequestsMapping, err = etcdFDB.WALToEtcdRequests()
	if err != nil {
		t.Error(err)
	}

	failpointImpactingWatch := s.Failpoint == failpoint.SleepBeforeSendWatchResponse
	if !failpointImpactingWatch {
		watchProgressNotifyEnabled := c.Cfg.ServerConfig.WatchProgressNotifyInterval != 0
		client.ValidateGotAtLeastOneProgressNotify(t, r.Client, s.Watch.RequestProgress || watchProgressNotifyEnabled)
	}
	validateConfig := forkedValidate.Config{ExpectRevisionUnique: s.Traffic.ExpectUniqueRevision()}
	result := forkedValidate.ValidateAndReturnVisualize(lg, validateConfig, r.Client, persistedRequests, 5*time.Minute)
	r.Visualize = result.Linearization.Visualize
	err = result.Error()
	if err != nil {
		t.Error(err)
	}
	panicked = false
}

func persistWAL(t *testing.T, resultsDirectory string, requests []etcdFDB.WalDump) {
	file, err := os.Create(filepath.Join(resultsDirectory, "wal.json"))
	if err != nil {
		t.Errorf("Failed to save WAL: %v", err)
		return
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, v := range requests {
		err := encoder.Encode(v)
		if err != nil {
			t.Errorf("Failed to encode operation: %v", err)
		}
	}
}

func runScenario(ctx context.Context, t *testing.T, s scenarios.TestScenario, lg *zap.Logger, clus *e2e.EtcdProcessCluster) (reports []report.ClientReport) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := errgroup.Group{}
	var operationReport, watchReport, failpointClientReport []report.ClientReport
	failpointInjected := make(chan report.FailpointInjection, 1)

	// using baseTime time-measuring operation to get monotonic clock reading
	// see https://github.com/golang/go/blob/master/src/time/time.go#L17
	baseTime := time.Now()
	ids := identity.NewIDProvider()
	g.Go(func() error {
		defer close(failpointInjected)
		// Give some time for traffic to reach qps target before injecting failpoint.
		time.Sleep(randomizeTime(WaitBeforeFailpoint, WaitJitter))
		fr, err := failpoint.Inject(ctx, t, lg, clus, s.Failpoint, baseTime, ids)
		if err != nil {
			t.Error(err)
			cancel()
		}
		// Give some time for traffic to reach qps target after injecting failpoint.
		time.Sleep(randomizeTime(WaitAfterFailpoint, WaitJitter))
		if fr != nil {
			failpointInjected <- fr.FailpointInjection
			failpointClientReport = fr.Client
		}
		return nil
	})
	maxRevisionChan := make(chan int64, 1)
	g.Go(func() error {
		defer close(maxRevisionChan)
		operationReport = forkedTraffic.SimulateTraffic(ctx, t, lg, clus, s.Profile, s.Traffic, failpointInjected, baseTime, ids)
		maxRevision := report.OperationsMaxRevision(operationReport)
		maxRevisionChan <- maxRevision
		lg.Info("Finished simulating Traffic", zap.Int64("max-revision", maxRevision))
		return nil
	})
	g.Go(func() error {
		var err error
		endpoints := processEndpoints(clus)
		watchReport, err = client.CollectClusterWatchEvents(ctx, lg, endpoints, maxRevisionChan, s.Watch, baseTime, ids)
		return err
	})
	err := g.Wait()
	if err != nil {
		t.Error(err)
	}

	// HashKV is not supported by Kine.
	//err = client.CheckEndOfTestHashKV(ctx, clus)
	//if err != nil {
	//	t.Error(err)
	//}
	return append(operationReport, append(failpointClientReport, watchReport...)...)
}

func randomizeTime(base time.Duration, jitter time.Duration) time.Duration {
	return base - jitter + time.Duration(rand.Int63n(int64(jitter)*2))
}

func processEndpoints(clus *e2e.EtcdProcessCluster) []string {
	endpoints := make([]string, 0, len(clus.Procs))
	for _, proc := range clus.Procs {
		endpoints = append(endpoints, proc.EndpointsGRPC()[0])
	}
	return endpoints
}

// forcestopCluster stops the etcd member with signal kill.
func forcestopCluster(clus *e2e.EtcdProcessCluster) error {
	for _, member := range clus.Procs {
		member.Kill()
	}
	return clus.ConcurrentStop()
}

func testResultsDirectory(t *testing.T) string {
	resultsDirectory, ok := os.LookupEnv("RESULTS_DIR")
	if !ok {
		resultsDirectory = "/tmp/"
	}
	resultsDirectory, err := filepath.Abs(resultsDirectory)
	if err != nil {
		panic(err)
	}
	path, err := filepath.Abs(filepath.Join(
		resultsDirectory, strings.ReplaceAll(t.Name(), "/", "_"), fmt.Sprintf("%v", time.Now().UnixNano())))
	require.NoError(t, err)
	err = os.RemoveAll(path)
	require.NoError(t, err)
	err = os.MkdirAll(path, 0o700)
	require.NoError(t, err)
	return path
}
