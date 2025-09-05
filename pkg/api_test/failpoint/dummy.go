package failpoint

import (
	"context"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
	"go.etcd.io/etcd/tests/v3/robustness/identity"
	"go.etcd.io/etcd/tests/v3/robustness/report"
	"go.etcd.io/etcd/tests/v3/robustness/traffic"
	"go.uber.org/zap"
	"testing"
	"time"
)

type DummyFailpoint struct{}

func (d DummyFailpoint) Inject(ctx context.Context, t *testing.T, lg *zap.Logger, clus *e2e.EtcdProcessCluster, baseTime time.Time, ids identity.Provider) ([]report.ClientReport, error) {
	return []report.ClientReport{}, nil
}

func (d DummyFailpoint) Name() string {
	return "DummyFailpoint"
}

func (d DummyFailpoint) Available(clusterConfig e2e.EtcdProcessClusterConfig, process e2e.EtcdProcess, profile traffic.Profile) bool {
	return true
}
