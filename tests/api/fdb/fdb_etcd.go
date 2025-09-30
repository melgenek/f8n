package fdb

import (
	"context"
	"github.com/k3s-io/kine/pkg/endpoint"
	"go.etcd.io/etcd/pkg/v3/proxy"
	"go.etcd.io/etcd/tests/v3/framework/config"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
	"k8s.io/utils/env"
	"time"
)

const NotifyInterval = 100 * time.Millisecond

var _ e2e.EtcdProcess = &FDBEtcdProcess{}

type FDBEtcdProcess struct {
	cancel context.CancelFunc

	etcdConfig *endpoint.ETCDConfig
}

func NewFDBEtcdProcess() (*FDBEtcdProcess, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := endpoint.Config{
		Listener:            "0.0.0.0:2379",
		Endpoint:            "fdb://" + env.GetString("FDB_CONNECTION_STRING", "docker:docker@127.0.0.1:4500"),
		NotifyInterval:      NotifyInterval,
		EmulatedETCDVersion: "3.5.13",
	}
	etcdConfig, err := endpoint.Listen(ctx, cfg)
	if err != nil {
		cancel()
		return nil, err
	}
	return &FDBEtcdProcess{
		etcdConfig: &etcdConfig,
		cancel:     cancel,
	}, nil
}

func (F FDBEtcdProcess) EndpointsGRPC() []string                       { return F.etcdConfig.Endpoints }
func (F FDBEtcdProcess) EndpointsHTTP() []string                       { return []string{} }
func (F FDBEtcdProcess) EndpointsMetrics() []string                    { return []string{} }
func (F FDBEtcdProcess) Etcdctl(...config.ClientOption) *e2e.EtcdctlV3 { return nil }
func (F FDBEtcdProcess) IsRunning() bool                               { return F.etcdConfig != nil }

func (F FDBEtcdProcess) Wait(ctx context.Context) error {
	return nil
}

func (F FDBEtcdProcess) Start(ctx context.Context) error { return nil }

func (F FDBEtcdProcess) Restart(context.Context) error { return nil }
func (F FDBEtcdProcess) Config() *e2e.EtcdServerProcessConfig {
	return &e2e.EtcdServerProcessConfig{
		Name: "fdb",
	}
}
func (F FDBEtcdProcess) PeerProxy() proxy.Server           { return nil }
func (F FDBEtcdProcess) Failpoints() *e2e.BinaryFailpoints { return nil }
func (F FDBEtcdProcess) LazyFS() *e2e.LazyFS               { return nil }
func (F FDBEtcdProcess) Logs() e2e.LogsExpect              { return nil }
func (F FDBEtcdProcess) Stop() error {
	F.cancel()
	return nil
}
func (F FDBEtcdProcess) Close() error {
	F.cancel()
	return nil
}
func (F FDBEtcdProcess) Kill() error {
	F.cancel()
	return nil
}

func (F FDBEtcdProcess) Pause() error  { return nil }
func (F FDBEtcdProcess) Resume() error { return nil }
