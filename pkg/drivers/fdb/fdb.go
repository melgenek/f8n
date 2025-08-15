package fdb

import (
	"context"
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"time"
)

var (
	_ server.Backend = &FDB{}
)

func New(_ context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	logrus.Info("New FDB backend")
	return false, NewFdbStructured(cfg.DataSourceName), nil
}

type FDB struct {
	connectionString string
	db               fdb.Database
	etcd             directory.DirectorySubspace

	byRevision       *ByRevisionSubspace
	byKeyAndRevision *ByKeyAndRevisionSubspace
	watch            *WatchSubspace
	compactRev       *CompactRevisionSubspace

	broadcaster broadcaster.Broadcaster
	ctx         context.Context
}

func NewFdbStructured(connectionString string) server.Backend {
	return &FdbLogger{
		backend: &FDB{
			connectionString: connectionString,
		},
		threshold: 500 * time.Millisecond,
	}
}

func (f *FDB) Start(ctx context.Context) error {
	f.ctx = ctx
	fdb.MustAPIVersion(730)

	db, err := fdb.OpenWithConnectionString(f.connectionString)
	if err != nil {
		return err
	}
	f.db = db

	etcd, err := directory.CreateOrOpen(db, []string{"etcd"}, nil)
	if err != nil {
		return err
	}
	f.etcd = etcd

	// todo don't clear on startup
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(etcd)
		return
	})

	if err != nil {
		return err
	}

	f.byRevision = CreateByRevisionSubspace(etcd)
	f.byKeyAndRevision = CreateByKeyRevisionSubspace(etcd)
	f.watch = CreateWatchSubspace(etcd)
	f.compactRev = CreateCompactRevisionSubspace(etcd)

	// https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := f.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if !errors.Is(err, server.ErrKeyExists) {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}
	go f.ttl(ctx)

	return nil
}

func (f *FDB) DbSize(_ context.Context) (int64, error) {
	result, err := transact(f.db, 0, func(tr fdb.Transaction) (int64, error) {
		return tr.GetEstimatedRangeSizeBytes(f.etcd).Get()
	})
	return result, err
}
