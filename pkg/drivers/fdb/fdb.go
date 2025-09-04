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
	"sync"
	"sync/atomic"
	"time"
)

var (
	_       server.Backend = &FDB{}
	ThisFDB *FDB
)

func init() {
	drivers.Register("fdb", New)
}

func New(_ context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	logrus.Info("New FDB backend")
	return false, NewFdbStructured(cfg.DataSourceName, "etcd"), nil
}

type FDB struct {
	connectionString string
	dirName          string

	db  fdb.Database
	dir directory.DirectorySubspace

	byRevision       *ByRevisionSubspace
	byKeyAndRevision *ByKeyAndRevisionSubspace
	watch            *WatchSubspace
	compactRev       *CompactRevisionSubspace
	rev              *RevisionSubspace
	wal              *WalSubspace

	broadcaster broadcaster.Broadcaster
	ctx         context.Context

	lastWatchRevByWatch sync.Map
	lastWatchRev        atomic.Int64
}

func NewFdbStructured(connectionString string, dirName string) server.Backend {
	ThisFDB = &FDB{
		connectionString: connectionString,
		dirName:          dirName,
	}
	return &FdbLogger{
		backend:   ThisFDB,
		threshold: 500 * time.Millisecond,
	}
}

func (f *FDB) Start(ctx context.Context) error {
	fdb.MustAPIVersion(730)
	f.ctx = ctx

	db, err := fdb.OpenWithConnectionString(f.connectionString)
	if err != nil {
		return err
	}
	f.db = db

	etcd, err := directory.CreateOrOpen(db, []string{f.dirName}, nil)
	if err != nil {
		return err
	}
	f.dir = etcd

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
	f.rev = CreateRevisionSubspace(etcd)
	f.wal = CreateWalSubspace(etcd)

	// https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if !CorrectnessTesting {
		if _, err := f.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
			if !errors.Is(err, server.ErrKeyExists) {
				logrus.Errorf("Failed to create health check key: %v", err)
			}
		}
	}
	go f.ttl(ctx)

	return nil
}

func (f *FDB) DbSize(_ context.Context) (int64, error) {
	result, err := transact(f.db, 0, func(tr fdb.Transaction) (int64, error) {
		return tr.GetEstimatedRangeSizeBytes(f.dir).Get()
	})
	return result, err
}
