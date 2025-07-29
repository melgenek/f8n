package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

var (
	_ server.Backend = (&FDB{})
)

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	return false, NewFdbStructured(cfg.DataSourceName), nil
}

func init() {
	drivers.Register("fdb", New)
}

type FDB struct {
	connectionString string
	db               fdb.Database
	kine             directory.DirectorySubspace

	byRevision       Subspace[tuple.Versionstamp, string]
	byKeyAndRevision Subspace[*KeyAndRevision, *Record]

	triggerWatch chan int64
	broadcaster  broadcaster.Broadcaster
	ctx          context.Context
}

func NewFdbStructured(connectionString string) *FDB {
	return &FDB{
		connectionString: connectionString,
		triggerWatch:     make(chan int64, 1024),
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

	kine, err := directory.CreateOrOpen(db, []string{"kine"}, nil)
	if err != nil {
		return err
	}
	f.kine = kine

	// todo don't clear on startup
	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(kine)
		return
	})

	if err != nil {
		return err
	}

	f.byRevision = CreateByRevisionSubspace(kine)
	f.byKeyAndRevision = CreateByKeyRevisionSubspace(kine)

	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := f.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}
	go f.ttl(ctx)

	return nil
}
