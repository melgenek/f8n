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
	return false, NewFdbStructured(cfg.DataSourceName, Directory), nil
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

	broadcaster broadcaster.Broadcaster
	ctx         context.Context

	lastWatchRev atomic.Int64
}

func NewFdbStructured(connectionString string, dirName string) server.Backend {
	logrus.Infof("Creating a FoundationDB driver with directory: '%s'", dirName)
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

	if err := setTLSConfig(); err != nil {
		return err
	}

	var db fdb.Database
	var err error
	if f.connectionString != "" {
		db, err = fdb.OpenWithConnectionString(f.connectionString)
	} else {
		db, err = fdb.OpenDefault()
	}
	if err != nil {
		return err
	}
	f.db = db

	etcd, err := directory.CreateOrOpen(db, []string{f.dirName}, nil)
	if err != nil {
		return err
	}
	f.dir = etcd

	if CleanDirOnStart {
		_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
			tr.ClearRange(etcd)
			return
		})
	}

	if err != nil {
		return err
	}

	f.byRevision = CreateByRevisionSubspace(etcd)
	f.byKeyAndRevision = CreateByKeyRevisionSubspace(etcd)
	f.watch = CreateWatchSubspace(etcd)
	f.compactRev = CreateCompactRevisionSubspace(etcd)
	f.rev = CreateRevisionSubspace(etcd)

	// https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if !APITest {
		if _, err := f.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
			if !errors.Is(err, server.ErrKeyExists) {
				logrus.Errorf("Failed to create health check key: %v", err)
			}
		}
	}
	go f.ttl(ctx)

	logrus.Info("Started the FoundationDB driver")
	return nil
}

func setTLSConfig() error {
	if TLSCertificateFile != "" {
		if err := fdb.Options().SetTLSCertPath(TLSCertificateFile); err != nil {
			return err
		}
	}
	if TLSKeyFile != "" {
		if err := fdb.Options().SetTLSKeyPath(TLSKeyFile); err != nil {
			return err
		}
	}
	if TLSPassword != "" {
		if err := fdb.Options().SetTLSPassword(TLSPassword); err != nil {
			return err
		}
	}
	if TLSCAFile != "" {
		if err := fdb.Options().SetTLSCaPath(TLSCAFile); err != nil {
			return err
		}
	}
	if TLSVerifyPeers != "" {
		if err := fdb.Options().SetTLSVerifyPeers([]byte(TLSVerifyPeers)); err != nil {
			return err
		}
	}
	return nil
}

func (f *FDB) DbSize(_ context.Context) (int64, error) {
	result, err := transact(f.db, 0, func(tr fdb.Transaction) (int64, error) {
		return tr.GetEstimatedRangeSizeBytes(f.dir).Get()
	})
	return result, err
}
