package fdb

import (
	"context"
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
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

func New(_ context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	logrus.Info("New FDB backend")
	return false, NewFDB(cfg.DataSourceName, cfg.BackendTLSConfig, Directory, wg), nil
}

type FDB struct {
	tlsConfig        tls.Config
	connectionString string
	dirName          string
	wg               *sync.WaitGroup

	db  fdb.Database
	dir directory.DirectorySubspace

	byRevision       *ByRevisionSubspace
	byKeyAndRevision *ByKeyAndRevisionSubspace
	watch            *WatchSubspace
	compactRev       *CompactRevisionSubspace
	rev              *RevisionSubspace

	broadcaster broadcaster.Broadcaster
	ctx         context.Context

	backgroundReadWg sync.WaitGroup
	lastWatchRev     atomic.Int64
}

func NewFDB(connectionString string, tlsConfig tls.Config, dirName string, wg *sync.WaitGroup) server.Backend {
	logrus.Infof("Creating a FoundationDB backend with directory: '%s'", dirName)
	ThisFDB = &FDB{
		connectionString: connectionString,
		tlsConfig:        tlsConfig,
		dirName:          dirName,
		wg:               wg,
	}
	return &FdbLogger{
		backend:   ThisFDB,
		threshold: 500 * time.Millisecond,
	}
}

func (f *FDB) Start(ctx context.Context) error {
	fdb.MustAPIVersion(730)
	f.ctx = ctx

	if err := logFDBTraces(); err != nil {
		return err
	}
	if err := f.setTLSConfig(); err != nil {
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
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		<-ctx.Done()
		logrus.Info("Closing db")
		f.backgroundReadWg.Wait()
		f.db.Close()
		logrus.Info("Closed db")
	}()

	if err := f.db.Options().SetTransactionTimeout(transactionTimeout.Milliseconds()); err != nil {
		return err
	}
	if dir, err := directory.CreateOrOpen(f.db, []string{f.dirName}, nil); err != nil {
		return err
	} else {
		f.dir = dir
	}

	if CleanDirOnStart {
		_, err = transact("start", db, 0, func(tr fdb.Transaction) (interface{}, error) {
			tr.ClearRange(f.dir)
			return 0, nil
		})
		if err != nil {
			return err
		}
	}

	f.byRevision = CreateByRevisionSubspace(f.dir)
	f.byKeyAndRevision = CreateByKeyRevisionSubspace(f.dir)
	f.watch = CreateWatchSubspace(f.dir)
	f.compactRev = CreateCompactRevisionSubspace(f.dir)
	f.rev = CreateRevisionSubspace(f.dir)

	// https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if !APITest {
		if _, err := f.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
			if !errors.Is(err, server.ErrKeyExists) {
				logrus.Errorf("Failed to create health check key: %v", err)
			}
		}
	}
	go f.ttl(ctx)

	logrus.Info("Started the FoundationDB backend")
	return nil
}

// https://apple.github.io/foundationdb/tls.html#configuring-tls
func (f *FDB) setTLSConfig() error {
	if f.tlsConfig.CertFile != "" {
		if err := fdb.Options().SetTLSCertPath(f.tlsConfig.CertFile); err != nil {
			return err
		}
	}
	if f.tlsConfig.KeyFile != "" {
		if err := fdb.Options().SetTLSKeyPath(f.tlsConfig.KeyFile); err != nil {
			return err
		}
	}
	if f.tlsConfig.CAFile != "" {
		if err := fdb.Options().SetTLSCaPath(f.tlsConfig.CAFile); err != nil {
			return err
		}
	}
	if f.tlsConfig.SkipVerify {
		// https://apple.github.io/foundationdb/tls.html#turning-down-the-validation
		if err := fdb.Options().SetTLSVerifyPeers([]byte("Check.Valid=0")); err != nil {
			return err
		}
	}
	return nil
}

func (f *FDB) DbSize(_ context.Context) (int64, error) {
	result, err := transact("db_size", f.db, 0, func(tr fdb.Transaction) (int64, error) {
		return tr.GetEstimatedRangeSizeBytes(f.dir).Get()
	})
	return result, err
}
