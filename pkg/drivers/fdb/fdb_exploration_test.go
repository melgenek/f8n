package fdb

import (
	"context"
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"k8s.io/utils/env"
)

var conn = env.GetString("FDB_CONNECTION_STRING", "docker:docker@127.0.0.1:4500")

func TestExplorationVersionstamp(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fdb.MustAPIVersion(730)
	db, err := fdb.OpenWithConnectionString(conn)
	require.NoError(t, err, "failed to start fdb")

	const (
		goroutines     = 100
		iterations     = 1000000
		valueSize      = 10000
		reportInterval = time.Second
	)

	var (
		writes  int64
		latency int64
	)

	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	go func() {
		var lastWrites int64
		lastTime := time.Now()
		for {
			select {
			case <-ticker.C:
				currentWrites := atomic.LoadInt64(&writes)
				currentLatency := atomic.SwapInt64(&latency, 0)
				now := time.Now()

				elapsed := now.Sub(lastTime).Seconds()
				writesSinceLast := currentWrites - lastWrites
				wps := float64(writesSinceLast) / elapsed

				var avgLatency time.Duration
				if writesSinceLast > 0 {
					avgLatency = time.Duration(currentLatency / writesSinceLast)
				}

				logrus.WithFields(logrus.Fields{
					"writes_per_second": wps,
					"average_latency":   avgLatency.String(),
				}).Info("Statistics")

				lastWrites = currentWrites
				lastTime = now
			case <-ctx.Done():
				return
			}
		}
	}()

	exploreDir, err := directory.CreateOrOpen(db, []string{"explore"}, nil)
	require.NoError(t, err, "failed to create a directory")
	_, err = transact("start", db, 0, func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(exploreDir)
		return 0, nil
	})
	require.NoError(t, err, "failed to clean the directory")

	var eg errgroup.Group
	eg.SetLimit(goroutines)

	for i := 0; i < goroutines; i++ {
		eg.Go(func() error {
			value := make([]byte, valueSize)
			if _, err := rand.Read(value); err != nil {
				return err
			}

			for j := 0; j < iterations; j++ {
				start := time.Now()

				_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					//setFirstInBatch(&tr)
					key, err := exploreDir.PackWithVersionstamp(tuple.Tuple{tuple.IncompleteVersionstamp(0)})
					if err != nil {
						return nil, err
					}
					tr.SetVersionstampedKey(key, value)
					return tr.GetVersionstamp(), nil
					//return nil, nil
				})

				if err != nil {
					return err
				}

				took := time.Since(start)
				atomic.AddInt64(&writes, 1)
				atomic.AddInt64(&latency, took.Nanoseconds())
			}
			return nil
		})
	}

	require.NoError(t, eg.Wait(), "test failed")
}

func TestExplorationId(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fdb.MustAPIVersion(730)
	db, err := fdb.OpenWithConnectionString(conn)
	require.NoError(t, err, "failed to start fdb")

	const (
		goroutines     = 100
		iterations     = 1000000
		valueSize      = 10000
		reportInterval = time.Second
	)

	var (
		writes  int64
		latency int64
		idx     int64
	)

	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	go func() {
		var lastWrites int64
		lastTime := time.Now()
		for {
			select {
			case <-ticker.C:
				currentWrites := atomic.LoadInt64(&writes)
				currentLatency := atomic.SwapInt64(&latency, 0)
				now := time.Now()

				elapsed := now.Sub(lastTime).Seconds()
				writesSinceLast := currentWrites - lastWrites
				wps := float64(writesSinceLast) / elapsed

				var avgLatency time.Duration
				if writesSinceLast > 0 {
					avgLatency = time.Duration(currentLatency / writesSinceLast)
				}

				logrus.WithFields(logrus.Fields{
					"writes_per_second": wps,
					"average_latency":   avgLatency.String(),
				}).Info("Statistics")

				lastWrites = currentWrites
				lastTime = now
			case <-ctx.Done():
				return
			}
		}
	}()

	exploreDir, err := directory.CreateOrOpen(db, []string{"explore"}, nil)
	require.NoError(t, err, "failed to create a directory")
	_, err = transact("start", db, 0, func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(exploreDir)
		return 0, nil
	})
	require.NoError(t, err, "failed to clean the directory")

	var eg errgroup.Group
	eg.SetLimit(goroutines)

	for i := 0; i < goroutines; i++ {
		eg.Go(func() error {
			value := make([]byte, valueSize)
			if _, err := rand.Read(value); err != nil {
				return err
			}

			for j := 0; j < iterations; j++ {
				newIdx := atomic.AddInt64(&idx, 1)
				start := time.Now()

				key := exploreDir.Pack(tuple.Tuple{newIdx})
				_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					tr.Set(key, value)
					return nil, tr.AddReadConflictKey(key)
				})

				if err != nil {
					return err
				}

				took := time.Since(start)
				atomic.AddInt64(&writes, 1)
				atomic.AddInt64(&latency, took.Nanoseconds())
			}
			return nil
		})
	}

	require.NoError(t, eg.Wait(), "test failed")
}

func TestExplorationIdSetReadVersion(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fdb.MustAPIVersion(730)
	db, err := fdb.OpenWithConnectionString(conn)
	require.NoError(t, err, "failed to start fdb")

	const (
		goroutines     = 100
		iterations     = 1000000
		valueSize      = 10000
		reportInterval = time.Second
	)

	var (
		writes      int64
		latency     int64
		idx         int64
		readVersion int64
	)

	readVersion, err = transact("readVersion", db, 0, func(tr fdb.Transaction) (int64, error) {
		return tr.GetReadVersion().Get()
	})
	if err != nil {
		panic(err)
	}

	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	go func() {
		var lastWrites int64
		lastTime := time.Now()
		for {
			select {
			case <-ticker.C:
				readVersion, err = transact("readVersion", db, 0, func(tr fdb.Transaction) (int64, error) {
					return tr.GetReadVersion().Get()
				})
				if err != nil {
					panic(err)
				}

				currentWrites := atomic.LoadInt64(&writes)
				currentLatency := atomic.SwapInt64(&latency, 0)
				now := time.Now()

				elapsed := now.Sub(lastTime).Seconds()
				writesSinceLast := currentWrites - lastWrites
				wps := float64(writesSinceLast) / elapsed

				var avgLatency time.Duration
				if writesSinceLast > 0 {
					avgLatency = time.Duration(currentLatency / writesSinceLast)
				}

				logrus.WithFields(logrus.Fields{
					"writes_per_second": wps,
					"average_latency":   avgLatency.String(),
				}).Info("Statistics")

				lastWrites = currentWrites
				lastTime = now
			case <-ctx.Done():
				return
			}
		}
	}()

	exploreDir, err := directory.CreateOrOpen(db, []string{"explore"}, nil)
	require.NoError(t, err, "failed to create a directory")
	_, err = transact("start", db, 0, func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(exploreDir)
		return 0, nil
	})
	require.NoError(t, err, "failed to clean the directory")

	var eg errgroup.Group
	eg.SetLimit(goroutines)

	for i := 0; i < goroutines; i++ {
		eg.Go(func() error {
			value := make([]byte, valueSize)
			if _, err := rand.Read(value); err != nil {
				return err
			}

			for j := 0; j < iterations; j++ {
				newIdx := atomic.AddInt64(&idx, 1)
				start := time.Now()

				key := exploreDir.Pack(tuple.Tuple{newIdx})
				_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					tr.SetReadVersion(readVersion)
					tr.Set(key, value)
					return nil, tr.AddReadConflictKey(key)
				})

				if err != nil {
					return err
				}

				took := time.Since(start)
				atomic.AddInt64(&writes, 1)
				atomic.AddInt64(&latency, took.Nanoseconds())
			}
			return nil
		})
	}

	require.NoError(t, eg.Wait(), "test failed")
}

func TestExplorationIdBatch(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fdb.MustAPIVersion(730)
	db, err := fdb.OpenWithConnectionString(conn)
	require.NoError(t, err, "failed to start fdb")

	const (
		goroutines     = 100
		iterations     = 1000000
		valueSize      = 10000
		reportInterval = time.Second
	)

	var (
		writes  int64
		latency int64
		idx     int64
	)

	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	go func() {
		var lastWrites int64
		lastTime := time.Now()
		for {
			select {
			case <-ticker.C:
				currentWrites := atomic.LoadInt64(&writes)
				currentLatency := atomic.SwapInt64(&latency, 0)
				now := time.Now()

				elapsed := now.Sub(lastTime).Seconds()
				writesSinceLast := currentWrites - lastWrites
				wps := float64(writesSinceLast) / elapsed

				var avgLatency time.Duration
				if writesSinceLast > 0 {
					avgLatency = time.Duration(currentLatency / writesSinceLast)
				}

				logrus.WithFields(logrus.Fields{
					"writes_per_second": wps,
					"average_latency":   avgLatency.String(),
				}).Info("Statistics")

				lastWrites = currentWrites
				lastTime = now
			case <-ctx.Done():
				return
			}
		}
	}()

	exploreDir, err := directory.CreateOrOpen(db, []string{"explore"}, nil)
	require.NoError(t, err, "failed to create a directory")
	_, err = transact("start", db, 0, func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(exploreDir)
		return 0, nil
	})
	require.NoError(t, err, "failed to clean the directory")

	var eg errgroup.Group
	eg.SetLimit(goroutines)

	for i := 0; i < goroutines; i++ {
		eg.Go(func() error {
			value := make([]byte, valueSize)
			if _, err := rand.Read(value); err != nil {
				return err
			}
			batch := 10
			for j := 0; j < iterations; j += batch {
				newIdx := atomic.AddInt64(&idx, int64(batch))
				start := time.Now()

				_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					for k := 0; k < batch; k++ {
						key := exploreDir.Pack(tuple.Tuple{newIdx + int64(k)})
						tr.Set(key, value)
						if err := tr.AddReadConflictKey(key); err != nil {
							return nil, err
						}
					}
					return nil, nil
				})

				if err != nil {
					return err
				}

				took := time.Since(start)
				atomic.AddInt64(&writes, int64(batch))
				atomic.AddInt64(&latency, took.Nanoseconds())
			}
			return nil
		})
	}

	require.NoError(t, eg.Wait(), "test failed")
}
