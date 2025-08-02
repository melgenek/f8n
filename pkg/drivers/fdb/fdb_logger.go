package fdb

import (
	"context"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

var (
	_ server.Backend = &FdbLogger{}
)

type FdbLogger struct {
	backend   server.Backend
	threshold time.Duration
}

func (b *FdbLogger) logMethod(dur time.Duration, str string, args ...any) {
	if dur > b.threshold {
		logrus.Warnf(str, args...)
	} else {
		logrus.Tracef(str, args...)
	}
}

func (b *FdbLogger) Start(ctx context.Context) error {
	logrus.Infof("Starting backend")
	return b.backend.Start(ctx)
}

func (b *FdbLogger) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		size := 0
		if kvRet != nil {
			size = len(kvRet.Value)
		}
		fStr := "GET %s, rev=%d => revRet=%d, kv=%v, size=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, revision, revRet, kvRet != nil, size, errRet, dur)
	}()

	return b.backend.Get(ctx, key, rangeEnd, limit, revision)
}

func (b *FdbLogger) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "CREATE %s, size=%d, lease=%d => rev=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, len(value), lease, revRet, errRet, dur)
	}()

	return b.backend.Create(ctx, key, value, lease)
}

func (b *FdbLogger) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, revision, revRet, kvRet != nil, deletedRet, errRet, dur)
	}()

	return b.backend.Delete(ctx, key, revision)
}

func (b *FdbLogger) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, prefix, startKey, limit, revision, revRet, len(kvRet), errRet, dur)
	}()

	return b.backend.List(ctx, prefix, startKey, limit, revision)
}

func (b *FdbLogger) Count(ctx context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "COUNT %s, start=%s, rev=%d => rev=%d, count=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, prefix, startKey, revision, revRet, count, err, dur)
	}()

	return b.backend.Count(ctx, prefix, startKey, revision)
}

func (b *FdbLogger) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, updated=%v, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, len(value), revision, lease, revRet, updateRet, errRet, dur)
	}()

	return b.backend.Update(ctx, key, value, revision, lease)
}

func (b *FdbLogger) Watch(ctx context.Context, prefix string, revision int64) server.WatchResult {
	return b.backend.Watch(ctx, prefix, revision)
}

// DbSize get the kineBucket size from JetStream.
func (b *FdbLogger) DbSize(ctx context.Context) (int64, error) {
	return b.backend.DbSize(ctx)
}

// CurrentRevision returns the current revision of the database.
func (b *FdbLogger) CurrentRevision(ctx context.Context) (revRet int64, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "CURRENT_REV => revRet=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, revRet, errRet, dur)
	}()
	return b.backend.CurrentRevision(ctx)
}

// Compact is a no-op / not implemented. Revision history is managed by the jetstream bucket.
func (b *FdbLogger) Compact(ctx context.Context, revision int64) (int64, error) {
	return revision, nil
}
