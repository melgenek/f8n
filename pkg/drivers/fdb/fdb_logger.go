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
	err := b.backend.Start(ctx)
	logrus.Infof("Started backend. Err: %v", err)
	return err
}

func (b *FdbLogger) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		size := 0
		if kvRet != nil {
			size = len(kvRet.Value)
		}
		fStr := "GET %s, rangeEnd=%s latestRev=%d => revRet=%d, kv=%v, size=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, rangeEnd, revision, revRet, kvRet != nil, size, errRet, dur)
	}()

	return b.backend.Get(ctx, key, rangeEnd, limit, revision)
}

func (b *FdbLogger) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "CREATE %s, size=%d, lease=%d => latestRev=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, len(value), lease, revRet, errRet, dur)
	}()

	return b.backend.Create(ctx, key, value, lease)
}

func (b *FdbLogger) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "UPDATE %s, value=%d, latestRev=%d, lease=%v => latestRev=%d, kv=%v, updated=%v, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, len(value), revision, lease, revRet, kvRet != nil, updateRet, errRet, dur)
	}()

	return b.backend.Update(ctx, key, value, revision, lease)
}

func (b *FdbLogger) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "DELETE %s, latestRev=%d => latestRev=%d, kv=%v, deleted=%v, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, revision, revRet, kvRet != nil, deletedRet, errRet, dur)
	}()

	return b.backend.Delete(ctx, key, revision)
}

func (b *FdbLogger) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "LIST %s, start=%s, limit=%d, latestRev=%d => latestRev=%d, kvs=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, prefix, startKey, limit, revision, revRet, len(kvRet), errRet, dur)
	}()

	return b.backend.List(ctx, prefix, startKey, limit, revision)
}

func (b *FdbLogger) Count(ctx context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "COUNT %s, start=%s, latestRev=%d => latestRev=%d, count=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, prefix, startKey, revision, revRet, count, err, dur)
	}()

	return b.backend.Count(ctx, prefix, startKey, revision)
}

func (b *FdbLogger) Watch(ctx context.Context, prefix string, revision int64) (res server.WatchResult) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "WATCH prefix=%s latestRev=%s => latestRev=%d, compactRev=%v, duration=%s"
		b.logMethod(dur, fStr, prefix, revision, res.CurrentRevision, res.CompactRevision, dur)
	}()
	return b.backend.Watch(ctx, prefix, revision)
}

func (b *FdbLogger) DbSize(ctx context.Context) (int64, error) {
	return b.backend.DbSize(ctx)
}

func (b *FdbLogger) CurrentRevision(ctx context.Context) (revRet int64, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "CURRENT_REV %v => revRet=%d, err=%v, duration=%s"
		watchId := ctx.Value("watchId")
		b.logMethod(dur, fStr, watchId, revRet, errRet, dur)
	}()
	return b.backend.CurrentRevision(ctx)
}

func (b *FdbLogger) Compact(ctx context.Context, revision int64) (revRet int64, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "COMPACT latestRev=%d => latestRev=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, revision, revRet, errRet, dur)
	}()
	return b.backend.Compact(ctx, revision)
}
