package fdb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// https://github.com/etcd-io/etcd/blob/f072712e29a2dafc92e7cfb3c76cea60e0d508b2/server/storage/mvcc/watcher_group.go#L28
// `var` is for testing purposes
var maxBatchSize = 1000

type AfterResult struct {
	currentRevision int64
	compactRevision int64
	events          []*server.Event
}

func (a *AfterResult) String() string {
	return fmt.Sprintf("currentRev=%d, records=%d", a.currentRevision, len(a.events))
}

func (f *FDB) Watch(ctx context.Context, prefix string, minRevision int64) server.WatchResult {
	// ETCD robustness tests pass null prefix
	if prefix == "\u0000" {
		prefix = "/"
	}
	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := f.innerWatch(ctx, prefix)

	// include the current minRevision in initial poll
	if minRevision > 0 {
		minRevision--
	}

	result := make(chan []*server.Event, 100)
	errc := make(chan error, 1)
	wr := server.WatchResult{Events: result, Errorc: errc}

	// initial read
	afterResult, err := f.afterAll(minRevision, func(key string) bool {
		return doesEventHavePrefix(key, prefix)
	})
	logrus.Tracef("INITIAL POLL key=%s latestRev=%d => latestRev=%d res=%v err=%v", prefix, minRevision, afterResult.currentRevision, len(afterResult.events), err)
	if err != nil {
		logrus.Errorf("Failed to 'afterAll' %s for minRevision %d: %v", prefix, minRevision, err)
		if errors.Is(err, server.ErrCompacted) {
			wr.CompactRevision = afterResult.compactRevision
			wr.CurrentRevision = afterResult.currentRevision
		} else {
			errc <- server.ErrGRPCUnhealthy
		}
		cancel()
		return wr
	}

	lastRevision := minRevision
	if len(afterResult.events) > 0 {
		lastRevision = afterResult.events[len(afterResult.events)-1].KV.ModRevision
	}
	go func() {
		if len(afterResult.events) > 0 {
			for _, event := range afterResult.events {
				logrus.Tracef("INITIAL POLL EVENT key=%s latestRev=%d", event.KV.Key, event.KV.ModRevision)
			}
			result <- afterResult.events
		}

		readMore := true
		for readMore {
			select {
			case events := <-readChan:
				//skip events that have already been sent in the initial batch
				for len(events) > 0 && events[0].KV.ModRevision <= lastRevision {
					events = events[1:]
				}
				if len(events) > 0 {
					result <- events
				}
			case <-ctx.Done():
				readMore = false
			}
		}

		close(result)
		cancel()
	}()

	return wr
}

func (f *FDB) innerWatch(ctx context.Context, prefix string) <-chan []*server.Event {
	res := make(chan []*server.Event, 100)
	values, err := f.broadcaster.Subscribe(ctx, f.startWatch)
	if err != nil {
		return nil
	}

	go func() {
		defer close(res)
		for batch := range values {
			events := batch.([]*server.Event)
			filteredEventList := make([]*server.Event, 0, len(events))
			for _, event := range events {
				if doesEventHavePrefix(event.KV.Key, prefix) {
					filteredEventList = append(filteredEventList, event)
				}
			}
			if len(filteredEventList) > 0 {
				res <- filteredEventList
			}
		}
	}()
	return res
}

func doesEventHavePrefix(key string, prefix string) bool {
	return (strings.HasSuffix(prefix, "/") && strings.HasPrefix(key, prefix)) || key == prefix
}

func (f *FDB) startWatch() (chan interface{}, error) {
	pollStart, err := f.CurrentRevision(f.ctx)
	if err != nil {
		return nil, err
	}

	c := make(chan interface{})
	go f.poll(c, pollStart)
	return c, nil
}

func (f *FDB) poll(result chan interface{}, pollStart int64) {
	f.backgroundReadWg.Add(1)
	defer f.backgroundReadWg.Done()
	f.lastWatchRev.Store(pollStart)

	defer close(result)

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		watchFuture, err := transact("watch", f.db, nil, func(tr fdb.Transaction) (fdb.FutureNil, error) {
			return f.watch.Watch(&tr), nil
		})
		if err != nil {
			continue
		}

		var events []*server.Event
		var lastRev int64
		currentRev := f.lastWatchRev.Load()
		logrus.Tracef("POLLING lastRev=%d", currentRev)
		before := time.Now()
		for lastRev, events, err = f.afterBatch(currentRev, func(s string) bool { return true }); err != nil; {
			logrus.Errorf("Error in 'afterBatch' err=%v", err)
		}
		logrus.Tracef("AFTER POLL lastRev=%d => res=%v err=%v dur=%v", currentRev, len(events), err, time.Since(before).Milliseconds())
		for _, event := range events {
			logrus.Tracef("AFTER POLL EVENT key=%s create=%v delete=%v lastRev=%d", event.KV.Key, event.Create, event.Delete, event.KV.ModRevision)
		}

		if len(events) > 0 {
			result <- events
			f.lastWatchRev.Store(events[len(events)-1].KV.ModRevision)
			watchFuture.Cancel()
		} else {
			f.lastWatchRev.Store(lastRev)
			select {
			case <-f.ctx.Done():
				watchFuture.Cancel()
				return
			case err := <-WaitForFutureNil(watchFuture):
				if err != nil {
					logrus.Errorf("Error waiting for a watch err=%v", err)
				}
			}
		}
	}
}

type afterCollector struct {
	// input
	f              *FDB
	limit          int
	capacity       int
	checkCompacted bool
	minRevision    int64
	takeKey        func(string) bool
	// output
	batchEvents     []*server.Event
	fetchPrevGroup  errgroup.Group
	events          []*server.Event
	compactRevision int64
	batchCompactRev int64
	latestRev       int64
	batchLatestRev  int64
}

type prevRevRequest struct {
	tr    fdb.Transaction
	rev   Revision
	event *server.Event
}

func newAfterCollector(f *FDB, minRevision int64, checkCompacted bool, limit int, takeKey func(string) bool) *afterCollector {
	capacity := limit
	if capacity == 0 {
		capacity = 100
	}
	return &afterCollector{
		f:              f,
		limit:          limit,
		capacity:       capacity,
		takeKey:        takeKey,
		minRevision:    minRevision,
		checkCompacted: checkCompacted,
		batchEvents:    make([]*server.Event, 0, capacity),
		events:         make([]*server.Event, 0, capacity),
	}
}

func (c *afterCollector) startBatch() {
	c.batchEvents = c.batchEvents[len(c.batchEvents):]
	c.batchLatestRev = 0
	c.batchCompactRev = 0

}

func (c *afterCollector) next(tr *fdb.Transaction, it *fdb.RangeIterator) (fdb.Key, bool, error) {
	rev, record, err := c.f.byRevision.GetFromIterator(it)
	if err != nil {
		return nil, false, err
	}
	if rev == nil {
		return nil, false, nil
	}

	if c.latestRev != 0 && *rev > c.latestRev {
		return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev, math.MaxInt64}), false, nil
	}

	if c.takeKey(record.Key) {
		event := revRecordToEvent(&RevRecord{Rev: *rev, Record: record})
		c.batchEvents = append(c.batchEvents, event)

		if record.PrevRevision != zeroRevision {
			c.fetchPrevGroup.Go(func() error {
				if prevRecord, err := c.f.byRevision.Get(tr, record.PrevRevision); err != nil {
					return err
				} else if prevRecord != nil {
					event.PrevKV = revRecordToEvent(&RevRecord{Rev: record.PrevRevision, Record: record}).KV
				}
				return nil
			})
		}
	}

	return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev, math.MaxInt64}), c.needMore(), nil
}

func (c *afterCollector) needMore() bool {
	return c.limit == 0 || len(c.batchEvents)+len(c.events) < c.limit
}

func (c *afterCollector) endBatch(tr *fdb.Transaction, _ bool) error {
	if c.latestRev == 0 {
		if latestRev, err := c.f.rev.Get(tr, toReadTr); err != nil {
			return err
		} else {
			c.batchLatestRev = latestRev
		}
	}

	if c.checkCompacted {
		compactRev, err := c.f.compactRev.Get(tr)
		if err != nil {
			return err
		}
		if c.minRevision > 0 && c.minRevision < compactRev {
			c.batchCompactRev = compactRev
			return server.ErrCompacted
		}
	}
	return c.fetchPrevGroup.Wait()
}

func (c *afterCollector) postBatch() {
	c.events = append(c.events, c.batchEvents...)
	c.latestRev = c.batchLatestRev
	c.compactRevision = c.batchCompactRev
}

func (f *FDB) afterAll(minRevision int64, takeKey func(string) bool) (AfterResult, error) {
	selector := f.afterRevisionSelector(minRevision, math.MaxInt64)

	collector := newAfterCollector(f, minRevision, true, 0, takeKey)
	err := processRange(f.db, selector, collector, splitRangeAfterDurationForRead, toReadTr)
	currentRevision := collector.latestRev
	if len(collector.events) > 0 {
		currentRevision = collector.events[len(collector.events)-1].KV.ModRevision
	}
	return AfterResult{
		currentRevision: currentRevision,
		compactRevision: collector.compactRevision,
		events:          collector.events,
	}, err
}

func (f *FDB) afterBatch(minRevision int64, takeKey func(string) bool) (int64, []*server.Event, error) {
	selector := f.afterRevisionSelector(minRevision, minRevision+int64(maxBatchSize))

	collector := newAfterCollector(f, minRevision, false, maxBatchSize, takeKey)
	_, err := processBatch(f.db, selector, collector, splitRangeAfterDurationForRead, toReadTr)
	return collector.latestRev, collector.events, err
}

func (f *FDB) afterRevisionSelector(minRevision Revision, maxRevision Revision) fdb.SelectorRange {
	begin := f.byRevision.GetSubspace().Pack(tuple.Tuple{minRevision, math.MaxInt64})
	end := f.byRevision.GetSubspace().Pack(tuple.Tuple{maxRevision, math.MaxInt64})

	// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/11
	// https://forums.foundationdb.org/t/foundation-db-go-lang-pagination/1305/17
	// https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
	selector := fdb.SelectorRange{
		Begin: fdb.FirstGreaterThan(begin),
		End:   fdb.FirstGreaterThan(end),
	}
	return selector
}
