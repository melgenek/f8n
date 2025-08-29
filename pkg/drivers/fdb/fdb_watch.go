package fdb

import (
	"context"
	"errors"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"math"
	"strings"
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
	return fmt.Sprintf("rev=%d, events=%d", a.currentRevision, len(a.events))
}

func (f *FDB) Watch(ctx context.Context, prefix string, revision int64) server.WatchResult {
	// ETCD robustness tests pass null prefix
	if prefix == "\u0000" {
		prefix = "/"
	}
	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := f.innerWatch(ctx, prefix)

	// include the current revision in listKeyValue
	if revision > 0 {
		revision--
	}

	result := make(chan []*server.Event, 100)
	errc := make(chan error, 1)
	wr := server.WatchResult{Events: result, Errorc: errc}

	// initial read
	afterResult, err := f.after(revision, true, 0, func(key string) bool {
		return doesEventHavePrefix(key, prefix)
	})
	logrus.Tracef("INITIAL POLL key=%s rev=%d => res=%v err=%v", prefix, revision, 0, err)
	if err != nil {
		logrus.Errorf("Failed to 'after' %s for revision %d: %v", prefix, revision, err)
		if errors.Is(err, server.ErrCompacted) {
			wr.CompactRevision = afterResult.compactRevision
			wr.CurrentRevision = afterResult.currentRevision
		} else {
			errc <- server.ErrGRPCUnhealthy
		}
		cancel()
		return wr
	}

	go func() {
		lastRevision := afterResult.currentRevision
		if len(afterResult.events) > 0 {
			result <- afterResult.events
		}

		for events := range readChan {
			//skip events that have already been sent in the initial batch
			for len(events) > 0 && events[0].KV.ModRevision <= lastRevision {
				events = events[1:]
			}

			if len(events) > 0 {
				result <- events
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
	currentRev := pollStart

	defer close(result)

	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}

		watchFuture, err := transact(f.db, nil, func(tr fdb.Transaction) (fdb.FutureNil, error) {
			return f.watch.Watch(&tr), nil
		})
		if err != nil {
			continue
		}

		var afterResult AfterResult
		for afterResult, err = f.after(currentRev, false, maxBatchSize, func(s string) bool { return true }); err != nil; {
			logrus.Errorf("Error in 'afterBatch' err=%v", err)
		}
		logrus.Tracef("AFTER POLL rev=%d => res=%v err=%v", currentRev, len(afterResult.events), err)

		if len(afterResult.events) > 0 {
			currentRev = afterResult.events[len(afterResult.events)-1].KV.ModRevision
		} else {
			currentRev = afterResult.currentRevision
		}
		if len(afterResult.events) > 0 {
			result <- afterResult.events
			watchFuture.(fdb.FutureNil).Cancel()
		} else if err := watchFuture.(fdb.FutureNil).Get(); err != nil {
			logrus.Errorf("Error waiting for a watch err=%v", err)
		}
	}
}

type afterCollector struct {
	// input
	f              *FDB
	limit          int
	checkCompacted bool
	minRevision    int64
	takeKey        func(string) bool
	// output
	batchEvents     []*server.Event
	events          []*server.Event
	compactRevision int64
	batchCompactRev int64
	rev             int64
	batchRev        int64
}

func newAfterCollector(f *FDB, minRevision int64, checkCompacted bool, limit int, takeKey func(string) bool) *afterCollector {
	capacity := limit
	if capacity == 0 {
		capacity = 100
	}
	return &afterCollector{
		f:              f,
		limit:          limit,
		takeKey:        takeKey,
		minRevision:    minRevision,
		checkCompacted: checkCompacted,
		batchEvents:    make([]*server.Event, 0, capacity),
		events:         make([]*server.Event, 0, capacity),
	}
}

func (c *afterCollector) startBatch() {
	c.batchEvents = c.batchEvents[len(c.batchEvents):]
	c.batchRev = 0
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

	if c.rev != 0 && versionstampToInt64(*rev) > c.rev {
		return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev, math.MaxInt64}), false, nil
	}

	if c.takeKey(record.Key) {
		event := revRecordToEvent(&RevRecord{Rev: *rev, Record: record})

		if record.PrevRevision != dummyVersionstamp {
			prevRecord, err := c.f.byRevision.Get(tr, record.PrevRevision)
			if err != nil {
				return nil, false, err
			}
			if prevRecord != nil {
				event.PrevKV = revRecordToEvent(&RevRecord{Rev: record.PrevRevision, Record: prevRecord}).KV
			} else {
				// Previous record has been compacted
				event.PrevKV = nil
			}
		}

		c.batchEvents = append(c.batchEvents, event)
	}

	return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev, math.MaxInt64}), c.needMore(), nil
}

func (c *afterCollector) needMore() bool {
	return c.limit == 0 || len(c.batchEvents)+len(c.events) < c.limit
}

func (c *afterCollector) endBatch(tr *fdb.Transaction, _ bool) error {
	if c.rev == 0 {
		rev, err := tr.GetReadVersion().Get()
		if err != nil {
			return err
		}
		c.batchRev = rev
	}

	if c.checkCompacted {
		compactRev, err := c.f.compactRev.Get(tr)
		if err != nil {
			return err
		}
		if c.minRevision > 0 && c.minRevision < versionstampToInt64(compactRev) {
			c.batchCompactRev = versionstampToInt64(compactRev)
			return server.ErrCompacted
		}
	}
	return nil
}

func (c *afterCollector) postBatch() {
	c.events = append(c.events, c.batchEvents...)
	c.rev = c.batchRev
	c.compactRevision = c.batchCompactRev
}

func (f *FDB) after(minRevision int64, checkCompacted bool, limit int, takeKey func(string) bool) (AfterResult, error) {
	selector := f.afterRevisionSelector(minRevision)

	collector := newAfterCollector(f, minRevision, checkCompacted, limit, takeKey)
	err := processRange(f.db, selector, collector)
	return AfterResult{
		currentRevision: collector.rev,
		compactRevision: collector.compactRevision,
		events:          collector.events,
	}, err
}

func (f *FDB) afterRevisionSelector(minRevision int64) fdb.SelectorRange {
	begin := f.byRevision.GetSubspace().Pack(tuple.Tuple{int64ToVersionstamp(minRevision), math.MaxInt64})
	_, end := f.byRevision.GetSubspace().FDBRangeKeySelectors()

	// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/11
	// https://forums.foundationdb.org/t/foundation-db-go-lang-pagination/1305/17
	// https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
	selector := fdb.SelectorRange{
		Begin: fdb.FirstGreaterThan(begin),
		End:   end,
	}
	return selector
}
