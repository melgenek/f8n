package fdb

import (
	"bytes"
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
	versionstampRev := int64ToVersionstamp(revision)
	collector := newAfterCollector(f, versionstampRev, 0, func(key string) bool {
		return doesEventHavePrefix(key, prefix)
	})
	selector := f.afterRevisionRangeSelector(f.byRevision.GetSubspace().Pack(tuple.Tuple{revision}))
	_, err := processRange(f.db, selector, collector)

	logrus.Tracef("INITIAL POLL key=%s rev=%d => res=%v err=%v", prefix, revision, len(collector.events), err)
	if err != nil {
		logrus.Errorf("Failed to 'afterAll' %s for revision %d: %v", prefix, revision, err)
		if errors.Is(err, server.ErrCompacted) {
			wr.CompactRevision = collector.compactRevision
			wr.CurrentRevision = collector.rev
		} else {
			errc <- server.ErrGRPCUnhealthy
		}
		cancel()
		return wr
	}

	go func() {
		lastRevision := collector.rev
		if len(collector.events) > 0 {
			result <- collector.events
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
	currentRev := f.byRevision.GetSubspace().Pack(tuple.Tuple{int64ToVersionstamp(pollStart)})

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

		beforeRangeRev := currentRev
		totalReadEvents := 0
		lastBatchResult := batchResult{streamHasMore: true}
		for lastBatchResult.streamHasMore {
			selector := f.afterRevisionRangeSelector(currentRev)
			collector := newAfterCollector(f, zeroVersionstamp, maxBatchSize, func(s string) bool { return true })
			lastBatchResult, err = processRange(f.db, selector, collector)

			if err != nil {
				logrus.Errorf("Error in 'poll.processRange' lastKey=%v err=%v", lastBatchResult, err)
				continue
			} else {
				if lastBatchResult.lastReadKey != nil {
					currentRev = lastBatchResult.lastReadKey
				} else {
					currentRev = f.byRevision.GetSubspace().Pack(tuple.Tuple{int64ToVersionstamp(collector.rev)})
				}

				if len(collector.events) > 0 {
					result <- collector.events
				}
				totalReadEvents += len(collector.events)
				logrus.Tracef(
					"AFTER POLL rev=%v => rev=%v hasMore=%v needsMore=%v res=%v err=%v",
					beforeRangeRev, currentRev, lastBatchResult.streamHasMore, lastBatchResult.collectorNeedsMore, totalReadEvents, err,
				)
			}
		}

		if totalReadEvents > 0 {
			watchFuture.(fdb.FutureNil).Cancel()
		} else if err := watchFuture.(fdb.FutureNil).Get(); err != nil {
			logrus.Errorf("Error waiting for a watch err=%v", err)
		}
	}
}

type afterCollector struct {
	// input
	f           *FDB
	limit       int
	minRevision tuple.Versionstamp
	takeKey     func(string) bool
	// output
	batchEvents     []*server.Event
	events          []*server.Event
	compactRevision int64
	batchCompactRev int64
	rev             int64
	batchRev        int64
}

func newAfterCollector(f *FDB, minRevision tuple.Versionstamp, limit int, takeKey func(string) bool) *afterCollector {
	return &afterCollector{
		f:           f,
		limit:       limit,
		takeKey:     takeKey,
		minRevision: minRevision,
		batchEvents: make([]*server.Event, 0, 1000),
		events:      make([]*server.Event, 0, 1000),
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

	if !bytes.Equal(c.minRevision.Bytes(), zeroVersionstamp.Bytes()) {
		compactRev, err := c.f.compactRev.Get(tr)
		if err != nil {
			return err
		}
		if bytes.Compare(c.minRevision.Bytes(), compactRev.Bytes()) < 0 {
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

func (f *FDB) afterRevisionRangeSelector(begin fdb.Key) fdb.SelectorRange {
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
