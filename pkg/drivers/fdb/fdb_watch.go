package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"strings"
)

// https://github.com/etcd-io/etcd/blob/f072712e29a2dafc92e7cfb3c76cea60e0d508b2/server/storage/mvcc/watcher_group.go#L28
const maxBatchSize = 1000

type AfterResult struct {
	currentRevision int64
	revRecords      []*server.Event
}

func (f *FDB) Watch(ctx context.Context, prefix string, revision int64) server.WatchResult {
	logrus.Tracef("WATCH %s, revision=%d", prefix, revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := f.innerWatch(ctx, prefix)

	// include the current revision in list
	if revision > 0 {
		revision--
	}

	result := make(chan []*server.Event, 100)
	wr := server.WatchResult{Events: result}

	// initial read
	rev, kvs, err := f.after(prefix, revision, 0)
	if err != nil {
		cancel()
	}

	logrus.Tracef("WATCH LIST key=%s rev=%d => rev=%d kvs=%d", prefix, revision, rev, len(kvs))

	go func() {
		lastRevision := revision
		if len(kvs) > 0 {
			lastRevision = rev
		}

		if len(kvs) > 0 {
			result <- kvs
		}

		for events := range readChan {
			//skip revRecords that have already been sent in the initial batch
			for len(events) > 0 && events[0].KV.ModRevision <= lastRevision {
				events = events[1:]
			}

			result <- events
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
			if len(events) > 0 {
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
	pollStart, err := f.CurrentRevision(nil)
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

		watchFuture, _ := f.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			return f.watch.Watch(&tr), nil
		})

		var newRev int64
		var events []*server.Event
		var err error
		for newRev, events, err = f.after("/", currentRev, maxBatchSize); err != nil; {
			logrus.Errorf("Error in 'after' err=%v", err)
		}

		currentRev = newRev
		if len(events) > 0 {
			result <- events
			watchFuture.(fdb.FutureNil).Cancel()
		} else if err := watchFuture.(fdb.FutureNil).Get(); err != nil {
			logrus.Errorf("Error waiting for a watch err=%v", err)
		}
	}
}

func (f *FDB) after(prefix string, minRevision, limit int64) (int64, []*server.Event, error) {
	begin := f.byRevision.GetSubspace().Pack(tuple.Tuple{int64ToVersionstamp(minRevision)})
	_, end := f.byRevision.GetSubspace().FDBRangeKeys()

	// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/11
	// https://forums.foundationdb.org/t/foundation-db-go-lang-pagination/1305/17
	// https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
	selector := fdb.SelectorRange{
		Begin: fdb.FirstGreaterThan(begin),
		End:   fdb.FirstGreaterOrEqual(end),
	}

	result, err := f.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		it := tr.GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		result := make([]*server.Event, 0, limit)
		for (int64(len(result)) < limit || limit == 0) && it.Advance() {
			kv, err := it.Get()
			if err != nil {
				return nil, err
			}
			rev, key, err := f.byRevision.ParseKV(kv)
			if err != nil {
				return nil, err
			}

			if doesEventHavePrefix(key, prefix) {
				record, err := f.byKeyAndRevision.Get(&tr, &KeyAndRevision{Key: key, Rev: rev})
				if err != nil {
					return nil, err
				}
				event := revRecordToEvent(&RevRecord{Rev: rev, Record: record})

				if record.PrevRevision != stubVersionstamp {
					prevRecord, err := f.byKeyAndRevision.Get(&tr, &KeyAndRevision{Key: key, Rev: record.PrevRevision})
					if err != nil {
						return nil, err
					}
					prevEvent := revRecordToEvent(&RevRecord{Rev: record.PrevRevision, Record: prevRecord})
					event.PrevKV = prevEvent.KV
				}

				result = append(result, event)
			}
		}

		rev, err := f.getCurrentRevision(tr)
		if err != nil {
			return nil, err
		}

		return &AfterResult{currentRevision: rev, revRecords: result}, nil
	})
	if err != nil {
		return 0, nil, err
	}

	return result.(*AfterResult).currentRevision, result.(*AfterResult).revRecords, err
}
