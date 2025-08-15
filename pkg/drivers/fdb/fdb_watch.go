package fdb

import (
	"context"
	"errors"
	"fmt"
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

	result := make(chan []*server.Event, 10)
	errc := make(chan error, 1)
	wr := server.WatchResult{Events: result, Errorc: errc}

	// initial read
	afterResult, err := f.after(prefix, revision, 0)
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
		lastRevision := revision
		if len(afterResult.events) > 0 {
			lastRevision = afterResult.currentRevision
		}

		if len(afterResult.events) > 0 {
			result <- afterResult.events
		}

		for events := range readChan {
			//skip events that have already been sent in the initial batch
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

		var afterResult *AfterResult
		for afterResult, err = f.after("/", currentRev, maxBatchSize); err != nil; {
			logrus.Errorf("Error in 'after' err=%v", err)
		}

		currentRev = afterResult.currentRevision
		if len(afterResult.events) > 0 {
			result <- afterResult.events
			watchFuture.(fdb.FutureNil).Cancel()
		} else if err := watchFuture.(fdb.FutureNil).Get(); err != nil {
			logrus.Errorf("Error waiting for a watch err=%v", err)
		}
	}
}

func (f *FDB) after(prefix string, minRevision, limit int64) (*AfterResult, error) {
	begin := f.byRevision.GetSubspace().Pack(tuple.Tuple{int64ToVersionstamp(minRevision)})
	_, end := f.byRevision.GetSubspace().FDBRangeKeySelectors()

	// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/11
	// https://forums.foundationdb.org/t/foundation-db-go-lang-pagination/1305/17
	// https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
	selector := fdb.SelectorRange{
		Begin: fdb.FirstGreaterThan(begin),
		End:   end,
	}

	result, err := transact(f.db, nil, func(tr fdb.Transaction) (*AfterResult, error) {
		it := tr.GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		result := make([]*server.Event, 0, limit)
		for (int64(len(result)) < limit || limit == 0) && it.Advance() {
			rev, record, err := f.byRevision.GetFromIterator(it)
			if err != nil {
				return nil, err
			}

			if doesEventHavePrefix(record.Key, prefix) {
				event := revRecordToEvent(&RevRecord{Rev: rev, Record: record})

				if record.PrevRevision != dummyVersionstamp {
					prevRecord, err := f.byRevision.Get(&tr, record.PrevRevision)
					if err != nil {
						return nil, err
					}
					if prevRecord != nil {
						event.PrevKV = revRecordToEvent(&RevRecord{Rev: record.PrevRevision, Record: prevRecord}).KV
					} else {
						// Previous record has been compacted
						event.PrevKV = nil
					}
				}

				result = append(result, event)
			}
		}

		rev, err := tr.GetReadVersion().Get()
		if err != nil {
			return nil, err
		}

		compactRev, err := f.compactRev.Get(&tr)
		if err != nil {
			return nil, err
		}
		if minRevision > 0 && minRevision < versionstampToInt64(compactRev) {
			return &AfterResult{
					currentRevision: rev,
					compactRevision: versionstampToInt64(compactRev),
					events:          result,
				},
				server.ErrCompacted
		}
		return &AfterResult{currentRevision: rev, events: result}, nil
	})
	logrus.Tracef("AFTER key=%s rev=%d => res=%v err=%v", prefix, minRevision, result, err)
	return result, err
}
