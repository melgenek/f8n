package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"math"
	"strings"
	"time"
)

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
				if doesEventHavePrefix(event, prefix) {
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

func doesEventHavePrefix(event *server.Event, prefix string) bool {
	return (strings.HasSuffix(prefix, "/") && strings.HasPrefix(event.KV.Key, prefix)) ||
		event.KV.Key == prefix
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

	lastBatchSize := math.MaxInt

	wait := time.NewTicker(1 * time.Second)
	defer wait.Stop()
	defer close(result)

	for {
		if lastBatchSize < 100 {
			select {
			case <-f.ctx.Done():
				return
			case check := <-f.triggerWatch:
				if check <= currentRev {
					continue
				}
			case <-wait.C:
			}
		}

		_, events, err := f.after("/", currentRev, 500)
		if err != nil || len(events) == 0 {
			continue
		}

		currentRev = events[len(events)-1].KV.ModRevision
		lastBatchSize = len(events)
		result <- events
	}
}

func (f *FDB) after(prefix string, minRevision, limit int64) (int64, []*server.Event, error) {
	begin := f.byRevision.Pack(tuple.Tuple{int64ToVersionstamp(minRevision)})
	begin = begin[:len(begin)-1]
	_, end := f.byRevision.FDBRangeKeys()

	// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/11
	// https://forums.foundationdb.org/t/foundation-db-go-lang-pagination/1305/17
	// https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
	selector := fdb.SelectorRange{
		Begin: fdb.FirstGreaterThan(begin),
		End:   fdb.FirstGreaterOrEqual(end),
	}

	//if err != nil {
	//	return 0, nil, err
	//}
	//begin = selector.Begin.FDBKey()
	//fmt.Println(begin)
	//fmt.Println(selector.End.FDBKey())
	//selector = fdb.KeyRange{Begin: begin[:len(begin)-1], End: selector.End.FDBKey()}

	result, err := f.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		it := tr.GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		result := make([]*server.Event, 0, limit)
		for (int64(len(result)) < limit || limit == 0) && it.Advance() {
			_, event, err := f.getNextByRevisionEntry(it)
			if err != nil {
				return nil, err
			}

			if doesEventHavePrefix(event, prefix) {
				result = append(result, event)
			}
		}

		resultRev := int64(0)
		if minRevision > 0 || len(result) != 0 {
			rev, err := f.getCurrentRevision(tr)
			if err != nil {
				return nil, err
			}
			resultRev = rev
		}

		return &RevResult{currentRevision: resultRev, events: result}, nil
	})
	if err != nil {
		return 0, nil, err
	}

	return result.(*RevResult).currentRevision, result.(*RevResult).events, err
}

func (f *FDB) getNextByRevisionEntry(it *fdb.RangeIterator) ([]byte, *server.Event, error) {
	kv, err := it.Get()
	if err != nil {
		return nil, nil, err
	}
	k, err := f.byRevision.Unpack(kv.Key)
	if err != nil {
		return nil, nil, err
	}
	versionstamp := k[0].(tuple.Versionstamp)
	versionstampInt64 := versionstampToInt64(&versionstamp)
	unpackedTuple, err := tuple.Unpack(kv.Value)
	if err != nil {
		return nil, nil, err
	}
	event := tupleToEvent(versionstampInt64, unpackedTuple)
	return kv.Key, event, nil
}
