package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"strings"
)

type RevResult struct {
	currentRevision int64
	events          []*server.Event
}

func (f *FDB) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	defer func() {
		logrus.Tracef("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%v, err=%v", prefix, startKey, limit, revision, revRet, kvRet, errRet)
	}()

	rev, events, err := f.list(nil, prefix, startKey, limit, revision, false)
	if err != nil {
		return rev, nil, err
	}
	if revision == 0 && len(events) == 0 {
		// if no revision is requested and no events are returned, then
		// get the current revision and relist.  Relist is required because
		// between now and getting the current revision something could have
		// been created.
		currentRev, err := f.CurrentRevision(ctx)
		if err != nil {
			return currentRev, nil, err
		}
		return f.List(ctx, prefix, startKey, limit, currentRev)
	} else if revision != 0 {
		rev = revision
	}

	kvs := make([]*server.KeyValue, 0, len(events))
	for _, event := range events {
		kvs = append(kvs, event.KV)
	}
	return rev, kvs, nil
}

func (f *FDB) list(tr *fdb.Transaction, prefix, startKey string, limit, maxRevision int64, includeDeletes bool) (resRev int64, resEvents []*server.Event, resErr error) {
	//  Examples:
	//  prefix=/bootstrap/, startKey=/bootstrap
	//  prefix=/bootstrap/abcd, startKey=/bootstrap/abcd
	//  prefix=/registry/secrets/, startKey=/registry/secrets/
	//  prefix=/registry/ranges/servicenodeports, startKey=""
	//  prefix=/, startKey=/registry/health
	//  prefix=/registry/podtemplates/chunking-6414/, startKey=/registry/podtemplates/chunking-6414/template-0016

	keyPrefix := f.byKeyAndRevision.Pack(tuple.Tuple{prefix})
	if strings.HasSuffix(prefix, "/") {
		// Removing 0x00 from the string encoding in the tuple to have a prefixed search
		// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/2
		keyPrefix = keyPrefix[:len(keyPrefix)-1]

		// Searching for equality
		if prefix == startKey {
			startKey = ""
		}
	} else {
		// Searching for equality
		startKey = ""
	}

	exec := func(tr fdb.Transaction) (interface{}, error) {
		keysToRevisionsRange, err := fdb.PrefixRange(keyPrefix)
		if err != nil {
			return nil, err
		}
		it := tr.GetRange(keysToRevisionsRange, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		result := make([]*server.Event, 0, limit)

		var candidateEvent *server.Event = nil
		for (int64(len(result)) < limit || limit == 0) && it.Advance() {
			newKey, newRevision, newEvent, err := f.getNextByKeyToRevisionEntry(it)
			if err != nil {
				return nil, err
			}

			if candidateEvent != nil && candidateEvent.KV.Key != newEvent.KV.Key {
				if !candidateEvent.Delete || includeDeletes {
					result = append(result, candidateEvent)
				}
				candidateEvent = nil
			}

			if (maxRevision == 0 || newRevision <= maxRevision) && newKey > startKey {
				candidateEvent = newEvent
			}
		}

		if candidateEvent != nil && (!candidateEvent.Delete || includeDeletes) {
			result = append(result, candidateEvent)
		}

		rev := int64(0)
		if maxRevision > 0 || len(result) != 0 {
			if rev, err = f.getCurrentRevision(tr); err != nil {
				return nil, err
			}
		}

		return &RevResult{currentRevision: rev, events: result}, nil
	}

	var result interface{}
	var err error
	if tr != nil {
		result, err = exec(*tr)
	} else {
		result, err = f.db.Transact(exec)
	}
	if err != nil {
		return 0, nil, err
	}

	revResult := result.(*RevResult)
	if maxRevision > revResult.currentRevision {
		return revResult.currentRevision, nil, server.ErrFutureRev
	}

	select {
	case f.triggerWatch <- revResult.currentRevision:
	default:
	}
	return revResult.currentRevision, revResult.events, nil
}

func (f *FDB) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	defer func() {
		f.adjustRevision(&revRet)
		logrus.Tracef("GET %s, rev=%d => rev=%d, kv=%v, err=%v", key, revision, revRet, kvRet != nil, errRet)
	}()

	rev, event, err := f.get(nil, key, rangeEnd, limit, revision, false)
	if event == nil {
		return rev, nil, err
	}
	return rev, event.KV, err
}

func (f *FDB) get(tr *fdb.Transaction, key, rangeEnd string, limit, revision int64, includeDeletes bool) (int64, *server.Event, error) {
	rev, events, err := f.list(tr, key, rangeEnd, limit, revision, includeDeletes)
	if err != nil {
		return 0, nil, err
	}
	if revision != 0 {
		rev = revision
	}
	if len(events) == 0 {
		return rev, nil, nil
	}
	return rev, events[0], nil
}

func (f *FDB) Count(ctx context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	defer func() {
		logrus.Tracef("COUNT %s, rev=%d => rev=%d, count=%d, err=%v", prefix, revision, revRet, count, err)
	}()
	rev, events, err := f.list(nil, prefix, startKey, 0, revision, false)
	count = int64(len(events))
	if err != nil {
		return 0, 0, err
	}

	if count == 0 {
		// if count is zero, then so is revision, so now get the current revision and re-count at that revision
		currentRev, err := f.CurrentRevision(ctx)
		if err != nil {
			return 0, 0, err
		}
		rev, rows, err := f.List(ctx, prefix, prefix, 1000, currentRev)
		return rev, int64(len(rows)), err
	}
	return rev, count, nil
}

func (f *FDB) getNextByKeyToRevisionEntry(it *fdb.RangeIterator) (string, int64, *server.Event, error) {
	kv, err := it.Get()
	if err != nil {
		return "", 0, nil, err
	}
	k, err := f.byKeyAndRevision.Unpack(kv.Key)
	if err != nil {
		return "", 0, nil, err
	}
	key := k[0].(string)
	versionstamp := k[1].(tuple.Versionstamp)
	versionstampInt64 := versionstampToInt64(&versionstamp)
	unpackedTuple, err := tuple.Unpack(kv.Value)
	if err != nil {
		return "", 0, nil, err
	}
	event := tupleToEvent(versionstampInt64, unpackedTuple)
	return key, versionstampInt64, event, nil
}

func (f *FDB) CurrentRevision(ctx context.Context) (int64, error) {
	key, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		return f.getCurrentRevision(tr)
	})
	return key.(int64), err
}

func (f *FDB) getCurrentRevision(tr fdb.Transaction) (int64, error) {
	return tr.GetReadVersion().Get()
}
