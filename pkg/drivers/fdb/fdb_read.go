package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"strings"
)

type RevResult struct {
	currentRevision int64
	revRecords      []*RevRecord
}

func (f *FDB) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	rev, records, err := f.list(nil, prefix, startKey, limit, revision, false)
	if err != nil {
		return rev, nil, err
	}
	if revision == 0 && len(records) == 0 {
		// if no revision is requested and no revRecords are returned, then
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

	kvs := make([]*server.KeyValue, 0, len(records))
	for _, revRecord := range records {
		event := revRecordToEvent(revRecord)
		kvs = append(kvs, event.KV)
	}
	return rev, kvs, nil
}

func (f *FDB) list(tr *fdb.Transaction, prefix, startKey string, limit, maxRevision int64, includeDeletes bool) (resRev int64, resEvents []*RevRecord, resErr error) {
	//  Examples:
	//  prefix=/bootstrap/, startKey=/bootstrap
	//  prefix=/bootstrap/abcd, startKey=/bootstrap/abcd
	//  prefix=/registry/secrets/, startKey=/registry/secrets/
	//  prefix=/registry/ranges/servicenodeports, startKey=""
	//  prefix=/, startKey=/registry/health
	//  prefix=/registry/podtemplates/chunking-6414/, startKey=/registry/podtemplates/chunking-6414/template-0016

	keyPrefix := f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{prefix})
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

		result := make([]*RevRecord, 0, limit)

		var candidateRecord *RevRecord = nil
		for (int64(len(result)) < limit || limit == 0) && it.Advance() {
			kv, err := it.Get()
			if err != nil {
				return nil, err
			}

			newRecordKey, newRecord, err := f.byKeyAndRevision.ParseKV(kv)
			if err != nil {
				return nil, err
			}

			if candidateRecord != nil && candidateRecord.Record.Key != newRecord.Key {
				if !candidateRecord.Record.IsDelete || includeDeletes {
					result = append(result, candidateRecord)
				}
				candidateRecord = nil
			}

			if (maxRevision == 0 || versionstampToInt64(newRecordKey.Rev) <= maxRevision) && newRecord.Key > startKey {
				candidateRecord = &RevRecord{Rev: newRecordKey.Rev, Record: newRecord}
			}
		}

		if candidateRecord != nil && (!candidateRecord.Record.IsDelete || includeDeletes) {
			result = append(result, candidateRecord)
		}

		if rev, err := tr.GetReadVersion().Get(); err != nil {
			return nil, err
		} else {
			return &RevResult{currentRevision: rev, revRecords: result}, nil
		}
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

	return revResult.currentRevision, revResult.revRecords, nil
}

func (f *FDB) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	rev, revRecord, err := f.get(nil, key, rangeEnd, limit, revision, false)
	if revRecord == nil {
		return rev, nil, err
	}
	return rev, revRecordToEvent(revRecord).KV, err
}

func (f *FDB) get(tr *fdb.Transaction, key, rangeEnd string, limit, revision int64, includeDeletes bool) (int64, *RevRecord, error) {
	rev, events, err := f.list(tr, key, rangeEnd, limit, revision, includeDeletes)
	if err != nil {
		return 0, nil, err
	}
	if len(events) == 0 {
		return rev, nil, nil
	}
	return rev, events[0], nil
}

func (f *FDB) getLast(tr *fdb.Transaction, key string) (*RevRecord, error) {
	keyRange := f.byKeyAndRevision.GetSubspace().Sub(key)
	it := tr.GetRange(keyRange, fdb.RangeOptions{Limit: 1, Mode: fdb.StreamingModeExact, Reverse: true}).Iterator()

	if it.Advance() {
		kv, err := it.Get()
		if err != nil {
			return nil, err
		}
		recordKey, recordValue, err := f.byKeyAndRevision.ParseKV(kv)
		if err != nil {
			return nil, err
		}
		return &RevRecord{Rev: recordKey.Rev, Record: recordValue}, err
	} else {
		return nil, nil
	}
}

func (f *FDB) Count(ctx context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	rev, events, err := f.list(nil, prefix, startKey, 0, revision, false)
	if err != nil {
		return 0, 0, err
	} else {
		return rev, int64(len(events)), nil
	}
}

func (f *FDB) CurrentRevision(ctx context.Context) (int64, error) {
	key, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		return tr.GetReadVersion().Get()
	})
	return key.(int64), err
}
