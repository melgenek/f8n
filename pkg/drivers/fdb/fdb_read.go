package fdb

import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"strings"
)

type RevResult struct {
	currentRevision int64
	revRecords      []*RevRecord
}

func (f *FDB) List(_ context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	rev, records, err := f.list("List", prefix, startKey, limit, revision)
	if err != nil {
		return rev, nil, err
	}
	if revision != 0 {
		rev = revision
	}

	kvs := make([]*server.KeyValue, 0, len(records))
	for _, revRecord := range records {
		event := revRecordToEvent(revRecord)
		kvs = append(kvs, event.KV)
	}
	return rev, kvs, nil
}

func (f *FDB) list(caller, prefix, startKey string, limit, maxRevision int64) (resRev int64, resEvents []*RevRecord, resErr error) {
	// Examples:
	// prefix=/bootstrap/, startKey=/bootstrap
	// prefix=/registry/secrets/, startKey=/registry/secrets/
	// prefix=/, startKey=/registry/health
	// prefix=/registry/health, startKey=/registry/health
	// prefix=/registry/ranges/serviceips, startKey=/registry/ranges/serviceips
	// prefix=/registry/podtemplates/chunking-6414/, startKey=/registry/podtemplates/chunking-6414/template-0016
	// prefix=/registry/masterleases/172.17.0.2, startKey=/registry/masterleases/172.17.0.2
	// prefix=/registry/clusterroles/system:aggregate-to-edit, startKey=/registry/clusterroles/system:aggregate-to-edit

	defer func() {
		logrus.Errorf("list (%s): prefix=%s, startKey=%s, limit=%d, maxRevision=%d => resRev=%d resEventsCount=%d resErr=%v", caller, prefix, startKey, limit, maxRevision, resRev, len(resEvents), resErr)
	}()

	var begin, end fdb.Selectable
	if strings.HasSuffix(prefix, "/") {
		// searching for prefix
		packedStartKey := f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{startKey})
		if prefix != startKey {
			// next key after the packedStartKey
			packedStartKeyKey, err := fdb.Strinc(packedStartKey)
			if err != nil {
				return 0, nil, fmt.Errorf("failed to create begin for list: %w", err)
			}
			packedStartKey = packedStartKeyKey
		} else {
			// searching for equality
		}
		begin = fdb.FirstGreaterOrEqual(packedStartKey)

		packedPrefix := f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{prefix})
		// Removing the last 0x00 from the string encoding in the tuple to have a prefixed search
		// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/2
		packedPrefixKey, err := fdb.Strinc(packedPrefix[:len(packedPrefix)-1])
		// err is always not nil, because tuple string is encoded as '0x02{string}0x00'
		if err != nil {
			return 0, nil, fmt.Errorf("failed to create end for list: %w", err)
		}
		// the last key is exclusive https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
		end = fdb.FirstGreaterOrEqual(fdb.Key(packedPrefixKey))
	} else if startKey != prefix {
		return 0, nil, fmt.Errorf("prefix is not equal to startKey. Prefix: %s, startKey: %s", prefix, startKey)
	} else {
		// searching for equality
		k := f.byKeyAndRevision.GetSubspace().Sub(prefix)
		begin, end = k.FDBRangeKeySelectors()
	}

	result, err := f.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := tr.Options().SetRetryLimit(1)
		if err != nil {
			return nil, fmt.Errorf("failed to set retry limit: %w", err)
		}
		it := tr.GetRange(fdb.SelectorRange{Begin: begin, End: end}, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		result := make([]*RevRecord, 0, limit)

		var candidateRecord *RevRecord = nil
		for (int64(len(result)) < limit || limit == 0) && it.Advance() {
			nextKeyAndRevRecord, err := f.byKeyAndRevision.GetFromIterator(it)
			if err != nil {
				return nil, err
			}

			if nextKeyAndRevRecord == nil {
				break
			}

			if candidateRecord != nil && candidateRecord.Record.Key != nextKeyAndRevRecord.Key.Key {
				if !candidateRecord.Record.IsDelete {
					result = append(result, candidateRecord)
				}
				candidateRecord = nil
			}

			if maxRevision == 0 || versionstampToInt64(nextKeyAndRevRecord.Key.Rev) <= maxRevision {
				nextRecord, err := f.byRevision.Get(&tr, nextKeyAndRevRecord.Key.Rev)
				if err != nil {
					return nil, err
				}

				candidateRecord = &RevRecord{Rev: nextKeyAndRevRecord.Key.Rev, Record: nextRecord}
			}
		}

		if candidateRecord != nil && !candidateRecord.Record.IsDelete {
			result = append(result, candidateRecord)
		}

		if rev, err := tr.GetReadVersion().Get(); err != nil {
			return nil, err
		} else {
			return &RevResult{currentRevision: rev, revRecords: result}, nil
		}
	})
	if err != nil {
		return 0, nil, err
	}

	revResult := result.(*RevResult)
	if maxRevision > revResult.currentRevision {
		return revResult.currentRevision, nil, server.ErrFutureRev
	}

	return revResult.currentRevision, revResult.revRecords, nil
}

func (f *FDB) Get(_ context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	if rangeEnd != "" {
		return 0, nil, fmt.Errorf("invalid 'rangeEnd' for Get. Expected: '', got %s", rangeEnd)
	}
	if limit != 1 {
		logrus.Tracef("Get request got `limit != 1`. Key: %s, rangeEnd=%s, limit=%d, rev=%d", key, rangeEnd, limit, revision)
	}
	rev, events, err := f.list("Get", key, key, 1, revision)
	if err != nil {
		return 0, nil, err
	}
	if len(events) == 0 {
		return rev, nil, nil
	}
	return rev, revRecordToEvent(events[0]).KV, nil
}

func (f *FDB) getLast(tr *fdb.Transaction, key string) (*ByKeyAndRevisionRecord, error) {
	keyRange := f.byKeyAndRevision.GetSubspace().Sub(key)
	it := tr.GetRange(keyRange, fdb.RangeOptions{Limit: 1, Mode: fdb.StreamingModeExact, Reverse: true}).Iterator()

	keyAndRevRecord, err := f.byKeyAndRevision.GetFromIterator(it)
	if err != nil {
		return nil, err
	} else {
		return keyAndRevRecord, nil
	}
}

func (f *FDB) Count(_ context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	rev, events, err := f.list("Count", prefix, startKey, 0, revision)
	if err != nil {
		return 0, 0, err
	} else {
		return rev, int64(len(events)), nil
	}
}

func (f *FDB) CurrentRevision(_ context.Context) (int64, error) {
	key, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		return tr.GetReadVersion().Get()
	})
	return key.(int64), err
}
