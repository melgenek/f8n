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

func (f *FDB) CurrentRevision(_ context.Context) (int64, error) {
	key, err := transact(f.db, 0, func(tr fdb.Transaction) (ret int64, e error) {
		return tr.GetReadVersion().Get()
	})
	return key, err
}

func (f *FDB) List(_ context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	rev, kvs, err := f.listKeyValue("List", prefix, startKey, limit, revision)
	if err != nil {
		return rev, nil, err
	}
	if revision != 0 {
		rev = revision
	}
	return rev, kvs, nil
}

func (f *FDB) Get(_ context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	if rangeEnd != "" {
		return 0, nil, fmt.Errorf("invalid 'rangeEnd' for Get. Expected: '', got %s", rangeEnd)
	}
	if limit != 1 {
		logrus.Tracef("Get request got `limit != 1`. Key: %s, rangeEnd=%s, limit=%d, rev=%d", key, rangeEnd, limit, revision)
	}
	rev, kvs, err := f.listKeyValue("Get", key, key, 1, revision)
	if err != nil {
		return 0, nil, err
	}
	if len(kvs) == 0 {
		return rev, nil, nil
	}
	return rev, kvs[0], nil
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
	collector := &countCollector{}
	rev, err := f.listWithCollector("Count", prefix, startKey, revision, collector)
	if err != nil {
		return 0, 0, err
	} else {
		return rev, collector.totalCount, nil
	}
}

func (f *FDB) listKeyValue(caller, prefix, startKey string, limit, maxRevision int64) (resRev int64, resEvents []*server.KeyValue, resErr error) {
	collector := newListCollector(f, limit)
	rev, err := f.listWithCollector(caller, prefix, startKey, maxRevision, collector)
	if err != nil {
		return 0, nil, err
	}
	kvs := make([]*server.KeyValue, 0, len(collector.records))
	for _, revRecord := range collector.records {
		event := revRecordToEvent(revRecord)
		kvs = append(kvs, event.KV)
	}
	return rev, kvs, nil
}

func (f *FDB) listWithCollector(caller, prefix, startKey string, maxRevision int64, collector Collector) (resRev int64, resErr error) {
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
		logrus.Errorf("listWithCollector (%s): prefix=%s, startKey=%s, maxRevision=%d => resRev=%d collector=%v resErr=%v", caller, prefix, startKey, maxRevision, resRev, collector, resErr)
	}()

	var begin, end fdb.Selectable
	if strings.HasSuffix(prefix, "/") {
		// searching for prefix
		packedStartKey := f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{startKey})
		if prefix != startKey {
			// next key after the packedStartKey
			packedStartKeyKey, err := fdb.Strinc(packedStartKey)
			if err != nil {
				return 0, fmt.Errorf("failed to create begin for listKeyValue: %w", err)
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
			return 0, fmt.Errorf("failed to create end for listKeyValue: %w", err)
		}
		// the last key is exclusive https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
		end = fdb.FirstGreaterOrEqual(fdb.Key(packedPrefixKey))
	} else if startKey != prefix {
		return 0, fmt.Errorf("prefix is not equal to startKey. Prefix: %s, startKey: %s", prefix, startKey)
	} else {
		// searching for equality
		k := f.byKeyAndRevision.GetSubspace().Sub(prefix)
		begin, end = k.FDBRangeKeySelectors()
	}

	rev, err := transact(f.db, 0, func(tr fdb.Transaction) (int64, error) {
		it := tr.GetRange(fdb.SelectorRange{Begin: begin, End: end}, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		collector.initBatch()
		var currentRecord *ByKeyAndRevisionRecord = nil
		for collector.canAppendMoreToBatch() && it.Advance() {
			nextKeyAndRevRecord, err := f.byKeyAndRevision.GetFromIterator(it)
			if err != nil {
				return 0, err
			}

			if nextKeyAndRevRecord == nil {
				break
			}

			if currentRecord != nil && currentRecord.Key.Key != nextKeyAndRevRecord.Key.Key {
				if !currentRecord.Value.IsDelete {
					if err := collector.appendToBatch(&tr, currentRecord); err != nil {
						return 0, err
					}
				}
				currentRecord = nil
			}

			if maxRevision == 0 || versionstampToInt64(nextKeyAndRevRecord.Key.Rev) <= maxRevision {
				currentRecord = nextKeyAndRevRecord
			}
		}

		if currentRecord != nil && !currentRecord.Value.IsDelete {
			if !currentRecord.Value.IsDelete {
				if err := collector.appendToBatch(&tr, currentRecord); err != nil {
					return 0, err
				}
			}
		}

		if err := collector.finalizeBatch(); err != nil {
			return 0, err
		}

		if rev, err := tr.GetReadVersion().Get(); err != nil {
			return 0, err
		} else {
			return rev, nil
		}
	})
	collector.appendBatchToResult()

	if err != nil {
		return 0, err
	}

	if maxRevision > rev {
		return rev, server.ErrFutureRev
	}

	return rev, nil
}
