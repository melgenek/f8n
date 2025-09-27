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
	lastWatchRev := f.lastWatchRev.Load()
	if lastWatchRev != 0 {
		return lastWatchRev, nil
	} else {
		rev, err := transact(f.db, 0, func(tr fdb.Transaction) (ret int64, e error) {
			if latestRev, err := f.rev.GetLatestRev(&tr); err != nil {
				return 0, err
			} else {
				return latestRev.Get()
			}
		})

		return rev, err
	}
}

func (f *FDB) List(_ context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	rev, kvs, err := f.listKeyValue("List", prefix, startKey, limit, revision, keysOnly)
	if err != nil {
		return rev, nil, err
	}
	return rev, kvs, nil
}

func (f *FDB) Get(_ context.Context, key, rangeEnd string, _, revision int64, keysOnly bool) (revRet int64, kvRet *server.KeyValue, errRet error) {
	if rangeEnd != "" {
		return 0, nil, fmt.Errorf("invalid 'rangeEnd' for Get. Expected: '', got %s", rangeEnd)
	}
	rev, kvs, err := f.listKeyValue("Get", key, key, 1, revision, keysOnly)
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
	if !it.Advance() {
		return nil, nil
	}
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

type countCollector struct {
	totalCount int64
	batchCount int64
}

func (c *countCollector) startBatch() {
	c.batchCount = 0
}

func (c *countCollector) next(*fdb.Transaction, *ByKeyAndRevisionRecord) (fdb.Key, bool, error) {
	c.batchCount++
	return nil, true, nil
}

func (c *countCollector) endBatch(*fdb.Transaction, bool) error {
	return nil
}

func (c *countCollector) postBatch() {
	c.totalCount += c.batchCount
}

func (c *countCollector) String() string {
	return fmt.Sprintf("{count=%d}", c.totalCount)
}

func (f *FDB) listKeyValue(caller, prefix, startKey string, limit, maxRevision int64, keysOnly bool) (resRev int64, resEvents []*server.KeyValue, resErr error) {
	collector := newListCollector(f, limit, keysOnly)
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

type listCollector struct {
	f              *FDB
	limit          int64
	keysOnly       bool
	records        []*RevRecord
	batchIterators []*fdb.RangeIterator
	batchRecords   []*RevRecord
}

func newListCollector(f *FDB, limit int64, keysOnly bool) *listCollector {
	capacity := limit
	if capacity == 0 {
		capacity = 100
	}
	return &listCollector{
		f:              f,
		limit:          limit,
		keysOnly:       keysOnly,
		records:        make([]*RevRecord, 0, capacity),
		batchIterators: make([]*fdb.RangeIterator, 0, capacity),
		batchRecords:   make([]*RevRecord, 0, capacity),
	}
}

func (c *listCollector) startBatch() {
	c.batchIterators = c.batchIterators[len(c.batchIterators):]
	c.batchRecords = c.batchRecords[len(c.batchRecords):]
}

func (c *listCollector) next(tr *fdb.Transaction, record *ByKeyAndRevisionRecord) (fdb.Key, bool, error) {
	if c.keysOnly {
		c.batchRecords = append(c.batchRecords, &RevRecord{Rev: record.Key.Rev, Record: record.Value})
	} else {
		recordIt, err := c.f.byRevision.GetIterator(tr, record.Key.Rev)
		if err != nil {
			return nil, false, err
		}
		c.batchIterators = append(c.batchIterators, recordIt)

		if len(c.batchIterators) >= 1 {
			if err := c.fetchIterators(); err != nil {
				return nil, false, err
			}
		}
	}
	return nil, c.needMore(), nil
}

func (c *listCollector) needMore() bool {
	return c.limit == 0 || int64(len(c.batchIterators)+len(c.batchRecords)+len(c.records)) < c.limit
}

func (c *listCollector) endBatch(*fdb.Transaction, bool) error {
	if c.keysOnly {
		return nil
	} else {
		return c.fetchIterators()
	}
}

func (c *listCollector) fetchIterators() error {
	for _, it := range c.batchIterators {
		rev, record, err := c.f.byRevision.GetFromIterator(it)
		if err != nil {
			return err
		} else if rev == nil {
			return fmt.Errorf("no records in by rev iterator")
		} else {
			c.batchRecords = append(c.batchRecords, &RevRecord{Rev: *rev, Record: record})
		}
	}
	c.batchIterators = c.batchIterators[len(c.batchIterators):]
	return nil
}

func (c *listCollector) postBatch() {
	c.records = append(c.records, c.batchRecords...)
}

func (c *listCollector) String() string {
	return fmt.Sprintf("{records=%d,batchRecords=%d}", len(c.records), len(c.batchRecords))
}

type recordCollector struct {
	f                  *FDB
	maxRevision        int64
	inner              Processor[*ByKeyAndRevisionRecord]
	currentRecord      *ByKeyAndRevisionRecord
	batchCurrentRecord *ByKeyAndRevisionRecord
	rev                int64
	batchRev           int64
}

func newRecordCollector(f *FDB, maxRevision int64, inner Processor[*ByKeyAndRevisionRecord]) *recordCollector {
	return &recordCollector{
		f:           f,
		maxRevision: maxRevision,
		inner:       inner,
	}
}

func (c *recordCollector) startBatch() {
	c.inner.startBatch()
	c.batchCurrentRecord = c.currentRecord
	c.batchRev = 0
}

func (c *recordCollector) next(tr *fdb.Transaction, it *fdb.RangeIterator) (fdb.Key, bool, error) {
	nextKeyAndRevRecord, err := c.f.byKeyAndRevision.GetFromIterator(it)
	if err != nil {
		return nil, false, err
	}
	if nextKeyAndRevRecord == nil {
		return nil, false, nil
	}

	if c.batchCurrentRecord != nil && c.batchCurrentRecord.Key.Key != nextKeyAndRevRecord.Key.Key {
		if !c.batchCurrentRecord.Value.IsDelete {
			if _, _, err := c.inner.next(tr, c.batchCurrentRecord); err != nil {
				return nil, false, err
			}
		}
		c.batchCurrentRecord = nil
	}

	recordRev := VersionstampToInt64(nextKeyAndRevRecord.Key.Rev)
	if (c.maxRevision == 0 || recordRev <= c.maxRevision) && (c.rev == 0 || recordRev <= c.rev) {
		c.batchCurrentRecord = nextKeyAndRevRecord
	}

	return c.f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{nextKeyAndRevRecord.Key.Key, nextKeyAndRevRecord.Key.Rev}), true, nil
}

func (c *recordCollector) endBatch(tr *fdb.Transaction, isLast bool) error {
	if isLast && c.batchCurrentRecord != nil && !c.batchCurrentRecord.Value.IsDelete {
		if _, _, err := c.inner.next(tr, c.batchCurrentRecord); err != nil {
			return err
		}
	}

	if err := c.inner.endBatch(tr, isLast); err != nil {
		return err
	}

	// Get the read revision for the first batch.
	// Do not read records that might've been concurrently added that are over this revision.
	if c.rev == 0 {
		if latestRevF, err := c.f.rev.GetLatestRev(tr); err != nil {
			return err
		} else if rev, err := latestRevF.Get(); err != nil {
			return err
		} else {
			c.batchRev = rev
		}
	}

	// The requested revision has been compacted
	if c.maxRevision > 0 {
		if compactRev, err := c.f.compactRev.Get(tr); err != nil {
			return err
		} else if c.maxRevision < VersionstampToInt64(compactRev) {
			return server.ErrCompacted
		}
	}

	return nil
}

func (c *recordCollector) postBatch() {
	c.inner.postBatch()
	c.rev = c.batchRev
	c.currentRecord = c.batchCurrentRecord
}

func (c *recordCollector) String() string {
	return fmt.Sprintf("{rev=%d, currentRecord=%v, inner=%v}", c.rev, c.currentRecord, c.inner)
}

func (f *FDB) listWithCollector(caller, prefix, startKey string, maxRevision int64, collector Processor[*ByKeyAndRevisionRecord]) (resRev int64, resErr error) {
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
		logrus.Tracef("listWithCollector (%s): prefix=%s, startKey=%s, maxRevision=%d => resRev=%d collector=%v resErr=%v", caller, prefix, startKey, maxRevision, resRev, collector, resErr)
	}()

	var begin, end fdb.Selectable
	if strings.HasSuffix(prefix, "/") {
		// searching for prefix
		packedStartKey := f.byKeyAndRevision.GetSubspace().Pack(tuple.Tuple{startKey})
		if prefix != startKey {
			// next key afterAll the packedStartKey
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

	rc := newRecordCollector(f, maxRevision, collector)
	err := processRange(f.db, fdb.SelectorRange{Begin: begin, End: end}, rc)
	if err != nil {
		return 0, err
	}

	if maxRevision > rc.rev {
		return rc.rev, server.ErrFutureRev
	}

	return rc.rev, nil
}
