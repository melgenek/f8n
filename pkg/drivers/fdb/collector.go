package fdb

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type Collector interface {
	initBatch()
	appendToBatch(tr *fdb.Transaction, record *ByKeyAndRevisionRecord) error
	canAppendMoreToBatch() bool
	finalizeBatch() error
	appendBatchToResult()
}

type countCollector struct {
	totalCount int64
	batchCount int64
}

func (c *countCollector) initBatch() {
	c.batchCount = 0
}

func (c *countCollector) appendToBatch(_ *fdb.Transaction, _ *ByKeyAndRevisionRecord) error {
	c.batchCount++
	return nil
}

func (c *countCollector) canAppendMoreToBatch() bool {
	return true
}

func (c *countCollector) finalizeBatch() error {
	return nil
}

func (c *countCollector) appendBatchToResult() {
	c.totalCount += c.batchCount
}

func (c *countCollector) String() string {
	return fmt.Sprintf("{count=%d}", c.totalCount)
}

type listCollector struct {
	f              *FDB
	limit          int64
	records        []*RevRecord
	batchIterators []*fdb.RangeIterator
	batchRecords   []*RevRecord
}

func newListCollector(f *FDB, limit int64) *listCollector {
	capacity := limit
	if capacity == 0 {
		capacity = 1000
	}
	return &listCollector{
		f:              f,
		limit:          limit,
		records:        make([]*RevRecord, 0, capacity),
		batchIterators: make([]*fdb.RangeIterator, 0, capacity),
		batchRecords:   make([]*RevRecord, 0, capacity),
	}
}

func (c *listCollector) initBatch() {
	c.batchIterators = c.batchIterators[len(c.batchIterators):]
	c.batchRecords = c.batchRecords[len(c.batchRecords):]
}

func (c *listCollector) appendToBatch(tr *fdb.Transaction, record *ByKeyAndRevisionRecord) error {
	recordIt, err := c.f.byRevision.GetIterator(tr, record.Key.Rev)
	if err != nil {
		return err
	}
	c.batchIterators = append(c.batchIterators, recordIt)
	return nil
}

func (c *listCollector) canAppendMoreToBatch() bool {
	return c.limit == 0 || int64(len(c.batchIterators)+len(c.records)) < c.limit
}

func (c *listCollector) finalizeBatch() error {
	for _, it := range c.batchIterators {
		rev, record, err := c.f.byRevision.GetFromIterator(it)
		if err != nil {
			return err
		}
		if record == nil {
			return fmt.Errorf("record is nil for revision %d", rev)
		}
		c.batchRecords = append(c.batchRecords, &RevRecord{Rev: rev, Record: record})
	}
	return nil
}

func (c *listCollector) appendBatchToResult() {
	for _, record := range c.batchRecords {
		c.records = append(c.records, record)
	}
}

func (c *listCollector) String() string {
	return fmt.Sprintf("{records=%d}", len(c.records))
}
