package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"math"
)

func (f *FDB) ReadWAL() ([]*RevRecord, error) {
	collector := newWalCollector(f)
	begin, end := f.byRevision.GetSubspace().FDBRangeKeySelectors()
	err := processRange(f.db, fdb.SelectorRange{Begin: begin, End: end}, collector)
	return collector.records, err
}

type walCollector struct {
	f *FDB
	// output
	batchEvents []*RevRecord
	records     []*RevRecord
}

func newWalCollector(f *FDB) *walCollector {
	return &walCollector{
		f:           f,
		batchEvents: make([]*RevRecord, 0, 1000),
		records:     make([]*RevRecord, 0, 1000),
	}
}

func (c *walCollector) startBatch() {
	c.batchEvents = c.batchEvents[len(c.batchEvents):]
}

func (c *walCollector) next(_ *fdb.Transaction, it *fdb.RangeIterator) (fdb.Key, bool, error) {
	rev, record, err := c.f.byRevision.GetFromIterator(it)
	if err != nil {
		return nil, false, err
	}
	if record == nil {
		return nil, false, nil
	}

	c.batchEvents = append(c.batchEvents, &RevRecord{Rev: *rev, Record: record})

	return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev, math.MaxInt64}), true, nil
}

func (c *walCollector) endBatch(*fdb.Transaction, bool) error {
	return nil
}

func (c *walCollector) postBatch() {
	c.records = append(c.records, c.batchEvents...)
}
