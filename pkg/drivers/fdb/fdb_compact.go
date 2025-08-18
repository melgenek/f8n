package fdb

import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type compactProcessor struct {
	f              *FDB
	batchCompacted tuple.Versionstamp
	lastTr         *fdb.Transaction
}

func newCompactProcessor(f *FDB) *compactProcessor {
	return &compactProcessor{
		f: f,
	}
}

func (c *compactProcessor) startBatch() {
	c.batchCompacted = dummyVersionstamp
}

func (c *compactProcessor) next(tr *fdb.Transaction, it *fdb.RangeIterator) (fdb.KeyConvertible, bool, error) {
	rev, record, err := c.f.byRevision.GetFromIterator(it)
	if err != nil {
		return nil, false, err
	}
	if rev == nil {
		return nil, false, nil
	}

	lastRecord, err := c.f.getLast(tr, record.Key)
	if err != nil {
		return nil, false, err
	}

	if lastRecord.Key.Rev != *rev || record.IsDelete {
		c.f.byKeyAndRevision.Delete(tr, &KeyAndRevision{Key: record.Key, Rev: *rev})
		if err := c.f.byRevision.Delete(tr, *rev); err != nil {
			return nil, false, err
		}
	}
	c.batchCompacted = *rev
	return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev}), true, nil
}

func (c *compactProcessor) endBatch(tr *fdb.Transaction, isLast bool) error {
	if c.batchCompacted != dummyVersionstamp {
		c.f.compactRev.Write(tr, c.batchCompacted)
	}
	if isLast {
		c.lastTr = tr
	}
	return nil
}

func (c *compactProcessor) postBatch() {
}

func (c *compactProcessor) String() string {
	return fmt.Sprintf("compactProcessor{%s}", c.batchCompacted)
}

func (f *FDB) Compact(_ context.Context, endRev int64) (int64, error) {
	begin, _ := f.byRevision.GetSubspace().FDBRangeKeySelectors()
	end := fdb.FirstGreaterThan(f.byRevision.GetSubspace().Pack(tuple.Tuple{int64ToVersionstamp(endRev)}))

	processor := newCompactProcessor(f)
	if err := processRange(f.db, fdb.SelectorRange{Begin: begin, End: end}, processor); err != nil {
		return 0, err
	}
	return processor.lastTr.GetCommittedVersion()
}
