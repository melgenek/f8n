package fdb

import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"math"
)

type compactProcessor struct {
	f              *FDB
	batchCompacted Revision
	batchRev       Revision
	rev            Revision
}

func newCompactProcessor(f *FDB) *compactProcessor {
	return &compactProcessor{
		f: f,
	}
}

func (c *compactProcessor) startBatch() {
	c.batchCompacted = zeroRevision
}

func (c *compactProcessor) next(tr *fdb.Transaction, it *fdb.RangeIterator) (fdb.Key, bool, error) {
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
		fmt.Println("Deleting record", record.Key, *rev)
		c.f.byKeyAndRevision.Delete(tr, &KeyAndRevision{Key: record.Key, Rev: *rev})
		if err := c.f.byRevision.Delete(tr, *rev); err != nil {
			return nil, false, err
		}
	}
	c.batchCompacted = *rev
	return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev, math.MaxInt64}), true, nil
}

func (c *compactProcessor) endBatch(tr *fdb.Transaction, isLast bool) error {
	if c.batchCompacted != zeroRevision {
		c.f.compactRev.Write(tr, c.batchCompacted)
	}
	if isLast {
		if rev, err := c.f.rev.Get(tr); err != nil {
			return err
		} else {
			c.batchRev = rev
		}
	}
	return nil
}

func (c *compactProcessor) postBatch() {
	c.rev = c.batchRev
}

func (c *compactProcessor) String() string {
	return fmt.Sprintf("compactProcessor{%v}", c.batchCompacted)
}

func (f *FDB) Compact(_ context.Context, endRev int64) (int64, error) {
	begin, _ := f.byRevision.GetSubspace().FDBRangeKeySelectors()
	end := fdb.FirstGreaterThan(f.byRevision.GetSubspace().Pack(tuple.Tuple{endRev}))

	processor := newCompactProcessor(f)
	if err := processRange(f.db, fdb.SelectorRange{Begin: begin, End: end}, processor); err != nil {
		return 0, err
	} else {
		return processor.rev, nil
	}
}
