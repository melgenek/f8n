package fdb

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"golang.org/x/sync/errgroup"
)

type compactProcessor struct {
	f    *FDB
	g    *errgroup.Group
	gCtx context.Context
	// output
	batchCompactedMutex sync.Mutex
	batchCompacted      Revision
	batchRev            Revision
	rev                 Revision
}

func newCompactProcessor(f *FDB) *compactProcessor {
	g, ctx := errgroup.WithContext(f.ctx)
	g.SetLimit(50)
	return &compactProcessor{
		f:    f,
		g:    g,
		gCtx: ctx,
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

	c.g.Go(func() error {
		lastRecord, err := c.f.getLast(tr, record.Key, toSnapshot)
		if err != nil {
			return err
		}
		if lastRecord == nil {
			return nil
		}

		if lastRecord.Key.Rev != *rev || record.IsDelete {
			c.f.byKeyAndRevision.Delete(tr, &KeyAndRevision{Key: record.Key, Rev: *rev})
			if err := c.f.byRevision.Delete(tr, *rev); err != nil {
				return err
			}
		}
		c.batchCompactedMutex.Lock()
		if c.batchCompacted < *rev {
			c.batchCompacted = *rev
		}
		c.batchCompactedMutex.Unlock()
		return nil
	})

	if err := c.gCtx.Err(); err != nil {
		return nil, false, err
	} else {
		return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev, math.MaxInt64}), true, nil
	}
}

func (c *compactProcessor) endBatch(tr *fdb.Transaction, isLast bool) error {
	if c.batchCompacted != zeroRevision {
		c.f.compactRev.Write(tr, c.batchCompacted)
	}
	if isLast {
		if rev, err := c.f.rev.Get(tr, toSnapshot); err != nil {
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
	if err := processRange(f.db, fdb.SelectorRange{Begin: begin, End: end}, processor, splitRangeAfterDurationForRead, toSnapshot); err != nil {
		return 0, err
	} else {
		return processor.rev, nil
	}
}
