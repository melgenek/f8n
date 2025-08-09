package fdb

import (
	"errors"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"time"
)

var forceRetryTransaction = func(i int) bool { return false }

type Collector[T any] interface {
	startBatch()
	next(tr *fdb.Transaction, record T) (fdb.KeyConvertible, bool, error)
	endBatch(tr *fdb.Transaction, isLast bool) error
	appendBatchToResult()
}

type batchResult struct {
	lastReadKey        fdb.KeyConvertible
	collectorNeedsMore bool
	iteratorHasMore    bool
}

func collectRange(db fdb.Database, selector fdb.SelectorRange, collector Collector[*fdb.RangeIterator]) error {
	beginSelector := selector.Begin

	for i := 0; ; i++ {
		res, err := collectBatch(db, fdb.SelectorRange{Begin: beginSelector, End: selector.End}, collector)
		if err != nil {
			return err
		}
		if !res.collectorNeedsMore {
			break
		}
		if !res.iteratorHasMore {
			break
		}
		beginSelector = fdb.FirstGreaterThan(res.lastReadKey)
	}

	return nil
}

func collectBatch(db fdb.Database, selector fdb.SelectorRange, collector Collector[*fdb.RangeIterator]) (batchResult, error) {
	start := time.Now()
	shouldBreakTransaction := func(tr fdb.Transaction) (bool, error) {
		if time.Since(start) > 1*time.Second {
			return true, nil
		}
		size, err := tr.GetApproximateSize().Get()
		if err != nil {
			return true, err
		}
		// https://web.archive.org/web/20150325020408/http://community.foundationdb.com/questions/4118/future-version.html
		// https://forums.foundationdb.org/t/optimizing-a-single-large-transaction-10-000-keys/1961/2
		// https://github.com/apple/foundationdb/issues/1681
		// Checking the transaction size is under 100KiB.
		// Value sizes are not included in the read transaction size.
		if size > 100*1024 {
			return true, nil
		}
		return false, nil
	}

	res, err := transact(db, batchResult{}, func(tr fdb.Transaction) (batchResult, error) {
		it := tr.GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		res := batchResult{collectorNeedsMore: true, iteratorHasMore: true}
		collector.startBatch()
		for res.collectorNeedsMore && res.iteratorHasMore {
			if !it.Advance() {
				res.iteratorHasMore = false
				break
			}
			if lastKey, collectorNeedsMore, err := collector.next(&tr, it); err != nil {
				return res, err
			} else {
				res.lastReadKey = lastKey
				res.collectorNeedsMore = collectorNeedsMore
			}
			if shouldBreak, err := shouldBreakTransaction(tr); err != nil {
				return res, err
			} else if shouldBreak {
				return res, nil
			}
		}

		if err := collector.endBatch(&tr, !res.iteratorHasMore); err != nil {
			return res, err
		}

		return res, nil
	})
	collector.appendBatchToResult()
	return res, err
}

func transact[T any](d fdb.Database, defaultValue T, f func(fdb.Transaction) (T, error)) (T, error) {
	tr, e := d.CreateTransaction()
	// Any error here is non-retryable
	if e != nil {
		return defaultValue, fmt.Errorf("failed to create a transaction: %w", e)
	}

	wrapped := func() (ret T, e error) {
		defer panicToError(&e)

		e = tr.Options().SetRetryLimit(3)
		if e != nil {
			return defaultValue, fmt.Errorf("failed to set retry limit: %w", e)
		}

		ret, e = f(tr)

		if e == nil {
			e = tr.Commit().Get()
		}

		return
	}

	return retryable(wrapped, tr.OnError)
}

func retryable[T any](wrapped func() (T, error), onError func(fdb.Error) fdb.FutureNil) (ret T, e error) {
	for i := 0; ; i++ {
		ret, e = wrapped()

		// No error means success!
		if e == nil {
			if forceRetryTransaction(i) {
				// commit_unknown_result
				onError(fdb.Error{1021}).MustGet()
			} else {
				return
			}
		}

		// Check if the error chain contains an fdb.Error
		var ep fdb.Error
		if errors.As(e, &ep) {
			e = onError(ep).Get()
		}

		// If OnError returns an error, then it's not
		// retryable; otherwise take another pass at things
		if e != nil {
			return
		}
	}
}

func panicToError(e *error) {
	if r := recover(); r != nil {
		fe, ok := r.(fdb.Error)
		if ok {
			*e = fe
		} else {
			panic(r)
		}
	}
}
