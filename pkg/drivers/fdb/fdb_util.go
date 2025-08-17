package fdb

import (
	"errors"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/sirupsen/logrus"
	"time"
)

const (
	// https://apple.github.io/foundationdb/api-error-codes.html
	notCommittedErrorCode = 1020 // Transaction not committed due to conflict with another transaction

	logConflictingKeys = false

	splitRangeAfterDuration  = 1 * time.Second
	transactionMaxRetryCount = 1000
)

var forceRetryTransaction = func(i int) bool { return false }

type Processor[T any] interface {
	startBatch()
	next(tr *fdb.Transaction, record T) (fdb.KeyConvertible, bool, error)
	endBatch(tr *fdb.Transaction, isLast bool) error
	postBatch()
}

type batchResult struct {
	lastReadKey        fdb.KeyConvertible
	collectorNeedsMore bool
	iteratorHasMore    bool
}

func processRange(db fdb.Database, selector fdb.SelectorRange, collector Processor[*fdb.RangeIterator]) error {
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

func collectBatch(db fdb.Database, selector fdb.SelectorRange, collector Processor[*fdb.RangeIterator]) (batchResult, error) {
	res, err := transact(db, batchResult{}, func(tr fdb.Transaction) (batchResult, error) {
		res := batchResult{collectorNeedsMore: true, iteratorHasMore: true}
		if err := tr.Options().SetTimeout(2 * splitRangeAfterDuration.Milliseconds()); err != nil {
			return res, fmt.Errorf("failed to set timeout limit: %w", err)
		}

		start := time.Now()
		// Snapshot read does not add read conflict ranges
		// https://forums.foundationdb.org/t/java-why-is-setreadversion-not-part-of-readtransaction-readsnapshot/646/11
		it := tr.Snapshot().GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		var firstKey fdb.KeyConvertible
		collector.startBatch()
		for i := 0; res.collectorNeedsMore && res.iteratorHasMore; i++ {
			if !it.Advance() {
				res.iteratorHasMore = false
				break
			}
			if lastKey, collectorNeedsMore, err := collector.next(&tr, it); err != nil {
				return res, err
			} else {
				if firstKey == nil {
					firstKey = lastKey
				}
				res.lastReadKey = lastKey
				res.collectorNeedsMore = collectorNeedsMore
			}
			if time.Since(start) > splitRangeAfterDuration {
				break
			}
		}

		if err := collector.endBatch(&tr, !res.iteratorHasMore); err != nil {
			return res, err
		}

		return res, nil
	})
	collector.postBatch()
	return res, err
}

func transact[T any](d fdb.Database, defaultValue T, f func(fdb.Transaction) (T, error)) (T, error) {
	tr, e := d.CreateTransaction()
	// Any error here is non-retryable
	if e != nil {
		return defaultValue, fmt.Errorf("failed to create a transaction: %w", e)
	}

	wrapped := func() (T, error) {
		defer panicToError(&e)

		// https://forums.foundationdb.org/t/defaults-for-transaction-timeouts-and-retries/315/2
		e = tr.Options().SetRetryLimit(transactionMaxRetryCount)
		if e != nil {
			return defaultValue, fmt.Errorf("failed to set timeout limit: %w", e)
		}

		if logConflictingKeys {
			e = tr.Options().SetReportConflictingKeys()
			if e != nil {
				return defaultValue, fmt.Errorf("failed to set conflicint keys option: %w", e)
			}
		}

		ret, e := f(tr)

		if e == nil {
			e = tr.Commit().Get()
		}

		if logConflictingKeys {
			var fe fdb.Error
			if errors.As(e, &fe) && fe.Code == notCommittedErrorCode {
				//https://forums.foundationdb.org/t/unable-to-use-conflicting-keys-special-keyspace-with-go-bindings/3097/3
				rng := fdb.KeyRange{
					Begin: fdb.Key("\xff\xff/transaction/conflicting_keys/"),
					End:   fdb.Key("\xff\xff/transaction/conflicting_keys/\xff"),
				}
				if kvs, err := tr.GetRange(rng, fdb.RangeOptions{}).GetSliceWithError(); err != nil {
					logrus.Errorf("Unable to read conflicting keys range: %v\n", e)
					e = err
				} else {
					logrus.Warnf("Conflicting keys: '%+v'", kvs)
				}
			}
		}

		return ret, e
	}

	return retryable(wrapped, tr.OnError)
}

func retryable[T any](wrapped func() (T, error), onError func(fdb.Error) fdb.FutureNil) (ret T, e error) {
	for i := 0; ; i++ {
		ret, e = wrapped()

		if forceRetryTransaction(i) {
			// commit_unknown_result
			e = fdb.Error{Code: 1021}
		}

		// No error means success!
		if e == nil {
			return
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
