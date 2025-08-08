package fdb

import (
	"errors"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var forceRetryTransaction = func(i int) bool { return false }

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
