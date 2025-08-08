package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func (f *FDB) DbSize(_ context.Context) (int64, error) {
	result, err := transact(f.db, 0, func(tr fdb.Transaction) (int64, error) {
		return tr.GetEstimatedRangeSizeBytes(f.kine).Get()
	})
	return result, err
}

func (f *FDB) Compact(_ context.Context, revision int64) (int64, error) {
	return 0, nil
}
