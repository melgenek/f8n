package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func (f *FDB) DbSize(ctx context.Context) (int64, error) {
	result, err := f.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.GetEstimatedRangeSizeBytes(f.kine).Get()
	})
	return result.(int64), err
}

func (f *FDB) Compact(ctx context.Context, revision int64) (int64, error) {
	return 0, nil
}
