package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

type Subspace[K any, V any] interface {
	GetSubspace() subspace.Subspace
	Write(tr *fdb.Transaction, key K, value V) error
	Get(tr *fdb.Transaction, key K) (V, error)
	ParseKV(kv fdb.KeyValue) (K, V, error)
}
