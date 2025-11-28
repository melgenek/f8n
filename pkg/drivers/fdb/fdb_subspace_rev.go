package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type PackKey = func(t tuple.Tuple) (fdb.Key, error)
type SetValue = func(fdb.KeyConvertible, []byte)

type RevisionSubspace struct {
	subspace subspace.Subspace
}

func CreateRevisionSubspace(directory directory.DirectorySubspace) *RevisionSubspace {
	return &RevisionSubspace{subspace: directory.Sub("rev")}
}

func (s *RevisionSubspace) Get(tr *fdb.Transaction) (Revision, error) {
	value, err := tr.Get(s.subspace).Get()
	if err != nil {
		return -1, err
	}
	if value == nil {
		return 1, nil
	}
	t, err := tuple.Unpack(value)
	if err != nil {
		return -1, err
	}

	return t[0].(int64), nil
}

func (s *RevisionSubspace) Set(tr *fdb.Transaction, value Revision) {
	tr.Set(s.subspace, tuple.Tuple{value}.Pack())
}

func GetWriteOps(tr *fdb.Transaction, subspace subspace.Subspace) (PackKey, SetValue) {
	return func(t tuple.Tuple) (fdb.Key, error) {
		return subspace.Pack(t), nil
	}, tr.Set
}
