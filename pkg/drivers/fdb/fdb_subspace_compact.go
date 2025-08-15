package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type CompactRevisionSubspace struct {
	subspace subspace.Subspace
}

func CreateCompactRevisionSubspace(directory directory.DirectorySubspace) *CompactRevisionSubspace {
	return &CompactRevisionSubspace{subspace: directory.Sub("compactRevision")}
}

func (s *CompactRevisionSubspace) Write(tr *fdb.Transaction, rev tuple.Versionstamp) {
	tr.Set(s.subspace, tuple.Tuple{rev}.Pack())
}

func (s *CompactRevisionSubspace) Get(tr *fdb.Transaction) (tuple.Versionstamp, error) {
	value, err := tr.Get(s.subspace).Get()
	if err != nil {
		return dummyVersionstamp, err
	}
	t, err := tuple.Unpack(value)
	if err != nil {
		return dummyVersionstamp, err
	}
	if value == nil {
		return dummyVersionstamp, nil
	}
	return t[0].(tuple.Versionstamp), nil
}
