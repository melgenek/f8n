package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

var _ Subspace[tuple.Versionstamp, string] = (*ByRevisionSubspace)(nil)

type ByRevisionSubspace struct {
	subspace subspace.Subspace
}

func CreateByRevisionSubspace(directory directory.DirectorySubspace) *ByRevisionSubspace {
	return &ByRevisionSubspace{
		subspace: directory.Sub("byRevision"),
	}
}

func (s *ByRevisionSubspace) GetSubspace() subspace.Subspace {
	return s.subspace
}

func (s *ByRevisionSubspace) Write(tr *fdb.Transaction, rev tuple.Versionstamp, key string) error {
	if revisionKey, err := s.subspace.PackWithVersionstamp(tuple.Tuple{rev}); err != nil {
		return err
	} else {
		tr.SetVersionstampedKey(revisionKey, tuple.Tuple{key}.Pack())
		return nil
	}
}

func (s *ByRevisionSubspace) Get(tr *fdb.Transaction, key tuple.Versionstamp) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *ByRevisionSubspace) ParseKV(kv fdb.KeyValue) (tuple.Versionstamp, string, error) {
	k, err := s.subspace.Unpack(kv.Key)
	if err != nil {
		return stubVersionstamp, "", err
	}
	versionstamp := k[0].(tuple.Versionstamp)
	unpackedTuple, err := tuple.Unpack(kv.Value)
	if err != nil {
		return stubVersionstamp, "", err
	}
	key := unpackedTuple[0].(string)
	return versionstamp, key, nil
}
