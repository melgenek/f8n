package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type KeyAndRevision struct {
	Key string
	Rev tuple.Versionstamp
}

type ByKeyAndRevisionValue struct {
	IsCreate       bool
	IsDelete       bool
	CreateRevision tuple.Versionstamp
	WriteUUID      tuple.UUID
}

type ByKeyAndRevisionRecord struct {
	Key   KeyAndRevision
	Value ByKeyAndRevisionValue
}

func (r *ByKeyAndRevisionRecord) GetCreateRevision() tuple.Versionstamp {
	if r.Value.IsCreate {
		return r.Key.Rev
	} else {
		return r.Value.CreateRevision
	}
}

type ByKeyAndRevisionSubspace struct {
	subspace subspace.Subspace
}

func CreateByKeyRevisionSubspace(directory directory.DirectorySubspace) *ByKeyAndRevisionSubspace {
	return &ByKeyAndRevisionSubspace{
		subspace: directory.Sub("byKeyAndRevision"),
	}
}

func (s *ByKeyAndRevisionSubspace) GetSubspace() subspace.Subspace {
	return s.subspace
}

func (s *ByKeyAndRevisionSubspace) Write(tr *fdb.Transaction, key *KeyAndRevision, value *ByKeyAndRevisionValue) error {
	if revisionKey, err := s.subspace.PackWithVersionstamp(tuple.Tuple{key.Key, key.Rev}); err != nil {
		return err
	} else {
		tr.SetVersionstampedKey(revisionKey, tuple.Tuple{value.IsCreate, value.IsDelete, value.CreateRevision, value.WriteUUID}.Pack())
		return nil
	}
}

func (s *ByKeyAndRevisionSubspace) GetFromIterator(it *fdb.RangeIterator) (*ByKeyAndRevisionRecord, error) {
	if !it.Advance() {
		return nil, nil
	}
	kv, err := it.Get()
	if err != nil {
		return nil, err
	}
	return s.parseKV(kv)
}

func (s *ByKeyAndRevisionSubspace) parseKV(kv fdb.KeyValue) (*ByKeyAndRevisionRecord, error) {
	k, err := s.subspace.Unpack(kv.Key)
	if err != nil {
		return nil, err
	}
	key := k[0].(string)
	versionstamp := k[1].(tuple.Versionstamp)
	unpackedTuple, err := tuple.Unpack(kv.Value)
	if err != nil {
		return nil, err
	}
	isCreate := unpackedTuple[0].(bool)
	isDelete := unpackedTuple[1].(bool)
	createRevision := unpackedTuple[2].(tuple.Versionstamp)
	writeUUID := unpackedTuple[3].(tuple.UUID)
	return &ByKeyAndRevisionRecord{
			KeyAndRevision{Key: key, Rev: versionstamp},
			ByKeyAndRevisionValue{IsCreate: isCreate, IsDelete: isDelete, CreateRevision: createRevision, WriteUUID: writeUUID},
		},
		nil
}
