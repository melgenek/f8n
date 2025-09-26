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

type ByKeyAndRevisionRecord struct {
	Key   KeyAndRevision
	Value *Record
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

func (s *ByKeyAndRevisionSubspace) Write(tr *fdb.Transaction, key *KeyAndRevision, record *Record) error {
	packKey, setValue := GetWriteOps(tr, s.subspace)
	if revisionKey, err := packKey(tuple.Tuple{key.Key, key.Rev}); err != nil {
		return err
	} else {
		setValue(revisionKey, s.recordToTuple(record).Pack())
		return nil
	}
}

func (s *ByKeyAndRevisionSubspace) Delete(tr *fdb.Transaction, key *KeyAndRevision) {
	tr.Clear(s.subspace.Pack(tuple.Tuple{key.Key, key.Rev}))
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
	record := s.tupleToRecord(unpackedTuple)
	record.Key = key
	return &ByKeyAndRevisionRecord{KeyAndRevision{Key: key, Rev: versionstamp}, record}, nil
}

func (s *ByKeyAndRevisionSubspace) recordToTuple(record *Record) tuple.Tuple {
	return tuple.Tuple{
		record.IsDelete,
		record.IsCreate,
		record.Lease,
		record.CreateRevision,
		record.PrevRevision,
		record.ValueSize,
		record.WriteUUID,
	}
}

func (s *ByKeyAndRevisionSubspace) tupleToRecord(t tuple.Tuple) *Record {
	return &Record{
		IsDelete:       t[0].(bool),
		IsCreate:       t[1].(bool),
		Lease:          t[2].(int64),
		CreateRevision: t[3].(tuple.Versionstamp),
		PrevRevision:   t[4].(tuple.Versionstamp),
		ValueSize:      t[5].(int64),
		WriteUUID:      t[6].(tuple.UUID),
	}
}
