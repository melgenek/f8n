package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type KeyAndRevision struct {
	Key string
	Rev Revision
}

type ByKeyAndRevisionRecord struct {
	Key   KeyAndRevision
	Value *Record
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

func (s *ByKeyAndRevisionSubspace) Get(tr *fdb.Transaction, key string, rev Revision) (*ByKeyAndRevisionRecord, error) {
	value, err := tr.Get(s.subspace.Pack(tuple.Tuple{key, rev})).Get()
	if err != nil {
		return nil, err
	}

	record, err := s.parseValue(value, key)
	if err != nil {
		return nil, err
	} else {
		return &ByKeyAndRevisionRecord{KeyAndRevision{Key: key, Rev: rev}, record}, nil
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
	rev := k[1].(Revision)
	record, err := s.parseValue(kv.Value, key)
	if err != nil {
		return nil, err
	} else {
		return &ByKeyAndRevisionRecord{KeyAndRevision{Key: key, Rev: rev}, record}, nil
	}
}

func (s *ByKeyAndRevisionSubspace) parseValue(value []byte, key string) (*Record, error) {
	unpackedTuple, err := tuple.Unpack(value)
	if err != nil {
		return nil, err
	}
	record := s.tupleToRecord(unpackedTuple)
	record.Key = key
	return record, nil
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
		CreateRevision: t[3].(Revision),
		PrevRevision:   t[4].(Revision),
		ValueSize:      t[5].(int64),
		WriteUUID:      t[6].(tuple.UUID),
	}
}
