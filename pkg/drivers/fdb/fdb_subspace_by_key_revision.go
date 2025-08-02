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

type RevRecord struct {
	Rev    tuple.Versionstamp
	Record *Record
}

func (r *RevRecord) GetCreateRevision() tuple.Versionstamp {
	if r.Record.IsCreate {
		return r.Rev
	} else {
		return r.Record.CreateRevision
	}
}

type Record struct {
	Key            string
	IsDelete       bool
	IsCreate       bool
	Lease          int64
	CreateRevision tuple.Versionstamp
	PrevRevision   tuple.Versionstamp
	Value          []byte
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

func (s *ByKeyAndRevisionSubspace) Write(tr *fdb.Transaction, key *KeyAndRevision, value *Record) error {
	record := recordToTuple(value).Pack()
	if revisionKey, err := s.subspace.PackWithVersionstamp(tuple.Tuple{key.Key, key.Rev}); err != nil {
		return err
	} else {
		tr.SetVersionstampedKey(revisionKey, record)
		return nil
	}
}

func (s *ByKeyAndRevisionSubspace) Get(tr *fdb.Transaction, key *KeyAndRevision) (*Record, error) {
	eventBytes, err := tr.Get(s.subspace.Pack(tuple.Tuple{key.Key, key.Rev})).Get()
	if err != nil {
		return nil, err
	}
	return parseValue(eventBytes)
}

func (s *ByKeyAndRevisionSubspace) ParseKV(kv fdb.KeyValue) (*KeyAndRevision, *Record, error) {
	k, err := s.subspace.Unpack(kv.Key)
	if err != nil {
		return nil, nil, err
	}
	key := k[0].(string)
	versionstamp := k[1].(tuple.Versionstamp)
	event, err := parseValue(kv.Value)
	if err != nil {
		return nil, nil, err
	}
	return &KeyAndRevision{Key: key, Rev: versionstamp}, event, nil
}

func parseValue(value []byte) (*Record, error) {
	unpackedTuple, err := tuple.Unpack(value)
	if err != nil {
		return nil, err
	}
	event := tupleToRecord(unpackedTuple)
	return event, nil
}

func recordToTuple(record *Record) tuple.Tuple {
	return tuple.Tuple{
		record.Key,
		record.IsDelete,
		record.IsCreate,
		record.Lease,
		record.CreateRevision,
		record.PrevRevision,
		record.Value,
	}
}

func tupleToRecord(t tuple.Tuple) *Record {
	return &Record{
		Key:            t[0].(string),
		IsDelete:       t[1].(bool),
		IsCreate:       t[2].(bool),
		Lease:          t[3].(int64),
		CreateRevision: t[4].(tuple.Versionstamp),
		PrevRevision:   t[5].(tuple.Versionstamp),
		Value:          t[6].([]byte),
	}
}
