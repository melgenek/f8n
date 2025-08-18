package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	chunkSize = 10000
)

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
	ValueSize      int64
	Value          []byte
}

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

func (s *ByRevisionSubspace) Write(tr *fdb.Transaction, rev tuple.Versionstamp, record *Record) error {
	record.ValueSize = int64(len(record.Value))
	if len(record.Value) > chunkSize {
		if err := s.writeBlob(tr, rev, record.Value[chunkSize:]); err != nil {
			return err
		}
		record.Value = record.Value[:chunkSize]
	}

	if revisionKey, err := s.subspace.PackWithVersionstamp(tuple.Tuple{rev}); err != nil {
		return err
	} else {
		tr.SetVersionstampedKey(revisionKey, recordToTuple(record).Pack())
	}

	return nil
}

func (s *ByRevisionSubspace) ParseKV(kv fdb.KeyValue) (tuple.Versionstamp, *Record, error) {
	k, err := s.subspace.Unpack(kv.Key)
	if err != nil {
		return dummyVersionstamp, nil, err
	}
	versionstamp := k[0].(tuple.Versionstamp)
	unpackedTuple, err := tuple.Unpack(kv.Value)
	if err != nil {
		return dummyVersionstamp, nil, err
	}
	event := tupleToRecord(unpackedTuple)
	return versionstamp, event, nil
}

func (s *ByRevisionSubspace) Get(tr *fdb.Transaction, rev tuple.Versionstamp) (*Record, error) {
	it, err := s.GetIterator(tr, rev)
	if err != nil {
		return nil, err
	}
	_, record, err := s.GetFromIterator(it)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *ByRevisionSubspace) Delete(tr *fdb.Transaction, rev tuple.Versionstamp) error {
	selector, err := fdb.PrefixRange(s.subspace.Pack(tuple.Tuple{rev}))
	if err != nil {
		return err
	}
	tr.ClearRange(selector)
	return nil
}

func (s *ByRevisionSubspace) GetIterator(tr *fdb.Transaction, rev tuple.Versionstamp) (*fdb.RangeIterator, error) {
	selector, err := fdb.PrefixRange(s.subspace.Pack(tuple.Tuple{rev}))
	if err != nil {
		return nil, err
	}
	it := tr.GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()
	return it, nil
}

func (s *ByRevisionSubspace) GetFromIterator(it *fdb.RangeIterator) (*tuple.Versionstamp, *Record, error) {
	if !it.Advance() {
		return nil, nil, nil
	}
	kv, err := it.Get()
	if err != nil {
		return nil, nil, err
	}
	rev, record, err := s.ParseKV(kv)
	if err != nil {
		return nil, nil, err
	}
	if record.ValueSize > int64(len(record.Value)) {
		buf := make([]byte, record.ValueSize)
		copy(buf, record.Value)
		offset := len(record.Value)
		if err := s.getBlob(it, buf, offset); err != nil {
			return nil, nil, err
		}
		record.Value = buf
	}
	return &rev, record, nil
}

func (s *ByRevisionSubspace) getBlob(it *fdb.RangeIterator, buf []byte, offset int) error {
	for offset != len(buf) && it.Advance() {
		if chunkKv, err := it.Get(); err != nil {
			return err
		} else {
			copy(buf[offset:], chunkKv.Value)
			offset += len(chunkKv.Value)
		}
	}
	return nil
}

func (s *ByRevisionSubspace) writeBlob(tr *fdb.Transaction, rev tuple.Versionstamp, value []byte) error {
	for offset := 0; offset < len(value); offset += chunkSize {
		end := offset + chunkSize
		if end > len(value) {
			end = len(value)
		}

		if key, err := s.subspace.PackWithVersionstamp(tuple.Tuple{rev, offset}); err != nil {
			return err
		} else {
			tr.SetVersionstampedKey(key, value[offset:end])
		}
	}
	return nil
}

func recordToTuple(record *Record) tuple.Tuple {
	return tuple.Tuple{
		record.Key,
		record.IsDelete,
		record.IsCreate,
		record.Lease,
		record.CreateRevision,
		record.PrevRevision,
		record.ValueSize,
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
		ValueSize:      t[6].(int64),
		Value:          t[7].([]byte),
	}
}
