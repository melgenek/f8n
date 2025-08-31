package fdb

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"math"
)

var CorrectnessTesting = false

type WalSubspace struct {
	subspace subspace.Subspace
}

func CreateWalSubspace(directory directory.DirectorySubspace) *WalSubspace {
	if CorrectnessTesting {
		return &WalSubspace{
			subspace: directory.Sub("wal"),
		}
	} else {
		return &WalSubspace{}
	}
}

func (s *WalSubspace) GetSubspace() subspace.Subspace {
	return s.subspace
}

func (s *WalSubspace) Write(tr *fdb.Transaction, rev tuple.Versionstamp, record *Record) error {
	if !CorrectnessTesting {
		return nil
	}

	record.ValueSize = int64(len(record.Value))
	packKey, setValue := GetWriteOps(tr, s.subspace)
	if revisionKey, err := packKey(tuple.Tuple{rev}); err != nil {
		return err
	} else {
		setValue(revisionKey, s.recordToTuple(record).Pack())
	}

	return nil
}

func (s *WalSubspace) parseKV(kv fdb.KeyValue) (tuple.Versionstamp, *Record, error) {
	k, err := s.subspace.Unpack(kv.Key)
	if err != nil {
		return dummyVersionstamp, nil, fmt.Errorf("failed to unpack key %v: %w", kv.Key, err)
	}
	versionstamp := k[0].(tuple.Versionstamp)
	if len(k) != 1 {
		panic(fmt.Sprintf("can parse only the first entry for the record. Key: %v", k))
	}
	unpackedTuple, err := tuple.Unpack(kv.Value)
	if err != nil {
		return dummyVersionstamp, nil, fmt.Errorf("failed to unpack value '%v': %w", kv.Value, err)
	}
	record := s.tupleToRecord(unpackedTuple)
	return versionstamp, record, nil
}

func (s *WalSubspace) GetIterator(tr *fdb.Transaction, rev tuple.Versionstamp) (*fdb.RangeIterator, error) {
	selector, err := fdb.PrefixRange(s.subspace.Pack(tuple.Tuple{rev}))
	if err != nil {
		return nil, err
	}
	it := tr.GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()
	return it, nil
}

func (s *WalSubspace) GetFromIterator(it *fdb.RangeIterator) (*tuple.Versionstamp, *Record, error) {
	if !it.Advance() {
		return nil, nil, nil
	}
	kv, err := it.Get()
	if err != nil {
		return nil, nil, err
	}
	rev, record, err := s.parseKV(kv)
	if err != nil {
		return nil, nil, err
	}
	return &rev, record, nil
}

func (s *WalSubspace) recordToTuple(record *Record) tuple.Tuple {
	return tuple.Tuple{
		record.Key,
		record.IsDelete,
		record.IsCreate,
		record.Value,
	}
}

func (s *WalSubspace) tupleToRecord(t tuple.Tuple) *Record {
	return &Record{
		Key:      t[0].(string),
		IsDelete: t[1].(bool),
		IsCreate: t[2].(bool),
		Value:    t[3].([]byte),
	}
}

func (f *FDB) ReadWAL() ([]RevRecord, error) {
	collector := newWalCollector(f)
	//begin, end := f.wal.GetSubspace().FDBRangeKeySelectors()
	begin, end := f.byRevision.GetSubspace().FDBRangeKeySelectors()
	err := processRange(f.db, fdb.SelectorRange{Begin: begin, End: end}, collector)
	return collector.records, err
}

type walCollector struct {
	f *FDB
	// output
	batchEvents []RevRecord
	records     []RevRecord
}

func newWalCollector(f *FDB) *walCollector {
	return &walCollector{
		f:           f,
		batchEvents: make([]RevRecord, 0, 1000),
		records:     make([]RevRecord, 0, 1000),
	}
}

func (c *walCollector) startBatch() {
	c.batchEvents = c.batchEvents[len(c.batchEvents):]
}

func (c *walCollector) next(_ *fdb.Transaction, it *fdb.RangeIterator) (fdb.Key, bool, error) {
	//latestRev, record, err := c.f.wal.GetFromIterator(it)
	rev, record, err := c.f.byRevision.GetFromIterator(it)
	if err != nil {
		return nil, false, err
	}
	if rev == nil {
		return nil, false, nil
	}

	c.batchEvents = append(c.batchEvents, RevRecord{Rev: *rev, Record: record})

	//return c.f.wal.GetSubspace().Pack(tuple.Tuple{*latestRev, math.MaxInt64}), true, nil
	return c.f.byRevision.GetSubspace().Pack(tuple.Tuple{*rev, math.MaxInt64}), true, nil
}

func (c *walCollector) endBatch(*fdb.Transaction, bool) error {
	return nil
}

func (c *walCollector) postBatch() {
	c.records = append(c.records, c.batchEvents...)
}
