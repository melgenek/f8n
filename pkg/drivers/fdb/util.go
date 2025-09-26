package fdb

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"math"
)

func revRecordToEvent(revRecord *RevRecord) *server.Event {
	event := &server.Event{
		Create: revRecord.Record.IsCreate,
		Delete: revRecord.Record.IsDelete,
		KV: &server.KeyValue{
			Key:            revRecord.Record.Key,
			CreateRevision: VersionstampToInt64(revRecord.GetCreateRevision()),
			ModRevision:    VersionstampToInt64(revRecord.Rev),
			Lease:          revRecord.Record.Lease,
			Value:          revRecord.Record.Value,
		},
	}
	if revRecord.Record.PrevRevision != dummyVersionstamp {
		event.PrevKV = &server.KeyValue{
			ModRevision: VersionstampToInt64(revRecord.Record.PrevRevision),
		}
	}
	if APITest && event.Delete {
		event.KV.Value = nil
	}

	return event
}

func versionstampBytesToInt64(bytes []byte) int64 {
	return int64(binary.BigEndian.Uint64(bytes[:8]))
}

func VersionstampToInt64(versionstamp tuple.Versionstamp) int64 {
	idInCommit := binary.BigEndian.Uint16(versionstamp.TransactionVersion[8:])
	if idInCommit != 0 && idInCommit != math.MaxUint16 {
		panic(fmt.Sprintf("there was more than one transaction in this commit. Versionstamp: %s", versionstamp.String()))
	}
	return versionstampBytesToInt64(versionstamp.TransactionVersion[:])
}

func int64ToVersionstamp(rev int64) tuple.Versionstamp {
	beginVersionstamp := tuple.Versionstamp{}
	binary.BigEndian.PutUint64(beginVersionstamp.TransactionVersion[:], uint64(rev))
	return beginVersionstamp
}

var dummyVersionstamp = createDummyVersionstamp()

func createDummyVersionstamp() tuple.Versionstamp {
	versionstamp := tuple.IncompleteVersionstamp(123)
	// VersionstampToInt64 should return 0 for dummy versionstamp
	for i := 0; i < 8; i++ {
		versionstamp.TransactionVersion[i] = 0
	}
	return versionstamp
}

func createUUID() tuple.UUID {
	var b [16]byte
	rand.Read(b[:])
	return b
}

var zeroFuture fdb.FutureInt64 = ConstInt64Future{0}

type ConstInt64Future struct {
	value int64
}

func (f ConstInt64Future) Get() (int64, error) { return f.value, nil }
func (f ConstInt64Future) MustGet() int64      { return f.value }
func (f ConstInt64Future) BlockUntilReady()    {}
func (f ConstInt64Future) IsReady() bool       { return true }
func (f ConstInt64Future) Cancel()             {}

type ConstKeyFuture struct {
	versionstamp tuple.Versionstamp
}

func (c ConstKeyFuture) Get() (fdb.Key, error) { return c.versionstamp.Bytes(), nil }
func (c ConstKeyFuture) MustGet() fdb.Key      { return c.versionstamp.Bytes() }
func (c ConstKeyFuture) BlockUntilReady()      {}
func (c ConstKeyFuture) IsReady() bool         { return true }
func (c ConstKeyFuture) Cancel()               {}

func WaitForFutureNil(f fdb.FutureNil) <-chan error {
	res := make(chan error, 1)
	go func() {
		err := f.Get()
		res <- err
	}()
	return res
}
