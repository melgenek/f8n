package fdb

import (
	"encoding/binary"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
)

func revRecordToEvent(revRecord *RevRecord) *server.Event {
	event := &server.Event{
		Create: revRecord.Record.IsCreate,
		Delete: revRecord.Record.IsDelete,
		KV: &server.KeyValue{
			Key:            revRecord.Record.Key,
			CreateRevision: versionstampToInt64(revRecord.GetCreateRevision()),
			ModRevision:    versionstampToInt64(revRecord.Rev),
			Lease:          revRecord.Record.Lease,
			Value:          revRecord.Record.Value,
		},
	}
	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	}
	if revRecord.Record.PrevRevision != stubVersionstamp {
		event.PrevKV = &server.KeyValue{
			ModRevision: versionstampToInt64(revRecord.Record.PrevRevision),
		}
	}

	return event
}

func versionstampBytesToInt64(bytes []byte) int64 {
	return int64(binary.BigEndian.Uint64(bytes[:8]))
}

func versionstampToInt64(versionstamp tuple.Versionstamp) int64 {
	return versionstampBytesToInt64(versionstamp.TransactionVersion[:])
}

func int64ToVersionstamp(minRevision int64) tuple.Versionstamp {
	beginVersionstamp := tuple.IncompleteVersionstamp(0xFFFF)
	binary.BigEndian.PutUint64(beginVersionstamp.TransactionVersion[:], uint64(minRevision))
	return beginVersionstamp
}

var stubVersionstamp = createStubVersionstamp()

func createStubVersionstamp() tuple.Versionstamp {
	versionstamp := tuple.IncompleteVersionstamp(10)
	for i := 0; i < 10; i++ {
		versionstamp.TransactionVersion[i] = byte(i)
	}
	return versionstamp
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
