package fdb

import (
	"crypto/rand"
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
			CreateRevision: revRecord.GetCreateRevision(),
			ModRevision:    revRecord.Rev,
			Lease:          revRecord.Record.Lease,
			Value:          revRecord.Record.Value,
		},
	}
	if revRecord.Record.PrevRevision != zeroRevision {
		event.PrevKV = &server.KeyValue{
			ModRevision: revRecord.Record.PrevRevision,
		}
	}
	if APITest && event.Delete {
		event.KV.Value = nil
	}

	return event
}

func createUUID() tuple.UUID {
	var b [16]byte
	rand.Read(b[:])
	return b
}

func WaitForFutureNil(f fdb.FutureNil) <-chan error {
	res := make(chan error, 1)
	go func() {
		err := f.Get()
		res <- err
	}()
	return res
}
