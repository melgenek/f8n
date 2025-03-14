package fdb

import (
	"encoding/binary"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
)

func tupleToEvent(versionstamp int64, t tuple.Tuple) *server.Event {
	event := &server.Event{
		Create: t[0].(bool),
		Delete: t[1].(bool),
		KV: &server.KeyValue{
			ModRevision:    versionstamp,
			Key:            t[2].(string),
			CreateRevision: t[3].(int64),
			Lease:          t[4].(int64),
			Value:          t[5].([]byte),
		},
		PrevKV: &server.KeyValue{
			ModRevision: t[6].(int64),
			Value:       t[7].([]byte),
		},
	}
	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	}
	return event
}

func eventToTuple(event *server.Event) tuple.Tuple {
	if event.KV == nil {
		event.KV = &server.KeyValue{}
	}
	if event.PrevKV == nil {
		event.PrevKV = &server.KeyValue{}
	}
	return tuple.Tuple{
		event.Create,
		event.Delete,
		event.KV.Key,
		event.KV.CreateRevision,
		event.KV.Lease,
		event.KV.Value,
		event.PrevKV.ModRevision,
		event.PrevKV.Value,
	}
}

func (f *FDB) adjustRevision(rev *int64) {
	if *rev != 0 {
		return
	}

	if newRev, err := f.CurrentRevision(nil); err == nil {
		*rev = newRev
	}
}

func versionstampBytesToInt64(bytes []byte) int64 {
	return int64(binary.BigEndian.Uint64(bytes[:8]))
}

func versionstampToInt64(versionstamp *tuple.Versionstamp) int64 {
	return versionstampBytesToInt64(versionstamp.TransactionVersion[:])
}

func int64ToVersionstamp(minRevision int64) tuple.Versionstamp {
	beginVersionstamp := tuple.IncompleteVersionstamp(0xFFFF)
	binary.BigEndian.PutUint64(beginVersionstamp.TransactionVersion[:], uint64(minRevision))
	return beginVersionstamp
}
