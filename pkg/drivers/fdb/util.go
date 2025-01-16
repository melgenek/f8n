package fdb

import (
	"bytes"
	"encoding/binary"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
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

func incrKey(tr fdb.Transaction, k fdb.KeyConvertible) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int64(1))
	if err != nil {
		return err
	}
	one := buf.Bytes()
	tr.Add(k, one)
	return nil
}

func getKey(tr fdb.Transaction, k fdb.KeyConvertible) (int64, error) {
	byteVal, err := tr.Get(k).Get()
	if err != nil {
		return 0, err
	}
	if byteVal == nil {
		return 0, nil
	}
	var numVal int64
	err = binary.Read(bytes.NewReader(byteVal), binary.LittleEndian, &numVal)
	if err != nil {
		return 0, err
	} else {
		return numVal, nil
	}
}
