package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

func (f *FDB) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	defer func() {
		logrus.Tracef("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	rev, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, prevEvent, err := f.get(&tr, key, "", 1, 0, true)
		if err != nil {
			return int64(0), err
		}
		createEvent := &server.Event{
			Create: true,
			KV: &server.KeyValue{
				Key:   key,
				Value: value,
				Lease: lease,
			},
			PrevKV: &server.KeyValue{
				ModRevision: rev,
			},
		}
		if prevEvent != nil {
			if !prevEvent.Delete {
				logrus.Tracef("ERR_KEY_EXISTS %s, prevEvent=%+v", key, prevEvent)
				return int64(0), server.ErrKeyExists
			}
			createEvent.PrevKV = prevEvent.KV
		}

		return f.append(&tr, createEvent)
	})

	if err != nil {
		select {
		case f.triggerWatch <- rev.(int64):
		default:
		}
	}

	return rev.(int64), err
}

func (f *FDB) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	defer func() {
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, kvRev, updateRet, errRet)
	}()

	type Result struct {
		revRet    int64
		kvRet     *server.KeyValue
		updateRet bool
		errRet    error
	}

	res, _ := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, event, err := f.get(&tr, key, "", 1, 0, false)
		if err != nil {
			return &Result{0, nil, false, err}, nil
		}

		if event == nil {
			return &Result{0, nil, false, nil}, nil
		}

		if event.KV.ModRevision != revision {
			return &Result{rev, event.KV, false, nil}, nil
		}

		updateEvent := &server.Event{
			KV: &server.KeyValue{
				Key:            key,
				CreateRevision: event.KV.CreateRevision,
				Value:          value,
				Lease:          lease,
			},
			PrevKV: event.KV,
		}

		rev, err = f.append(&tr, updateEvent)
		if err != nil {
			return &Result{0, nil, false, err}, nil
		}

		updateEvent.KV.ModRevision = rev
		return &Result{rev, updateEvent.KV, true, err}, nil
	})
	castedRes := res.(*Result)

	if castedRes.errRet != nil {
		select {
		case f.triggerWatch <- castedRes.revRet:
		default:
		}
	}

	return castedRes.revRet, castedRes.kvRet, castedRes.updateRet, castedRes.errRet
}

func (f *FDB) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	defer func() {
		logrus.Tracef("DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v", key, revision, revRet, kvRet != nil, deletedRet, errRet)
	}()

	type Result struct {
		revRet     int64
		kvRet      *server.KeyValue
		deletedRet bool
		errRet     error
	}

	res, _ := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, event, err := f.get(&tr, key, "", 1, 0, true)
		if err != nil {
			return &Result{0, nil, false, err}, nil
		}

		if event == nil {
			return &Result{rev, nil, true, nil}, nil
		}

		if event.Delete {
			return &Result{rev, event.KV, true, nil}, nil
		}

		if revision != 0 && event.KV.ModRevision != revision {
			return &Result{rev, event.KV, false, nil}, nil
		}

		deleteEvent := &server.Event{
			Delete: true,
			KV:     event.KV,
			PrevKV: event.KV,
		}

		rev, err = f.append(&tr, deleteEvent)
		if err != nil {
			return &Result{0, nil, false, err}, nil
		}
		deleteEvent.KV.ModRevision = rev
		return &Result{rev, event.KV, true, err}, nil
	})
	castedRes := res.(*Result)

	if castedRes.errRet != nil {
		select {
		case f.triggerWatch <- castedRes.revRet:
		default:
		}
	}

	return castedRes.revRet, castedRes.kvRet, castedRes.deletedRet, castedRes.errRet
}

func (f *FDB) append(tr *fdb.Transaction, event *server.Event) (retRev int64, retErr error) {
	record := eventToTuple(event).Pack()
	if err := incrKey(*tr, f.currentRevision); err != nil {
		return 0, err
	}

	newRev, err := f.getCurrentRevision(*tr)
	if err != nil {
		return 0, err
	}

	tr.Set(f.byRevision.Pack(tuple.Tuple{newRev}), record)
	tr.Set(f.byKeyAndRevision.Pack(tuple.Tuple{event.KV.Key, newRev}), record)
	return newRev, err
}
