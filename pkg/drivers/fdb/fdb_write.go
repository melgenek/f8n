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
		f.adjustRevision(&revRet)
		logrus.Tracef("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	type Result struct {
		revRet   interface{}
		isFuture bool
	}

	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, prevEvent, err := f.get(&tr, key, "", 1, 0, true)
		if err != nil {
			return Result{int64(0), false}, err
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
				return Result{int64(0), false}, server.ErrKeyExists
			}
			createEvent.PrevKV = prevEvent.KV
		}

		revF, err := f.append(&tr, createEvent)
		if err != nil {
			return &Result{int64(0), false}, err
		}
		return &Result{revF, true}, nil
	})

	if err != nil {
		return 0, err
	}

	castedRes := res.(*Result)

	var rev int64
	if castedRes.isFuture {
		revisionKey, err := castedRes.revRet.(fdb.FutureKey).Get()
		if err != nil {
			return 0, err
		}
		rev = versionstampBytesToInt64(revisionKey)
		select {
		case f.triggerWatch <- rev:
		default:
		}
	} else {
		rev = castedRes.revRet.(int64)
	}

	return rev, nil
}

func (f *FDB) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	defer func() {
		f.adjustRevision(&revRet)
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, kvRev, updateRet, errRet)
	}()

	type Result struct {
		revRet    interface{}
		isFuture  bool
		kvRet     *server.KeyValue
		updateRet bool
	}

	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, event, err := f.get(&tr, key, "", 1, 0, false)
		if err != nil {
			return &Result{int64(0), false, nil, false}, err
		}

		if event == nil {
			return &Result{int64(0), false, nil, false}, nil
		}

		if event.KV.ModRevision != revision {
			return &Result{rev, false, event.KV, false}, nil
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

		revF, err := f.append(&tr, updateEvent)
		if err != nil {
			return &Result{int64(0), false, nil, false}, err
		}
		return &Result{revF, true, updateEvent.KV, true}, nil
	})
	if err != nil {
		return 0, nil, false, err
	}
	castedRes := res.(*Result)

	var rev int64
	if castedRes.isFuture {
		revisionKey, err := castedRes.revRet.(fdb.FutureKey).Get()
		if err != nil {
			return 0, nil, false, err
		}
		rev = versionstampBytesToInt64(revisionKey)
		castedRes.kvRet.ModRevision = rev
		select {
		case f.triggerWatch <- rev:
		default:
		}
	} else {
		rev = castedRes.revRet.(int64)
	}

	return rev, castedRes.kvRet, castedRes.updateRet, nil
}

func (f *FDB) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	defer func() {
		f.adjustRevision(&revRet)
		logrus.Tracef("DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v", key, revision, revRet, kvRet != nil, deletedRet, errRet)
	}()

	type Result struct {
		revRet     interface{}
		isFuture   bool
		kvRet      *server.KeyValue
		deletedRet bool
	}

	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, event, err := f.get(&tr, key, "", 1, 0, true)
		if err != nil {
			return &Result{int64(0), false, nil, false}, err
		}

		if event == nil {
			return &Result{rev, false, nil, true}, nil
		}

		if event.Delete {
			return &Result{rev, false, event.KV, true}, nil
		}

		if revision != 0 && event.KV.ModRevision != revision {
			return &Result{rev, false, event.KV, false}, nil
		}

		deleteEvent := &server.Event{
			Delete: true,
			KV:     event.KV,
			PrevKV: event.KV,
		}

		revF, err := f.append(&tr, deleteEvent)
		if err != nil {
			return &Result{int64(0), false, nil, false}, err
		}
		return &Result{revF, true, event.KV, true}, nil
	})
	if err != nil {
		return 0, nil, false, err
	}

	castedRes := res.(*Result)
	var rev int64
	if castedRes.isFuture {
		revisionKey, err := castedRes.revRet.(fdb.FutureKey).Get()
		if err != nil {
			return 0, nil, false, err
		}
		rev = versionstampBytesToInt64(revisionKey)
		castedRes.kvRet.ModRevision = rev
		select {
		case f.triggerWatch <- rev:
		default:
		}
	} else {
		rev = castedRes.revRet.(int64)
	}

	return rev, castedRes.kvRet, castedRes.deletedRet, nil
}

func (f *FDB) append(tr *fdb.Transaction, event *server.Event) (retRev fdb.FutureKey, retErr error) {
	record := eventToTuple(event).Pack()
	newRev := tuple.IncompleteVersionstamp(0)
	if revisionKey, err := f.byRevision.PackWithVersionstamp(tuple.Tuple{newRev}); err != nil {
		return nil, err
	} else {
		tr.SetVersionstampedKey(revisionKey, record)
	}

	if revisionKey, err := f.byKeyAndRevision.PackWithVersionstamp(tuple.Tuple{event.KV.Key, newRev}); err != nil {
		return nil, err
	} else {
		tr.SetVersionstampedKey(revisionKey, record)
	}

	return tr.GetVersionstamp(), nil
}
