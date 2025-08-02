package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

func (f *FDB) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	type Result struct {
		revRet   interface{}
		isFuture bool
	}

	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		_, prevRevRecord, err := f.get(&tr, key, "", 1, 0, true)
		if err != nil {
			return Result{int64(0), false}, err
		}

		createRecord := &Record{
			Key:            key,
			IsCreate:       true,
			IsDelete:       false,
			Lease:          lease,
			Value:          value,
			CreateRevision: stubVersionstamp,
			PrevRevision:   stubVersionstamp,
		}
		if prevRevRecord != nil {
			if !prevRevRecord.Record.IsDelete {
				logrus.Tracef("ERR_KEY_EXISTS %s, prevRevRecord=%+v", key, prevRevRecord)
				return Result{int64(0), false}, server.ErrKeyExists
			}
			createRecord.PrevRevision = prevRevRecord.Rev
		}

		revF, err := f.append(&tr, createRecord)
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
	} else {
		rev = castedRes.revRet.(int64)
	}

	return rev, nil
}

func (f *FDB) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	type Result struct {
		revRet    interface{}
		isFuture  bool
		kvRet     *server.KeyValue
		updateRet bool
	}

	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, revRecord, err := f.get(&tr, key, "", 1, 0, false)
		if err != nil {
			return &Result{int64(0), false, nil, false}, err
		}

		if revRecord == nil {
			return &Result{int64(0), false, nil, false}, nil
		}

		if versionstampToInt64(revRecord.Rev) != revision {
			return &Result{rev, false, revRecordToEvent(revRecord).KV, false}, nil
		}

		updateRecord := &Record{
			Key:            key,
			IsCreate:       false,
			IsDelete:       false,
			Lease:          lease,
			Value:          value,
			CreateRevision: revRecord.GetCreateRevision(),
			PrevRevision:   revRecord.Rev,
		}

		revF, err := f.append(&tr, updateRecord)
		if err != nil {
			return &Result{int64(0), false, nil, false}, err
		}
		return &Result{revF, true, revRecordToEvent(&RevRecord{Record: updateRecord}).KV, true}, nil
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
	} else {
		rev = castedRes.revRet.(int64)
	}

	return rev, castedRes.kvRet, castedRes.updateRet, nil
}

func (f *FDB) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	type Result struct {
		revRet     interface{}
		isFuture   bool
		kvRet      *server.KeyValue
		deletedRet bool
	}

	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, revRecord, err := f.get(&tr, key, "", 1, 0, true)
		if err != nil {
			return &Result{int64(0), false, nil, false}, err
		}

		if revRecord == nil {
			return &Result{rev, false, nil, true}, nil
		}

		if revRecord.Record.IsDelete {
			return &Result{rev, false, revRecordToEvent(revRecord).KV, true}, nil
		}

		if revision != 0 && versionstampToInt64(revRecord.Rev) != revision {
			return &Result{rev, false, revRecordToEvent(revRecord).KV, false}, nil
		}

		deleteRecord := &Record{
			Key:            key,
			IsCreate:       false,
			IsDelete:       true,
			Lease:          0,
			Value:          nil,
			CreateRevision: revRecord.GetCreateRevision(),
			PrevRevision:   revRecord.Rev,
		}

		revF, err := f.append(&tr, deleteRecord)
		if err != nil {
			return &Result{int64(0), false, nil, false}, err
		}
		return &Result{revF, true, revRecordToEvent(&RevRecord{Record: deleteRecord}).KV, true}, nil
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
	} else {
		rev = castedRes.revRet.(int64)
	}

	return rev, castedRes.kvRet, castedRes.deletedRet, nil
}

func (f *FDB) append(tr *fdb.Transaction, record *Record) (retRev fdb.FutureKey, retErr error) {
	newRev := tuple.IncompleteVersionstamp(0)
	if err := f.byRevision.Write(tr, newRev, record.Key); err != nil {
		return nil, err
	}

	if err := f.byKeyAndRevision.Write(tr, &KeyAndRevision{Key: record.Key, Rev: newRev}, record); err != nil {
		return nil, err
	}

	if err := f.watch.Write(tr); err != nil {
		return nil, err
	}

	return tr.GetVersionstamp(), nil
}
