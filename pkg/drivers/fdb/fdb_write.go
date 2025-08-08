package fdb

import (
	"context"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

type writeResult struct {
	keyFuture    fdb.FutureKey
	revFuture    fdb.FutureInt64
	isKey        bool
	revRecord    *RevRecord
	writeSuccess bool
}

func newModificationResultKey(revFuture fdb.FutureKey, revRecord *RevRecord, writeSuccess bool) *writeResult {
	return &writeResult{
		keyFuture:    revFuture,
		isKey:        true,
		revRecord:    revRecord,
		writeSuccess: writeSuccess,
	}
}

func newModificationResultRev(rev fdb.FutureInt64, revRecord *RevRecord, writeSuccess bool) *writeResult {
	return &writeResult{
		revFuture:    rev,
		isKey:        false,
		revRecord:    revRecord,
		writeSuccess: writeSuccess,
	}
}

func (r *writeResult) getResult() (int64, *server.KeyValue, bool, error) {
	var revRes int64
	if r.isKey {
		revisionKey, err := r.keyFuture.Get()
		if err != nil {
			return 0, nil, r.writeSuccess, err
		}
		revRes = versionstampBytesToInt64(revisionKey)
	} else if rev, err := r.revFuture.Get(); err != nil {
		return 0, nil, false, err
	} else {
		revRes = rev
	}

	var kv *server.KeyValue = nil
	if r.revRecord != nil {
		kv = revRecordToEvent(r.revRecord).KV
		if r.isKey {
			kv.ModRevision = revRes
		}
	}

	return revRes, kv, r.writeSuccess, nil
}

func (f *FDB) Create(_ context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		lastRecord, err := f.getLast(&tr, key)

		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}

		createRecord := &Record{
			Key:            key,
			IsCreate:       true,
			IsDelete:       false,
			Lease:          lease,
			Value:          value,
			CreateRevision: dummyVersionstamp,
			PrevRevision:   dummyVersionstamp,
		}
		if lastRecord != nil {
			if !lastRecord.Value.IsDelete {
				logrus.Tracef("ERR_KEY_EXISTS %s, prevRev=%+v", key, lastRecord.Key.Rev)
				return newModificationResultRev(zeroFuture, nil, false), server.ErrKeyExists
			}
			createRecord.PrevRevision = lastRecord.Key.Rev
		}

		keyFuture, err := f.append(&tr, createRecord)
		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}
		return newModificationResultKey(keyFuture, nil, true), nil
	})

	if err != nil {
		return 0, err
	}

	castedRes := res.(*writeResult)
	rev, _, _, err := castedRes.getResult()
	return rev, err
}

func (f *FDB) Update(_ context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		lastRecord, err := f.getLast(&tr, key)
		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}

		if lastRecord == nil || lastRecord.Value.IsDelete {
			return newModificationResultRev(zeroFuture, nil, false), nil
		}

		if versionstampToInt64(lastRecord.Key.Rev) != revision {
			if record, err := f.byRevision.Get(&tr, lastRecord.Key.Rev); err != nil {
				return newModificationResultRev(zeroFuture, nil, false), err
			} else {
				return newModificationResultRev(tr.GetReadVersion(), &RevRecord{Rev: lastRecord.Key.Rev, Record: record}, false), err
			}
		}

		updateRecord := &Record{
			Key:            key,
			IsCreate:       false,
			IsDelete:       false,
			Lease:          lease,
			Value:          value,
			CreateRevision: lastRecord.GetCreateRevision(),
			PrevRevision:   lastRecord.Key.Rev,
		}

		keyFuture, err := f.append(&tr, updateRecord)
		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}
		return newModificationResultKey(keyFuture, &RevRecord{Record: updateRecord}, true), nil
	})
	if err != nil {
		return 0, nil, false, err
	}
	return res.(*writeResult).getResult()
}

func (f *FDB) Delete(_ context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	res, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		lastRecord, err := f.getLast(&tr, key)
		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}

		if lastRecord == nil {
			return newModificationResultRev(tr.GetReadVersion(), nil, true), nil
		}

		if lastRecord.Value.IsDelete {
			if record, err := f.byRevision.Get(&tr, lastRecord.Key.Rev); err != nil {
				return newModificationResultRev(zeroFuture, nil, false), err
			} else {
				return newModificationResultRev(tr.GetReadVersion(), &RevRecord{Rev: lastRecord.Key.Rev, Record: record}, true), nil
			}

		}

		if revision != 0 && versionstampToInt64(lastRecord.Key.Rev) != revision {
			if record, err := f.byRevision.Get(&tr, lastRecord.Key.Rev); err != nil {
				return newModificationResultRev(zeroFuture, nil, false), err
			} else {
				return newModificationResultRev(tr.GetReadVersion(), &RevRecord{Rev: lastRecord.Key.Rev, Record: record}, false), nil
			}
		}

		deleteRecord := &Record{
			Key:            key,
			IsCreate:       false,
			IsDelete:       true,
			Lease:          0,
			Value:          nil,
			CreateRevision: lastRecord.GetCreateRevision(),
			PrevRevision:   lastRecord.Key.Rev,
		}

		keyFuture, err := f.append(&tr, deleteRecord)
		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}
		return newModificationResultKey(keyFuture, &RevRecord{Record: deleteRecord}, true), nil
	})
	if err != nil {
		return 0, nil, false, err
	}
	return res.(*writeResult).getResult()
}

func (f *FDB) append(tr *fdb.Transaction, record *Record) (retRev fdb.FutureKey, retErr error) {
	newRev := tuple.IncompleteVersionstamp(0)
	if err := f.byRevision.Write(tr, newRev, record); err != nil {
		return nil, err
	}

	byKeyRevValue := &ByKeyAndRevisionValue{IsDelete: record.IsDelete, IsCreate: record.IsCreate, CreateRevision: record.CreateRevision}
	if err := f.byKeyAndRevision.Write(tr, &KeyAndRevision{Key: record.Key, Rev: newRev}, byKeyRevValue); err != nil {
		return nil, err
	}

	if err := f.watch.Write(tr); err != nil {
		return nil, err
	}

	return tr.GetVersionstamp(), nil
}
