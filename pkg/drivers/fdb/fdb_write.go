package fdb

import (
	"bytes"
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

func (f *FDB) Create(_ context.Context, key string, value []byte, lease int64) (int64, error) {
	// Use a UUID to avoid duplicate writes in case of transaction retries.
	// https://apple.github.io/foundationdb/automatic-idempotency.html
	lastWriteUUID := createUUID()
	res, err := transact(f.db, nil, func(tr fdb.Transaction) (*writeResult, error) {
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
			if lastRecord.Value.IsCreate {
				if bytes.Equal(lastWriteUUID[:], lastRecord.Value.WriteUUID[:]) {
					logrus.Errorf("Create succeeded in the previous tr attempt '%s', rev=%+v", key, lastRecord.Key.Rev)
					return newModificationResultRev(tr.GetReadVersion(), nil, true), err
				} else {
					return newModificationResultRev(zeroFuture, nil, false), server.ErrKeyExists
				}
			} else if !lastRecord.Value.IsDelete {
				logrus.Errorf("The key '%s' already exists, prevRev=%+v", key, lastRecord.Key.Rev)
				return newModificationResultRev(zeroFuture, nil, false), server.ErrKeyExists
			}
			createRecord.PrevRevision = lastRecord.Key.Rev
		}

		keyFuture, uuid, err := f.append(&tr, createRecord)
		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}
		lastWriteUUID = uuid
		return newModificationResultKey(keyFuture, nil, true), nil
	})

	if err != nil {
		return 0, err
	}

	rev, _, _, err := res.getResult()
	return rev, err
}

func (f *FDB) Update(_ context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	lastWriteUUID := createUUID()
	res, err := transact(f.db, nil, func(tr fdb.Transaction) (*writeResult, error) {
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
			} else if bytes.Equal(lastWriteUUID[:], lastRecord.Value.WriteUUID[:]) {
				logrus.Errorf("Update succeeded in the previous tr attempt '%s', rev=%+v", key, lastRecord.Key.Rev)
				return newModificationResultRev(tr.GetReadVersion(), &RevRecord{Rev: lastRecord.Key.Rev, Record: record}, true), nil
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

		keyFuture, uuid, err := f.append(&tr, updateRecord)
		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}
		lastWriteUUID = uuid
		return newModificationResultKey(keyFuture, &RevRecord{Record: updateRecord}, true), nil
	})
	if err != nil {
		return 0, nil, false, err
	}
	return res.getResult()
}

func (f *FDB) Delete(_ context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	res, err := transact(f.db, nil, func(tr fdb.Transaction) (*writeResult, error) {
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

		keyFuture, _, err := f.append(&tr, deleteRecord)
		if err != nil {
			return newModificationResultRev(zeroFuture, nil, false), err
		}
		return newModificationResultKey(keyFuture, &RevRecord{Record: deleteRecord}, true), nil
	})
	if err != nil {
		return 0, nil, false, err
	}
	return res.getResult()
}

func (f *FDB) append(tr *fdb.Transaction, record *Record) (fdb.FutureKey, tuple.UUID, error) {
	uuid := createUUID()
	newRev := tuple.IncompleteVersionstamp(0)
	if err := f.byRevision.Write(tr, newRev, record); err != nil {
		return nil, uuid, err
	}

	byKeyRevValue := &ByKeyAndRevisionValue{
		IsDelete:       record.IsDelete,
		IsCreate:       record.IsCreate,
		CreateRevision: record.CreateRevision,
		WriteUUID:      uuid,
	}
	if err := f.byKeyAndRevision.Write(tr, &KeyAndRevision{Key: record.Key, Rev: newRev}, byKeyRevValue); err != nil {
		return nil, uuid, err
	}

	if err := f.watch.Write(tr); err != nil {
		return nil, uuid, err
	}

	return tr.GetVersionstamp(), uuid, nil
}
