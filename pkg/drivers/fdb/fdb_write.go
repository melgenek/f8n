package fdb

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

const maxRecordSize = 2 * 1024 * 1024 // 2 MiB

type writeResult struct {
	rev          int64
	kv           *server.KeyValue
	writeSuccess bool
}

var noWriteResult = writeResult{zeroRevision, nil, false}

func (f *FDB) Create(_ context.Context, key string, value []byte, lease int64) (int64, error) {
	if len(value) > maxRecordSize {
		return 0, rpctypes.ErrRequestTooLarge
	}

	// Use a UUID to avoid duplicate writes in case of transaction retries.
	// https://apple.github.io/foundationdb/automatic-idempotency.html
	lastWriteUUID := createUUID()
	op := func(tr *fdb.Transaction, getNewRev getRev, getLatestRev getRev) (writeResult, error) {
		lastRecord, err := f.getLast(tr, key, toReadTr)
		if err != nil {
			return noWriteResult, err
		}

		createRecord := &Record{
			Key:            key,
			IsCreate:       true,
			IsDelete:       false,
			Lease:          lease,
			ValueSize:      int64(len(value)),
			Value:          value,
			CreateRevision: zeroRevision,
			PrevRevision:   zeroRevision,
		}
		if lastRecord != nil {
			if lastRecord.Value.IsCreate && bytes.Equal(lastWriteUUID[:], lastRecord.Value.WriteUUID[:]) {
				logrus.Tracef("Create succeeded in the previous tr attempt '%s', rev=%+v", key, lastRecord.Key.Rev)
				return writeResult{lastRecord.Key.Rev, nil, true}, err
			} else if !lastRecord.Value.IsDelete {
				logrus.Tracef("The key '%s' already exists, prevRev=%+v", key, lastRecord.Key.Rev)
				return noWriteResult, server.ErrKeyExists
			}
			createRecord.PrevRevision = lastRecord.Key.Rev
		}

		newRev := getNewRev()
		createRecord.CreateRevision = newRev
		uuid, err := f.append(tr, newRev, createRecord)
		if err != nil {
			return noWriteResult, err
		}
		lastWriteUUID = uuid
		return writeResult{newRev, nil, true}, nil
	}
	res, err := f.writeTrMgr.Exec(op, int64(len(key)+len(value)))
	if err != nil {
		return 0, err
	} else {
		return res.rev, nil
	}
}

func (f *FDB) Update(_ context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	if len(value) > maxRecordSize {
		return 0, nil, false, rpctypes.ErrRequestTooLarge
	}

	lastWriteUUID := createUUID()
	op := func(tr *fdb.Transaction, getNewRev getRev, getLatestRev getRev) (writeResult, error) {
		lastRecord, err := f.getLast(tr, key, toReadTr)
		if err != nil {
			return noWriteResult, err
		}

		if lastRecord == nil || lastRecord.Value.IsDelete {
			return writeResult{getLatestRev(), nil, false}, nil
		}

		if lastRecord.Key.Rev != revision {
			if record, err := f.byRevision.Get(tr, lastRecord.Key.Rev); err != nil {
				return noWriteResult, err
			} else if bytes.Equal(lastWriteUUID[:], lastRecord.Value.WriteUUID[:]) {
				logrus.Tracef("Update succeeded in the previous tr attempt '%s', latestRev=%+v", key, lastRecord.Key.Rev)
				return writeResult{
					lastRecord.Key.Rev,
					revRecordToEvent(&RevRecord{Rev: lastRecord.Key.Rev, Record: record}).KV,
					true,
				}, nil
			} else {
				return writeResult{
					getLatestRev(),
					revRecordToEvent(&RevRecord{Rev: lastRecord.Key.Rev, Record: record}).KV,
					false,
				}, nil
			}
		}

		updateRecord := &Record{
			Key:            key,
			IsCreate:       false,
			IsDelete:       false,
			Lease:          lease,
			ValueSize:      int64(len(value)),
			Value:          value,
			CreateRevision: lastRecord.Value.CreateRevision,
			PrevRevision:   lastRecord.Key.Rev,
		}

		newRev := getNewRev()
		uuid, err := f.append(tr, newRev, updateRecord)
		if err != nil {
			return noWriteResult, err
		}
		lastWriteUUID = uuid
		return writeResult{
			newRev,
			revRecordToEvent(&RevRecord{Rev: newRev, Record: updateRecord}).KV,
			true,
		}, nil
	}
	res, err := f.writeTrMgr.Exec(op, int64(len(key)+len(value)))
	if err != nil {
		return 0, nil, false, err
	} else {
		return res.rev, res.kv, res.writeSuccess, nil
	}
}

func (f *FDB) Delete(_ context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	lastWriteUUID := createUUID()
	op := func(tr *fdb.Transaction, getNewRev getRev, getLatestRev getRev) (writeResult, error) {
		lastRecord, err := f.getLast(tr, key, toReadTr)
		if err != nil {
			return noWriteResult, err
		}

		if lastRecord == nil {
			return writeResult{getLatestRev(), nil, false}, nil
		}

		record, err := f.byRevision.Get(tr, lastRecord.Key.Rev)
		if err != nil {
			return noWriteResult, err
		}

		if lastRecord.Value.IsDelete {
			if bytes.Equal(lastWriteUUID[:], lastRecord.Value.WriteUUID[:]) {
				logrus.Tracef("Delete succeeded in the previous tr attempt '%s', latestRev=%+v", key, lastRecord.Key.Rev)
				return writeResult{
					lastRecord.Key.Rev,
					revRecordToEvent(&RevRecord{Rev: lastRecord.Key.Rev, Record: record}).KV,
					true,
				}, nil
			} else {
				return writeResult{getLatestRev(), nil, false}, nil
			}
		}

		if revision != 0 && lastRecord.Key.Rev != revision {
			return writeResult{
				getLatestRev(),
				revRecordToEvent(&RevRecord{Rev: lastRecord.Key.Rev, Record: record}).KV,
				false,
			}, nil
		}

		deleteRecord := &Record{
			Key:            key,
			IsCreate:       false,
			IsDelete:       true,
			Lease:          record.Lease,
			ValueSize:      record.ValueSize,
			Value:          record.Value,
			CreateRevision: lastRecord.Value.CreateRevision,
			PrevRevision:   lastRecord.Key.Rev,
		}

		newRev := getNewRev()
		uuid, err := f.append(tr, newRev, deleteRecord)
		if err != nil {
			return noWriteResult, err
		}
		lastWriteUUID = uuid
		return writeResult{
			newRev,
			revRecordToEvent(&RevRecord{Rev: newRev, Record: deleteRecord}).KV,
			true,
		}, nil
	}
	res, err := f.writeTrMgr.Exec(op, int64(len(key)))
	if err != nil {
		return 0, nil, false, err
	} else {
		return res.rev, res.kv, res.writeSuccess, nil
	}
}

type getRev = func() Revision
type operation = func(tr *fdb.Transaction, getNewRev getRev, getLatestRev getRev) (writeResult, error)
type transactionResult struct {
	committed bool
	err       error
}

type writeTransactionManager struct {
	tr  *fdb.Transaction
	rev atomic.Int64

	writeStart   chan int64
	writePermits chan interface{}
	writeEnd     chan interface{}
	trEnd        chan transactionResult
}

func (f *FDB) newWriteTransactionManager() *writeTransactionManager {
	mgr := &writeTransactionManager{
		writeStart:   make(chan int64, 1),
		writePermits: make(chan interface{}, 1),
		writeEnd:     make(chan interface{}, 1),
		trEnd:        make(chan transactionResult, 1),
	}
	go func() {
		for {
			size := <-mgr.writeStart
			inFlightRequests := 1
			inFlightRequestsSize := size

			writeTimeout := time.After(WriteBatchDuration)

			iteration := 0
			_, err := transact("writer", f.db, 0, func(tr fdb.Transaction) (int64, error) {
				iteration++

				rev, err := f.rev.Get(&tr, toReadTr)
				if err != nil {
					return 0, err
				}

				mgr.rev.Store(rev)
				mgr.tr = &tr

				if iteration > 1 {
					// retrying
					for i := 0; i < inFlightRequests; i++ {
						mgr.trEnd <- transactionResult{committed: false, err: nil}
					}
					for i := 0; i < inFlightRequests; i++ {
						mgr.writePermits <- nil
					}
				} else {
					mgr.writePermits <- nil
				loop:
					for {
						if inFlightRequestsSize > maxRecordSize {
							break loop
						}
						select {
						case size = <-mgr.writeStart:
							inFlightRequests++
							inFlightRequestsSize += size
							mgr.writePermits <- nil
						case <-writeTimeout:
							break loop
						}
					}
				}

				for i := 0; i < inFlightRequests; i++ {
					<-mgr.writeEnd
				}

				f.rev.Set(&tr, mgr.rev.Load())
				return 0, nil
			})

			for i := 0; i < inFlightRequests; i++ {
				mgr.trEnd <- transactionResult{committed: true, err: err}
			}
		}
	}()
	return mgr
}

func (mgr *writeTransactionManager) Exec(op operation, size int64) (writeResult, error) {
	mgr.writeStart <- size
	var err error
	var res writeResult
	for {
		<-mgr.writePermits

		getNewRev := func() Revision {
			return mgr.rev.Add(1)
		}
		getLatestRev := func() Revision {
			return mgr.rev.Load()
		}
		res, err = op(mgr.tr, getNewRev, getLatestRev)
		mgr.writeEnd <- nil

		trRes := <-mgr.trEnd
		if trRes.err != nil {
			return res, trRes.err
		} else if trRes.committed {
			break
		}
	}

	return res, err
}

func (f *FDB) append(tr *fdb.Transaction, rev Revision, record *Record) (tuple.UUID, error) {
	uuid := createUUID()
	record.WriteUUID = uuid

	if err := f.byRevision.Write(tr, rev, record); err != nil {
		return uuid, err
	}

	if err := f.byKeyAndRevision.Write(tr, &KeyAndRevision{Key: record.Key, Rev: rev}, record); err != nil {
		return uuid, err
	}

	if err := f.watch.Write(tr); err != nil {
		return uuid, err
	}

	return uuid, nil
}
