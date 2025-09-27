package fdb

import (
	"github.com/melgenek/f8n/pkg/drivers/fdb"
	"go.etcd.io/etcd/tests/v3/robustness/model"
)

type WalDump struct {
	PrevRev  int64
	Rev      int64
	Key      string
	IsCreate bool
	IsDelete bool
	Req      model.EtcdRequest
}

func WALToEtcdRequests() ([]model.EtcdRequest, []WalDump, error) {
	wal, err := fdb.ThisFDB.ReadWAL()
	if err != nil {
		return nil, nil, err
	}
	requests := make([]model.EtcdRequest, 0, len(wal))
	walDumps := make([]WalDump, 0, len(wal))
	for _, walReq := range wal {
		var req model.EtcdRequest
		if walReq.Record.IsCreate {
			req = Create(walReq.Record.Key, walReq.Record.Value)
		} else if walReq.Record.IsDelete {
			req = Delete(walReq.Record.Key, fdb.VersionstampToInt64(walReq.Record.PrevRevision))
		} else {
			req = Update(walReq.Record.Key, walReq.Record.Value, fdb.VersionstampToInt64(walReq.Record.PrevRevision))
		}
		requests = append(requests, req)
		walDumps = append(walDumps, WalDump{
			Rev:      fdb.VersionstampToInt64(walReq.Rev),
			PrevRev:  fdb.VersionstampToInt64(walReq.Record.PrevRevision),
			Key:      walReq.Record.Key,
			IsCreate: walReq.Record.IsCreate,
			IsDelete: walReq.Record.IsDelete,
			Req:      req,
		})
	}

	return requests, walDumps, nil
}

func Create(key string, value []byte) model.EtcdRequest {
	request := model.EtcdRequest{
		Type: model.Txn,
		Txn: &model.TxnRequest{
			Conditions: []model.EtcdCondition{
				{
					Key:              key,
					ExpectedRevision: 0,
				},
			},
			OperationsOnSuccess: []model.EtcdOperation{
				{
					Type: model.PutOperation,
					Put: model.PutOptions{
						Key:   key,
						Value: model.ToValueOrHash(string(value)),
					},
				},
			},
			OperationsOnFailure: []model.EtcdOperation{},
		},
	}
	return request
}

func Update(key string, value []byte, prevRevision int64) model.EtcdRequest {
	request := model.EtcdRequest{
		Type: model.Txn,
		Txn: &model.TxnRequest{
			Conditions: []model.EtcdCondition{
				{
					Key:              key,
					ExpectedRevision: prevRevision,
				},
			},
			OperationsOnSuccess: []model.EtcdOperation{
				{
					Type: model.PutOperation,
					Put: model.PutOptions{
						Key:   key,
						Value: model.ToValueOrHash(string(value)),
					},
				},
			},
			OperationsOnFailure: []model.EtcdOperation{
				{
					Type: model.RangeOperation,
					Range: model.RangeOptions{
						Start: key,
						End:   "",
						Limit: 0,
					},
				},
			},
		},
	}
	return request
}

func Delete(key string, prevRev int64) model.EtcdRequest {
	var conditions []model.EtcdCondition
	var successOps []model.EtcdOperation
	var failureOps []model.EtcdOperation
	if prevRev != 0 {
		conditions = append(conditions, model.EtcdCondition{
			Key:              key,
			ExpectedRevision: prevRev,
		})
		successOps = append(successOps, model.EtcdOperation{
			Type: model.DeleteOperation,
			Delete: model.DeleteOptions{
				Key: key,
			},
		})
		failureOps = append(failureOps, model.EtcdOperation{
			Type: model.RangeOperation,
			Range: model.RangeOptions{
				Start: key,
				End:   "",
				Limit: 0,
			},
		})
	} else {
		successOps = append(successOps, model.EtcdOperation{
			Type: model.RangeOperation,
			Range: model.RangeOptions{
				Start: key,
				End:   "",
				Limit: 0,
			},
		})
		successOps = append(successOps, model.EtcdOperation{
			Type: model.DeleteOperation,
			Delete: model.DeleteOptions{
				Key: key,
			},
		})
	}
	request := model.EtcdRequest{
		Type: model.Txn,
		Txn: &model.TxnRequest{
			Conditions:          conditions,
			OperationsOnSuccess: successOps,
			OperationsOnFailure: failureOps,
		},
	}
	return request
}
