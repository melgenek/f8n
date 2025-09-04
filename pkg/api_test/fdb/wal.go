package fdb

import (
	"fmt"
	"github.com/melgenek/f8n/pkg/drivers/fdb"
	"go.etcd.io/etcd/tests/v3/robustness/model"
	"strconv"
)

type WalDump struct {
	PrevRev     int64
	Rev         int64
	ExpectedRev int64
	Key         string
	IsCreate    bool
	IsDelete    bool
	Req         model.EtcdRequest
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
		if walReq.IsCreate {
			req = Create(walReq.Key, walReq.Value)
		} else if walReq.IsDelete {
			req = Delete(walReq.Key, fdb.VersionstampToInt64(walReq.PrevRevision), walReq.ExpectedRevision)
		} else {
			req = Update(
				walReq.Key,
				walReq.Value,
				fdb.VersionstampToInt64(walReq.PrevRevision),
				fdb.VersionstampToInt64(walReq.Rev),
				walReq.ExpectedRevision,
			)
		}
		requests = append(requests, req)
		walDumps = append(walDumps, WalDump{
			Rev:         fdb.VersionstampToInt64(walReq.Rev),
			PrevRev:     fdb.VersionstampToInt64(walReq.PrevRevision),
			ExpectedRev: walReq.ExpectedRevision,
			Key:         walReq.Key,
			IsCreate:    walReq.IsCreate,
			IsDelete:    walReq.IsDelete,
			Req:         req,
		})
	}

	return requests, walDumps, nil
}

func Create(key string, value []byte) model.EtcdRequest {
	key = replaceKey(key)
	//version := maybeVersion(key, value)
	conditions := []model.EtcdCondition{}
	successOps := []model.EtcdOperation{}
	failureOps := []model.EtcdOperation{}
	if key == "compact_rev_key" {
		conditions = append(conditions, model.EtcdCondition{
			Key:             key,
			ExpectedVersion: 0,
		})
		revAsValue := fmt.Appendf(nil, "%d", 0)
		successOps = append(successOps, model.EtcdOperation{
			Type: model.PutOperation,
			Put: model.PutOptions{
				Key:   key,
				Value: model.ToValueOrHash(string(revAsValue)),
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
		conditions = append(conditions, model.EtcdCondition{
			Key:              key,
			ExpectedRevision: 0,
		})
		successOps = append(successOps, model.EtcdOperation{
			Type: model.PutOperation,
			Put: model.PutOptions{
				Key:   key,
				Value: model.ToValueOrHash(string(value)),
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

func Update(key string, value []byte, prevRevision int64, currentRevision int64, expectedRev int64) model.EtcdRequest {
	key = replaceKey(key)
	version := maybeVersion(key, value)
	successOps := []model.EtcdOperation{}
	conditions := []model.EtcdCondition{}
	if key == "compact_rev_key" {
		conditions = append(conditions, model.EtcdCondition{
			Key:             key,
			ExpectedVersion: version,
		})
		revAsValue := fmt.Appendf(nil, "%d", prevRevision)
		successOps = append(successOps, model.EtcdOperation{
			Type: model.PutOperation,
			Put: model.PutOptions{
				Key:   key,
				Value: model.ToValueOrHash(string(revAsValue)),
			},
		})
	} else {
		conditions = append(conditions, model.EtcdCondition{
			Key:              key,
			ExpectedRevision: expectedRev,
		})
		successOps = append(successOps, model.EtcdOperation{
			Type: model.PutOperation,
			Put: model.PutOptions{
				Key:   key,
				Value: model.ToValueOrHash(string(value)),
			},
		})
	}

	request := model.EtcdRequest{
		Type: model.Txn,
		Txn: &model.TxnRequest{
			Conditions:          conditions,
			OperationsOnSuccess: successOps,
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

func Delete(key string, rev int64, expectedRev int64) model.EtcdRequest {
	conditions := []model.EtcdCondition{}
	successOps := []model.EtcdOperation{}
	failureOps := []model.EtcdOperation{}
	if expectedRev != 0 {
		conditions = append(conditions, model.EtcdCondition{
			Key:              key,
			ExpectedRevision: expectedRev,
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

// Kine uses "compact_rev_key" internally, so API server compaction key is stored as "compact_rev_key_apiserver"
// https://github.com/k3s-io/kine/blob/7399536ef0e5ebd2356c9ac68d8c8850dd07a231/pkg/server/compact.go#L14C2-L14C15
func replaceKey(key string) string {
	if key == "compact_rev_key_apiserver" {
		return "compact_rev_key"
	} else {
		return key
	}
}

// In Kine version is stored as value and only for the compaction key
// https://github.com/k3s-io/kine/blob/7399536ef0e5ebd2356c9ac68d8c8850dd07a231/pkg/server/compact.go#L56
func maybeVersion(key string, value []byte) int64 {
	if key == "compact_rev_key" {
		version, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			panic(err)
		}
		return version
	} else {
		return 0
	}
}
