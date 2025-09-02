package fdb

import (
	"fmt"
	"github.com/melgenek/f8n/pkg/drivers/fdb"
	"go.etcd.io/etcd/tests/v3/robustness/model"
	"strconv"
)

func WALToEtcdRequests() ([]model.EtcdRequest, map[fdb.RevRecord]model.EtcdRequest, error) {
	wal, err := fdb.ThisFDB.ReadWAL()
	if err != nil {
		return nil, nil, err
	}
	requests := make([]model.EtcdRequest, 0, len(wal))
	mapping := make(map[fdb.RevRecord]model.EtcdRequest)
	for _, walReq := range wal {
		var req model.EtcdRequest
		if walReq.Record.IsCreate {
			req = Create(walReq.Record.Key, walReq.Record.Value)
		} else if walReq.Record.IsDelete {
			req = Delete(walReq.Record.Key, fdb.VersionstampToInt64(walReq.Record.PrevRevision))
		} else {
			req = Update(
				walReq.Record.Key,
				walReq.Record.Value,
				fdb.VersionstampToInt64(walReq.Record.PrevRevision),
				fdb.VersionstampToInt64(walReq.Rev),
			)
		}
		requests = append(requests, req)
		mapping[walReq] = req
	}

	return requests, mapping, nil
}

func Create(key string, value []byte) model.EtcdRequest {
	key = replaceKey(key)
	version := maybeVersion(key, value)
	conditions := []model.EtcdCondition{}
	successOps := []model.EtcdOperation{}
	failureOps := []model.EtcdOperation{}
	if key == "compact_rev_key" {
		revAsValue := fmt.Appendf(nil, "%d", 0)
		conditions = append(conditions, model.EtcdCondition{
			Key:             key,
			ExpectedVersion: version,
		})
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
		successOps = append(successOps, model.EtcdOperation{
			Type: model.PutOperation,
			Put: model.PutOptions{
				Key:   key,
				Value: model.ToValueOrHash(string(value)),
			},
		})
		conditions = append(conditions, model.EtcdCondition{
			Key:              key,
			ExpectedRevision: 0,
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

//logger.go:146: 2025-09-02T09:09:23.929+0200	ERROR	Broke watch guarantee	{"guarantee": "reliable", "client": 2, "diff": "  []model.PersistedEvent{
//  \t... // 458 identical elements
//  \t{Event: {Type: \"put-operation\", Key: \"/registry/pods/default/8iAu9\", Value: {Value: \"615\"}}, Revision: 460},
//  \t{Event: {Type: \"put-operation\", Key: \"/registry/pods/default/ep8rS\", Value: {Value: \"602\"}}, Revision: 461},
//- \t{
//- \t\tEvent: model.Event{
//- \t\t\tType:  \"put-operation\",
//- \t\t\tKey:   \"compact_rev_key\",
//- \t\t\tValue: model.ValueOrHash{Value: \"0\"},
//- \t\t},
//- \t\tRevision: 462,
//- \t\tIsCreate: true,
//- \t},
//  \t{Event: {Type: \"put-operation\", Key: \"/tombstone\", Value: {Value: \"true\"}}, Revision: 463, ...},
//  }
//"}

func Update(key string, value []byte, prevRevision int64, currentRevision int64) model.EtcdRequest {
	key = replaceKey(key)
	version := maybeVersion(key, value)
	successOps := []model.EtcdOperation{}
	conditions := []model.EtcdCondition{}
	if key == "compact_rev_key" {
		revAsValue := fmt.Appendf(nil, "%d", prevRevision)
		successOps = append(successOps, model.EtcdOperation{
			Type: model.PutOperation,
			Put: model.PutOptions{
				Key:   key,
				Value: model.ToValueOrHash(string(revAsValue)),
			},
		})
		conditions = append(conditions, model.EtcdCondition{
			Key:             key,
			ExpectedVersion: version,
		})
	} else {
		successOps = append(successOps, model.EtcdOperation{
			Type: model.PutOperation,
			Put: model.PutOptions{
				Key:   key,
				Value: model.ToValueOrHash(string(value)),
			},
		})
		conditions = append(conditions, model.EtcdCondition{
			Key:              key,
			ExpectedRevision: prevRevision,
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

func Delete(key string, rev int64) model.EtcdRequest {
	conditions := []model.EtcdCondition{}
	successOps := []model.EtcdOperation{}
	failureOps := []model.EtcdOperation{}
	if rev != 0 {
		conditions = append(conditions, model.EtcdCondition{
			Key:              key,
			ExpectedRevision: rev,
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

func replaceKey(key string) string {
	if key == "compact_rev_key_apiserver" {
		return "compact_rev_key"
	} else {
		return key
	}
}

func maybeVersion(key string, value []byte) int64 {
	if key == "compact_rev_key" {
		version, err := strconv.Atoi(string(value))
		if err != nil {
			panic(err)
		}
		return int64(version - 1)
	} else {
		return 0
	}
}
