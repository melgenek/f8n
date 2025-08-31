// // Copyright 2023 The etcd Authors
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
package app

//
//import (
//	"fmt"
//	"go.etcd.io/etcd/tests/v3/robustness/model"
//	"sort"
//	"strings"
//)
//
//func NewReplay(persistedRequests []model.EtcdRequest) *EtcdReplay {
//	state := freshEtcdState()
//	// Padding for index 0 and 1, so index matches revision..
//	revisionToEtcdState := []model.EtcdState{state, state}
//	var events []PersistedEvent
//	for _, request := range persistedRequests {
//		newState, response := state.Step(request)
//		if state.Revision != newState.Revision {
//			revisionToEtcdState = append(revisionToEtcdState, newState)
//		}
//		events = append(events, toWatchEvents(&state, request, response)...)
//		state = newState
//	}
//	return &EtcdReplay{
//		revisionToEtcdState: revisionToEtcdState,
//		Events:              events,
//	}
//}
//
//type EtcdReplay struct {
//	revisionToEtcdState []model.EtcdState
//	Events              []PersistedEvent
//}
//
//func (r *EtcdReplay) StateForRevision(revision int64) (model.EtcdState, error) {
//	if int(revision) >= len(r.revisionToEtcdState) {
//		return model.EtcdState{}, fmt.Errorf("requested revision %d, higher than observed in replay %d", revision, len(r.revisionToEtcdState)-1)
//	}
//	return r.revisionToEtcdState[revision], nil
//}
//
//func (r *EtcdReplay) EventsForWatch(watch WatchRequest) (events []PersistedEvent) {
//	for _, e := range r.Events {
//		if e.Revision < watch.Revision || !e.Match(watch) {
//			continue
//		}
//		events = append(events, e)
//	}
//	return events
//}
//
//func toWatchEvents(prevState *model.EtcdState, request model.EtcdRequest, response model.MaybeEtcdResponse) (events []PersistedEvent) {
//	if response.Error != "" {
//		return events
//	}
//
//	switch request.Type {
//	case model.Txn:
//		var ops []model.EtcdOperation
//		if response.Txn.Failure {
//			ops = request.Txn.OperationsOnFailure
//		} else {
//			ops = request.Txn.OperationsOnSuccess
//		}
//		for _, op := range ops {
//			switch op.Type {
//			case model.RangeOperation:
//			case model.DeleteOperation:
//				e := PersistedEvent{
//					Event: Event{
//						Type: op.Type,
//						Key:  op.Delete.Key,
//					},
//					Revision: response.Revision,
//				}
//				if _, ok := prevState.KeyValues[op.Delete.Key]; ok {
//					events = append(events, e)
//				}
//			case model.PutOperation:
//				_, leaseExists := prevState.Leases[op.Put.LeaseID]
//				if op.Put.LeaseID != 0 && !leaseExists {
//					break
//				}
//
//				e := PersistedEvent{
//					Event: Event{
//						Type:  op.Type,
//						Key:   op.Put.Key,
//						Value: op.Put.Value,
//					},
//					Revision: response.Revision,
//				}
//				if _, ok := prevState.KeyValues[op.Put.Key]; !ok {
//					e.IsCreate = true
//				}
//				events = append(events, e)
//			default:
//				panic(fmt.Sprintf("unsupported operation type: %v", op))
//			}
//		}
//	case model.LeaseRevoke:
//		deletedKeys := []string{}
//		for key := range prevState.Leases[request.LeaseRevoke.LeaseID].Keys {
//			if _, ok := prevState.KeyValues[key]; ok {
//				deletedKeys = append(deletedKeys, key)
//			}
//		}
//
//		sort.Strings(deletedKeys)
//		for _, key := range deletedKeys {
//			e := PersistedEvent{
//				Event: Event{
//					Type: model.DeleteOperation,
//					Key:  key,
//				},
//				Revision: response.Revision,
//			}
//			events = append(events, e)
//		}
//	}
//	return events
//}
//
//type WatchEvent struct {
//	PersistedEvent
//	PrevValue *model.ValueRevision
//}
//
//type PersistedEvent struct {
//	Event
//	Revision int64
//	IsCreate bool
//}
//
//type Event struct {
//	Type  model.OperationType
//	Key   string
//	Value model.ValueOrHash
//}
//
//func (e Event) Match(request WatchRequest) bool {
//	if request.WithPrefix {
//		return strings.HasPrefix(e.Key, request.Key)
//	}
//	return e.Key == request.Key
//}
//
//type WatchRequest struct {
//	Key                string
//	Revision           int64
//	WithPrefix         bool
//	WithProgressNotify bool
//	WithPrevKV         bool
//}
