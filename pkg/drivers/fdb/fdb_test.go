package fdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
	"time"

	"github.com/elliotchance/orderedmap/v3"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestFDB(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)
	n := 4
	sameKeyN := 3
	f := NewFdbStructured("VufDkgAW:O2dFQHXk@127.0.0.1:4689")
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(20)*time.Second)
	defer cancelCtx()

	err := f.Start(ctx)
	require.NoError(t, err)

	watchAll := f.Watch(ctx, "/abc/", 0)
	watchKey := f.Watch(ctx, fmt.Sprintf("/abc/key%04d", n-1), 0)

	_, result, err := f.List(ctx, "/", "/registry/health", 0, 0)
	require.NoError(t, err)
	require.Empty(t, result)

	_, result, err = f.List(ctx, "/registry/health", "/registry/health", 0, 0)
	require.NoError(t, err)
	require.Len(t, result, 1)

	events := orderedmap.NewOrderedMap[string, []*server.KeyValue]()
	history := make([]*server.Event, 0)

	nextRev, err := f.Create(ctx, "/abc/l/", []byte("/xyzV"), 0)
	require.NoError(t, err)

	event := &server.KeyValue{
		Key:            "/abc/l/",
		CreateRevision: nextRev,
		ModRevision:    nextRev,
		Value:          []byte("/xyzV"),
		Lease:          0,
	}

	events.Set("/abc/l/", []*server.KeyValue{event})
	history = append(history, &server.Event{Delete: false, Create: true, KV: event})
	currentRev := nextRev

	_, result, err = f.List(ctx, "/abc/l/", "/abc/l/", 0, currentRev)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)

	for i := 0; i < n; i++ {
		for j := 0; j < sameKeyN; j++ {
			keyName := fmt.Sprintf("/abc/key%04d", i)
			value := []byte(fmt.Sprintf("val%04d", i))

			var event *server.KeyValue
			if j == 0 {
				_, getResult, err := f.Get(ctx, keyName, "", 1, 0)
				require.NoError(t, err)
				require.Empty(t, getResult)

				nextRev, err = f.Create(ctx, keyName, value, 0)
				require.NoError(t, err)
				event = &server.KeyValue{
					Key:            keyName,
					CreateRevision: nextRev,
					ModRevision:    nextRev,
					Value:          value,
					Lease:          0,
				}
			} else {
				keyEvents := events.GetOrDefault(keyName, []*server.KeyValue{})
				prevRev := keyEvents[len(keyEvents)-1].ModRevision
				updatedRev, newKv, updated, err := f.Update(ctx, keyName, value, prevRev, 0)
				nextRev = updatedRev
				event = &server.KeyValue{
					Key:            keyName,
					CreateRevision: keyEvents[0].CreateRevision,
					ModRevision:    nextRev,
					Value:          value,
					Lease:          0,
				}
				require.NoError(t, err)
				require.True(t, updated)
				require.Equal(t, event, newKv)
			}

			_, err = f.Create(ctx, keyName, value, 0)
			require.Error(t, err)

			newCurrentRev, err := f.CurrentRevision(ctx)
			require.NoError(t, err)
			require.LessOrEqual(t, nextRev, newCurrentRev)
			require.Less(t, currentRev, nextRev)

			_, result, err = f.List(ctx, "/abc/", "/abc/key", 0, currentRev)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			_, result, err = f.List(ctx, "/abc/", "", 0, currentRev)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			_, result, err = f.List(ctx, "/abc/", "/abc/", 0, currentRev)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			_, result, err = f.List(ctx, "/abc/", "/abc", 0, currentRev)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			rev, result, err := f.List(ctx, keyName, keyName, 0, 0)
			require.NoError(t, err)
			require.LessOrEqual(t, nextRev, rev)
			require.Equal(t, []*server.KeyValue{event}, result)

			rev, result, err = f.List(ctx, "/", keyName, 0, 0)
			require.NoError(t, err)
			require.NotContains(t, result, event)

			keyEvents := events.GetOrDefault(keyName, []*server.KeyValue{})
			var prevKv *server.KeyValue
			if j != 0 {
				prevKv = keyEvents[len(keyEvents)-1]
			}
			history = append(history, &server.Event{Delete: false, Create: j == 0, KV: event, PrevKV: prevKv})
			events.Set(keyName, append(keyEvents, event))
			currentRev = nextRev

			rev, result, err = f.List(ctx, "/abc/", "/abc/key", 0, currentRev)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), result)
			require.Equal(t, currentRev, rev)

			_, result, err = f.List(ctx, "/abc/key", "/abc/key", 0, currentRev)
			require.NoError(t, err)
			require.Empty(t, result)

			rev, result, err = f.List(ctx, keyName, keyName, 0, currentRev)
			require.NoError(t, err)
			require.Equal(t, nextRev, rev)
			require.Equal(t, []*server.KeyValue{event}, result)

			rev, result, err = f.List(ctx, "/", keyName, 0, currentRev)
			require.NoError(t, err)
			require.NotContains(t, result, event)

			rev, result, err = f.List(ctx, "/abc/", "/registry/health", 0, 1)
			require.NoError(t, err)
			require.Empty(t, result)
			require.Equal(t, rev, int64(1))

			fmt.Printf("%d\n", i)
		}
	}

	keyName := fmt.Sprintf("/abc/key%04d", 0)
	deleteRev, deleteEvent, deletedRet, err := f.Delete(ctx, keyName, 0)
	logrus.Tracef("Deleted %v", deleteEvent)

	require.NoError(t, err)
	require.True(t, deletedRet)

	currentRev = deleteRev

	keyEvents := events.GetOrDefault(keyName, []*server.KeyValue{})
	history = append(history, &server.Event{Delete: true, Create: false, KV: deleteEvent, PrevKV: keyEvents[len(keyEvents)-1]})
	events.Delete(keyName)

	rev, result, err := f.List(ctx, "/abc/", "/abc/key", 0, currentRev)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.Equal(t, rev, currentRev)

	rev, result, err = f.List(ctx, "/abc/", "/abc/key", 0, 0)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.Equal(t, rev, currentRev)

	for i := 0; i < len(history); {
		fmt.Printf("Watch %d\n", i)
		select {
		case watchedEvents := <-watchAll.Events:
			for _, watchedEvent := range watchedEvents {
				if i == len(history) {
					require.Fail(t, "I: %d", i)
				}
				require.Equal(t, history[i].KV, watchedEvent.KV)
				i++
			}
		case <-ctx.Done():
			require.Errorf(t, ctx.Err(), "context done")
		}
	}

	for i := 0; i < sameKeyN; {
		select {
		case watchedEvents := <-watchKey.Events:
			for _, watchedEvent := range watchedEvents {
				if i == n {
					require.Fail(t, "I: %d", i)
				}
				require.Equal(t, history[n*sameKeyN-sameKeyN+i+1], watchedEvent)
				i++
			}
		case <-ctx.Done():
			require.Errorf(t, ctx.Err(), "context done")
		}
	}
}

func TestFDBLargeRecords(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)

	f := NewFdbStructured("VufDkgAW:O2dFQHXk@127.0.0.1:4689")
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(20)*time.Second)
	defer cancelCtx()

	err := f.Start(ctx)
	require.NoError(t, err)

	recordCount := 5
	recordSize := 8 * 1024 * 1024 // 8 MiB

	// Create large records
	records := make(map[string][]byte)
	for i := 0; i < recordCount; i++ {
		key := fmt.Sprintf("/large/key%d", i)
		value := make([]byte, recordSize)
		_, err := rand.Read(value)
		require.NoError(t, err)
		records[key] = value

		rev, err := f.Create(ctx, key, value, 0)
		require.NoError(t, err)
		assert.Greater(t, rev, int64(0))
	}

	//// Get each record individually
	for key, value := range records {
		_, kv, err := f.Get(ctx, key, "", 0, 0)
		require.NoError(t, err)
		require.NotNil(t, kv)
		assert.Equal(t, key, kv.Key)
		assert.True(t, bytes.Equal(value, kv.Value), "value for key %s does not match", key)
	}

	// List all records
	_, kvs, err := f.List(ctx, "/large/", "/large/", 0, 0)
	require.NoError(t, err)
	assert.Len(t, kvs, recordCount)

	// Verify listed records
	listedRecords := make(map[string][]byte)
	for _, kv := range kvs {
		listedRecords[kv.Key] = kv.Value
	}
	assert.Equal(t, records, listedRecords, "listed records do not match created records")

	// Delete one record
	keyToDelete := "/large/key1"
	_, _, deleted, err := f.Delete(ctx, keyToDelete, 0)
	require.NoError(t, err)
	require.True(t, deleted, "record should be deleted")
	delete(records, keyToDelete)

	// Verify it's gone with Get
	_, kv, err := f.Get(ctx, keyToDelete, "", 0, 0)
	require.NoError(t, err)
	require.Nil(t, kv, "deleted record should not be found with Get")

	// Verify it's gone with List
	_, kvsAfterDelete, err := f.List(ctx, "/large/", "", 0, 0)
	require.NoError(t, err)
	require.Len(t, kvsAfterDelete, recordCount-1, "list should have one less record after delete")

	listedRecordsAfterDelete := make(map[string][]byte)
	for _, kv := range kvsAfterDelete {
		listedRecordsAfterDelete[kv.Key] = kv.Value
	}
	require.Equal(t, records, listedRecordsAfterDelete, "listed records after delete do not match expected")
}

func orEmpty(result []*server.KeyValue) []*server.KeyValue {
	if result == nil {
		result = make([]*server.KeyValue, 0)
	}
	return result
}

func valuesAsSlice(events *orderedmap.OrderedMap[string, []*server.KeyValue]) []*server.KeyValue {
	flat := make([]*server.KeyValue, 0)
	for _, kvs := range slices.Collect(events.Values()) {
		flat = append(flat, kvs[len(kvs)-1])
	}

	slices.SortFunc(flat, func(a, b *server.KeyValue) int {
		if a.Key < b.Key {
			return -1
		} else {
			return 1
		}
	})

	return flat
}
