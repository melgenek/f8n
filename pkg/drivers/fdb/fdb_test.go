package fdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/sync/errgroup"
	"slices"
	"testing"
	"time"

	"github.com/elliotchance/orderedmap/v3"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestFDB(t *testing.T) {
	forceRetryTransaction = func(i int) bool { return false }

	logrus.SetLevel(logrus.TraceLevel)
	n := 4
	sameKeyN := 3
	f := NewFdbStructured("docker:docker@127.0.0.1:4500", "dir1")
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(60)*time.Second)
	err := f.Start(ctx)
	require.NoError(t, err)
	createRecords(t, f, ctx, 500, maxRecordSize) // fill up fdb
	cancelCtx()

	f = NewFdbStructured("docker:docker@127.0.0.1:4500", "dir2")
	ctx, cancelCtx = context.WithTimeout(context.Background(), time.Duration(60)*time.Second)
	defer cancelCtx()
	err = f.Start(ctx)
	require.NoError(t, err)

	watchAll := f.Watch(ctx, "/abc/", 0)
	watchKey := f.Watch(ctx, fmt.Sprintf("/abc/key%04d", n-1), 0)

	_, result, err := f.List(ctx, "/", "/registry/health", 0, 0)
	require.NoError(t, err)
	require.Empty(t, result)

	_, result, err = f.List(ctx, "/registry/health", "/registry/health", 0, 0)
	require.NoError(t, err)
	require.Len(t, result, 1, "Expected to find /registry/health in the list, but got: %v", result)

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

			_, count, err := f.Count(ctx, "/abc/", "/abc", currentRev)
			require.NoError(t, err)
			require.Equal(t, int64(events.Len()), count)

			rev, result, err := f.List(ctx, keyName, keyName, 0, 0)
			require.NoError(t, err)
			require.LessOrEqual(t, nextRev, rev)
			require.Equal(t, []*server.KeyValue{event}, result)

			_, count, err = f.Count(ctx, keyName, keyName, 0)
			require.NoError(t, err)
			require.Equal(t, int64(1), count)

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
	require.GreaterOrEqual(t, rev, currentRev)

	rev, result, err = f.List(ctx, "/abc/", "/abc/key", 0, 0)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.GreaterOrEqual(t, rev, currentRev)

	for i := 0; i < len(history); {
		select {
		case watchedEvents := <-watchAll.Events:
			fmt.Printf("Watched %v\n", watchedEvents)
			for _, watchedEvent := range watchedEvents {
				if i == len(history) {
					require.Fail(t, "I: %d", i)
				}
				require.Equal(t, history[i].KV, watchedEvent.KV)
				i++
			}
		case <-ctx.Done():
			require.Fail(t, "context done")
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
			require.Fail(t, "context done")
		}
	}

	currentRev, err = f.CurrentRevision(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, history[len(history)-1].KV.ModRevision, currentRev)
}

func TestFDBLargeRecords(t *testing.T) {
	forceRetryTransaction = func(i int) bool { return false }
	logrus.SetLevel(logrus.WarnLevel)

	f := NewFdbStructured("docker:docker@127.0.0.1:4500", "dir1")
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(100)*time.Second)
	defer cancelCtx()

	err := f.Start(ctx)
	require.NoError(t, err)

	watchLarge := f.Watch(ctx, "/large/", 0)

	recordCount := 300
	records := createRecords(t, f, ctx, recordCount, maxRecordSize)

	for i := 0; i < recordCount; {
		batch := <-watchLarge.Events
		for _, watchedEvent := range batch {
			i++
			require.Equal(t, records[watchedEvent.KV.Key], watchedEvent.KV.Value, "watched value for key '%s' does not match", watchedEvent.KV.Key)
		}
	}

	// Get record
	_, kv, err := f.Get(ctx, "/large/key42", "", 0, 0)
	require.NoError(t, err)
	require.NotNil(t, kv)
	assert.Equal(t, "/large/key42", kv.Key)
	assert.True(t, bytes.Equal(records["/large/key42"], kv.Value), "value for key '/large/key42' does not match")

	// List all records
	_, kvs, err := f.List(ctx, "/large/", "/large/", 0, 0)
	require.NoError(t, err)
	require.Equal(t, recordCount, len(kvs))

	// List all records
	_, kvs2, err := f.List(ctx, "/large/", "/large/key0", 0, 0)
	require.NoError(t, err)
	require.Equal(t, recordCount-1, len(kvs2))

	// Count all records
	_, count, err := f.Count(ctx, "/large/", "/large/", 0)
	require.NoError(t, err)
	require.Equal(t, int64(recordCount), count)

	// Count all records
	_, count, err = f.Count(ctx, "/large/", "/large/key0", 0)
	require.NoError(t, err)
	require.Equal(t, int64(recordCount)-1, count)

	// Verify listed records
	listedRecords := make(map[string][]byte)
	for _, kv := range kvs {
		listedRecords[kv.Key] = kv.Value
	}
	require.Equal(t, records, listedRecords, "listed records do not match created records")

	// Delete one record
	keyToDelete := "/large/key1"
	_, _, deleted, err := f.Delete(ctx, keyToDelete, 0)
	require.NoError(t, err)
	require.True(t, deleted, "record should be deleted")
	delete(records, keyToDelete)

	// Verify it's gone with Get
	_, kv, err = f.Get(ctx, keyToDelete, "", 0, 0)
	require.NoError(t, err)
	require.Nil(t, kv, "deleted record should not be found with Get")

	// Verify it's gone with List
	_, kvsAfterDelete, err := f.List(ctx, "/large/", "", 0, 0)
	require.NoError(t, err)
	require.Len(t, kvsAfterDelete, recordCount-1, "listKeyValue should have one less record after delete")

	listedRecordsAfterDelete := make(map[string][]byte)
	for _, kv := range kvsAfterDelete {
		listedRecordsAfterDelete[kv.Key] = kv.Value
	}
	require.Equal(t, records, listedRecordsAfterDelete, "listed records after delete do not match expected")
}

func TestCompaction(t *testing.T) {
	keyName := "/abc/key123"
	value := []byte("val123")
	newValue := []byte("newVal123")
	updatedLease := int64(123)

	f := NewFdbStructured("docker:docker@127.0.0.1:4500", "dir1")
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(20)*time.Second)
	defer cancelCtx()

	err := f.Start(ctx)
	require.NoError(t, err)

	// Create a key
	nextRev, err := f.Create(ctx, keyName, value, 0)
	require.NoError(t, err)
	require.Greater(t, nextRev, int64(0), "Expected a valid revision after creation")

	// Update the key
	updatedRev, updatedKv, updated, err := f.Update(ctx, keyName, newValue, nextRev, updatedLease)
	require.NoError(t, err)
	require.True(t, updated, "Expected the key to be updated")
	require.Equal(t, nextRev, updatedKv.CreateRevision, "CreateRevision should match the original")
	require.GreaterOrEqual(t, updatedRev, updatedKv.ModRevision, "ModRevision less than or equal to the new revision")
	require.Equal(t, keyName, updatedKv.Key, "Key should match the original key")
	require.Equal(t, newValue, updatedKv.Value, "Value should be updated value")
	require.Equal(t, updatedLease, updatedKv.Lease, "Lease should match the provided lease")

	// Get created value by revision
	_, kv, err := f.Get(ctx, keyName, "", 0, updatedRev-1)
	require.NoError(t, err)
	require.NotNil(t, kv, "Expected to find the key after compaction")
	require.Equal(t, value, kv.Value, "Value should match the updated value")

	// Compact the database
	compactRev, err := f.Compact(ctx, updatedRev)
	require.NoError(t, err)
	require.Greater(t, compactRev, int64(0), "Expected a valid revision after compaction")

	// Get value by revision
	_, _, err = f.Get(ctx, keyName, "", 0, updatedRev-1)
	require.ErrorIs(t, err, server.ErrCompacted)

	// Verify the key still exists after compaction
	_, kv, err = f.Get(ctx, keyName, "", 0, 0)
	require.NoError(t, err)
	require.NotNil(t, kv, "Expected to find the key after compaction")
	require.Equal(t, keyName, kv.Key, "Key should match the original key")
	require.GreaterOrEqual(t, updatedRev, kv.ModRevision, "ModRevision less than or equal to the new revision")
	require.Equal(t, newValue, kv.Value, "Value should match the updated value")

	// Verify the health key exists
	_, kv, err = f.Get(ctx, "/registry/health", "", 0, 0)
	require.NoError(t, err)
	require.NotNil(t, kv, "Expected to find the key after compaction")

	// Verify the key can still be listed
	_, kvs, err := f.List(ctx, "/abc/", "/abc/key", 0, 0)
	require.NoError(t, err)
	require.Len(t, kvs, 1, "Expected to find one key after compaction")
	require.Equal(t, keyName, kvs[0].Key, "Listed key should match the original key")
	require.GreaterOrEqual(t, updatedRev, kvs[0].ModRevision, "Listed key ModRevision should match the updated revision")
	require.Equal(t, newValue, kvs[0].Value, "Listed key Value should match the updated value")

	// Verify the key can still be counted
	_, count, err := f.Count(ctx, "/abc/", "/abc/key", 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "Expected to count one key after compaction")

	// Verify the key can still be watched
	watch := f.Watch(ctx, keyName, 0)
	select {
	case events := <-watch.Events:
		require.Len(t, events, 1, "Expected one event after watching the key")
		require.Equal(t, keyName, events[0].KV.Key, "Watched key should match the original key")
		require.Equal(t, updatedKv.ModRevision, events[0].KV.ModRevision, "Watched key ModRevision should match the updated revision")
		require.Equal(t, newValue, events[0].KV.Value, "Watched key Value should match the updated value")
	case wRrr := <-watch.Errorc:
		require.NoError(t, wRrr, "Expected no error while watching the key")
	}

	// Verify the key can still be deleted
	deleteRev, deleteKv, deleted, err := f.Delete(ctx, keyName, 0)
	require.NoError(t, err)
	require.True(t, deleted, "Expected the key to be deleted")
	require.GreaterOrEqual(t, deleteRev, updatedRev, "Expected a valid revision after deletion")
	require.Equal(t, keyName, deleteKv.Key, "Deleted key should match the original key")
	require.Equal(t, newValue, deleteKv.Value, "Deleted key Value should match the updated value")
	require.Equal(t, updatedLease, deleteKv.Lease, "Lease should match the provided lease")

	// Verify the key is gone after deletion
	_, kv, err = f.Get(ctx, keyName, "", 0, 0)
	require.NoError(t, err)
	require.Nil(t, kv, "Expected to not find the key after deletion")

	// Verify the key is gone in the list
	_, kvs, err = f.List(ctx, "/abc/", "/abc/key", 0, 0)
	require.NoError(t, err)
	require.Empty(t, kvs, "Expected to not find the key in the list after deletion")

	// Verify the key is gone in the count
	_, count, err = f.Count(ctx, "/abc/", "/abc/key", 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "Expected to count zero keys after deletion")

	// Verify the compact revision is can be done for an empty list
	_, err = f.Compact(ctx, deleteRev)
	require.NoError(t, err)

	// Verify the health key exists
	_, kv, err = f.Get(ctx, "/registry/health", "", 0, 0)
	require.NoError(t, err)
	require.NotNil(t, kv, "Expected to find the key after compaction")

	// Verify the key does not exist
	watch = f.Watch(ctx, keyName, 0)
	select {
	case events := <-watch.Events:
		require.Lenf(t, events, 0, "Expected no events after watching the deleted key")
	case wRrr := <-watch.Errorc:
		require.NoError(t, wRrr, "Expected no error while watching the key")
	case <-time.After(3 * time.Second):
		// No events as expected
	}

	// Verify the old revision cannot be watched
	watch = f.Watch(ctx, keyName, updatedRev)
	require.NotNil(t, watch.CompactRevision)
	require.NotNil(t, watch.CurrentRevision)
}

func TestWatchAll(t *testing.T) {
	logrus.SetLevel(logrus.InfoLevel)

	maxBatchSize = 10
	recordsCount := 53

	f := NewFdbStructured("docker:docker@127.0.0.1:4500", "dir1")
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
	defer cancelCtx()

	err := f.Start(ctx)
	require.NoError(t, err)

	w := f.Watch(ctx, "/large/", 0)

	createRecords(t, f, ctx, recordsCount, 2)

	totalRecords := 0
	for totalRecords < recordsCount {
		select {
		case events := <-w.Events:
			totalRecords += len(events)
		case <-ctx.Done():
			require.Fail(t, "context done")
		}
	}

	require.Equal(t, recordsCount, totalRecords)
}

func TestExceedSizeLarge(t *testing.T) {
	key := "/large/too_large_key"
	value := make([]byte, maxRecordSize+1)
	_, err := rand.Read(value)
	require.NoError(t, err)

	f := NewFdbStructured("docker:docker@127.0.0.1:4500", "dir1")
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(3)*time.Second)
	defer cancelCtx()

	err = f.Start(ctx)
	require.NoError(t, err)

	_, err = f.Create(ctx, key, value, 0)
	require.ErrorIs(t, err, rpctypes.ErrRequestTooLarge)

	_, _, _, err = f.Update(ctx, key, value, 0, 0)
	require.ErrorIs(t, err, rpctypes.ErrRequestTooLarge)
}

func createRecords(t *testing.T, f server.Backend, ctx context.Context, recordCount int, recordSize int) map[string][]byte {
	g := errgroup.Group{}
	g.SetLimit(50)
	records := make(map[string][]byte, recordCount)
	for i := 0; i < recordCount; i++ {
		key := fmt.Sprintf("/large/key%d", i)
		value := make([]byte, recordSize)
		_, err := rand.Read(value)
		require.NoError(t, err)
		records[key] = value
		v := func(key string, value []byte) (e error) {
			_, err := f.Create(ctx, key, value, 0)
			return err
		}
		g.Go(func() error { return v(key, value) })
	}
	require.NoError(t, g.Wait())
	return records
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
