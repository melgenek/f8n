package fdb

import (
	"context"
	"fmt"
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
