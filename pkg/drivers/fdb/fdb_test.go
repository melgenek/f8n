package fdb

import (
	"context"
	"fmt"
	"github.com/elliotchance/orderedmap/v3"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"slices"
	"testing"
	"time"
)

func TestFDB(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)
	n := 4
	sameKeyN := 1
	f := NewFdbStructured("VufDkgAW:O2dFQHXk@127.0.0.1:4689")
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(1000)*time.Second)
	defer cancelCtx()

	err := f.Start(ctx)
	require.NoError(t, err)

	watchAll := f.Watch(ctx, "/abc/", 0)
	watchKey := f.Watch(ctx, fmt.Sprintf("/abc/key%04d", n-1), 0)

	events := orderedmap.NewOrderedMap[int, *server.KeyValue]()
	history := make([]*server.KeyValue, 0)

	newRev, err := f.Create(ctx, "/abc/l/", []byte("/xyzV"), 0)
	require.NoError(t, err)

	event := &server.KeyValue{
		Key:            "/abc/l/",
		CreateRevision: newRev,
		ModRevision:    newRev,
		Value:          []byte("/xyzV"),
		Lease:          0,
	}

	events.Set(-1, event)
	history = append(history, event)
	currentRev := newRev

	_, result, err := f.List(ctx, "/abc/l/", "/abc/l/", 0, currentRev)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)

	for i := 0; i < n; i++ {
		keyName := fmt.Sprintf("/abc/key%04d", i)
		value := []byte(fmt.Sprintf("val%04d", i))

		// _, result, err := f.Get(ctx, keyName, "", 1, 0)
		// require.NoError(t, err)
		// require.Empty(t, result)

		newRev, err := f.Create(ctx, keyName, value, 0)
		require.NoError(t, err)

		_, err = f.Create(ctx, keyName, value, 0)
		require.Error(t, err)

		event := &server.KeyValue{
			Key:            keyName,
			CreateRevision: newRev,
			ModRevision:    newRev,
			Value:          value,
			Lease:          0,
		}

		require.EventuallyWithT(t, func(t *assert.CollectT) {
			newCurrentRev, err := f.CurrentRevision(ctx)
			assert.NoError(t, err)
			assert.Equal(t, newRev, newCurrentRev)
			assert.Less(t, currentRev, newRev)
		}, 1*time.Second, 10*time.Millisecond)

		_, result, err = f.List(ctx, "/abc/", "/abc/key", 0, currentRev)
		require.NoError(t, err)
		require.Equal(t, valuesAsSlice(events), orEmpty(result))

		_, result, err = f.List(ctx, "/abc/", "", 0, currentRev)
		assert.NoError(t, err)
		require.Equal(t, valuesAsSlice(events), orEmpty(result))

		// _, result, err = f.list(ctx, "/", "/abc/key", 0, currentRev, false)
		// require.NoError(t, err)
		// require.Equal(t, valuesAsSlice(events), orEmpty(result))

		_, result, err = f.List(ctx, "/abc/", "/abc/", 0, currentRev)
		require.NoError(t, err)
		require.Equal(t, valuesAsSlice(events), orEmpty(result))

		_, result, err = f.List(ctx, "/abc/", "/abc", 0, currentRev)
		require.NoError(t, err)
		require.Equal(t, valuesAsSlice(events), orEmpty(result))

		rev, result, err := f.List(ctx, keyName, keyName, 0, 0)
		require.NoError(t, err)
		require.Equal(t, newRev, rev)
		require.Equal(t, []*server.KeyValue{event}, result)

		// rev, result, err = f.after(ctx, "", currentRev, int64(n))
		// require.NoError(t, err)
		// require.Equal(t, newRev, rev)
		// require.Empty(t, result)

		// rev, result, err = f.after(ctx, "/abc/", currentRev, int64(n))
		// require.NoError(t, err)
		// require.Equal(t, newRev, rev)
		// require.Equal(t, []*server.Event{event}, result)

		events.Set(i, event)
		history = append(history, event)
		currentRev = newRev

		// rev, result, err = f.after(ctx, "/abc/", currentRev, int64(n))
		// require.NoError(t, err)
		// require.Equal(t, newRev, rev)
		// require.Empty(t, result)

		// rev, result, err = f.after(ctx, "/", currentRev, int64(n))
		// require.NoError(t, err)
		// require.Equal(t, newRev, rev)
		// require.Empty(t, result)

		rev, result, err = f.List(ctx, "/abc/", "/abc/key", 0, currentRev)
		require.NoError(t, err)
		require.Equal(t, valuesAsSlice(events), result)
		require.Equal(t, currentRev, rev)

		rev, result, err = f.List(ctx, keyName, keyName, 0, currentRev)
		require.NoError(t, err)
		require.Equal(t, newRev, rev)
		require.Equal(t, []*server.KeyValue{event}, result)

		rev, result, err = f.List(ctx, "/abc/", "/registry/health", 0, 1)
		require.NoError(t, err)
		require.Empty(t, result)
		require.Equal(t, rev, int64(1))

		fmt.Printf("%d\n", i)
	}

	keyName := fmt.Sprintf("/abc/key%04d", 0)
	deleteRev, deleteEvent, deletedRet, err := f.Delete(ctx, keyName, 0)
	logrus.Tracef("Deleted %v", deleteEvent)

	require.NoError(t, err)
	require.True(t, deletedRet)

	currentRev = deleteRev
	events.Set(0, deleteEvent)
	history = append(history, deleteEvent)

	// rev, result, err := f.list(ctx, "/abc/", "/abc/key", 0, 0, true)
	// require.NoError(t, err)
	// require.Equal(t, valuesAsSlice(events), result)
	// require.Equal(t, rev, currentRev)

	events.Delete(0)

	rev, result, err := f.List(ctx, "/abc/", "/abc/key", 0, currentRev)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.Equal(t, rev, currentRev)

	rev, result, err = f.List(ctx, "/abc/", "/abc/key", 0, 0)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.Equal(t, rev, currentRev)

	// rev, result, err = f.list(ctx, "/", "/", 0, 0, false)
	// require.NoError(t, err)
	// require.Equal(t, valuesAsSlice(events), result)
	// require.Equal(t, rev, currentRev)

	for i := 0; i < len(history); {
		fmt.Printf("Watch %d\n", i)
		select {
		case watchedEvents := <-watchAll.Events:
			for _, watchedEvent := range watchedEvents {
				if i == len(history) {
					require.Fail(t, "I: %d", i)
				}
				logrus.Tracef("Watched %v", watchedEvent)
				require.Equal(t, history[i], watchedEvent.KV)
				i++
			}
		case <-ctx.Done():
			require.Errorf(t, ctx.Err(), "context done")
		}
	}

	for i := 0; i < sameKeyN; i++ {
		select {
		case watchedEvents := <-watchKey.Events:
			for _, watchedEvent := range watchedEvents {
				if i == n {
					require.Fail(t, "I: %d", i)
				}
				require.Equal(t, history[n*sameKeyN-sameKeyN+i+1], watchedEvent.KV)
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

func valuesAsSlice(events *orderedmap.OrderedMap[int, *server.KeyValue]) []*server.KeyValue {
	res := slices.Collect(events.Values())
	if res == nil {
		return make([]*server.KeyValue, 0)
	} else {
		tmp := res[0]
		res = res[1:]
		res = append(res, tmp)
		return res
	}
}
