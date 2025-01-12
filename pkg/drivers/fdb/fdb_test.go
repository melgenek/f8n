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

	watchAll := f.Watch(ctx, "/abc/")
	watchKey := f.Watch(ctx, fmt.Sprintf("/abc/key%04d", n-1))

	events := orderedmap.NewOrderedMap[int, *server.Event]()
	history := make([]*server.Event, 0)

	event := &server.Event{
		Create: true,
		KV: &server.KeyValue{
			Key:   "/abc/l/",
			Value: []byte("/xyzV"),
		},
		PrevKV: &server.KeyValue{
			Value: []byte("/xyzPV"),
		},
	}
	newRev, err := f.Append(ctx, event)
	require.NoError(t, err)
	event.KV.ModRevision = newRev
	event.KV.CreateRevision = newRev
	event.PrevKV = nil

	events.Set(-1, event)
	history = append(history, event)
	currentRev := newRev

	_, result, err := f.List(ctx, "/abc/l/", "/abc/l/", 0, currentRev, false)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)

	for i := 0; i < n; i++ {
		keyName := fmt.Sprintf("/abc/key%04d", i)
		_, result, err := f.List(ctx, keyName, "", 1, 0, true)
		require.NoError(t, err)
		require.Empty(t, result)

		for j := 0; j < sameKeyN; j++ {
			event := &server.Event{
				Create: true,
				KV: &server.KeyValue{
					Key:   keyName,
					Value: []byte(fmt.Sprintf("val%04d", i)),
				},
				PrevKV: &server.KeyValue{
					Value: []byte(fmt.Sprintf("prevVal%04d", i)),
				},
			}

			newRev, err := f.Append(ctx, event)
			require.NoError(t, err)
			event.KV.ModRevision = newRev
			event.KV.CreateRevision = newRev
			event.PrevKV = nil

			require.EventuallyWithT(t, func(t *assert.CollectT) {
				newCurrentRev, err := f.CurrentRevision(ctx)
				assert.NoError(t, err)
				assert.Equal(t, newRev, newCurrentRev)
				assert.Less(t, currentRev, newRev)
			}, 1*time.Second, 10*time.Millisecond)

			_, result, err := f.List(ctx, "/abc/", "/abc/key", 0, currentRev, false)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			_, result, err = f.List(ctx, "/abc/", "", 0, currentRev, false)
			assert.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			_, result, err = f.List(ctx, "/", "/abc/key", 0, currentRev, false)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			_, result, err = f.List(ctx, "/abc/", "/abc/", 0, currentRev, false)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			_, result, err = f.List(ctx, "/abc/", "/abc", 0, currentRev, false)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), orEmpty(result))

			rev, result, err := f.List(ctx, keyName, keyName, 0, 0, false)
			require.NoError(t, err)
			require.Equal(t, newRev, rev)
			require.Equal(t, []*server.Event{event}, result)

			rev, result, err = f.After(ctx, "", currentRev, int64(n))
			require.NoError(t, err)
			require.Equal(t, newRev, rev)
			require.Empty(t, result)

			rev, result, err = f.After(ctx, "/abc/", currentRev, int64(n))
			require.NoError(t, err)
			require.Equal(t, newRev, rev)
			require.Equal(t, []*server.Event{event}, result)

			events.Set(i, event)
			history = append(history, event)
			currentRev = newRev

			rev, result, err = f.After(ctx, "/abc/", currentRev, int64(n))
			require.NoError(t, err)
			require.Equal(t, newRev, rev)
			require.Empty(t, result)

			rev, result, err = f.After(ctx, "/", currentRev, int64(n))
			require.NoError(t, err)
			require.Equal(t, newRev, rev)
			require.Empty(t, result)

			rev, result, err = f.List(ctx, "/abc/", "/abc/key", 0, currentRev, false)
			require.NoError(t, err)
			require.Equal(t, valuesAsSlice(events), result)
			require.Equal(t, currentRev, rev)

			rev, result, err = f.List(ctx, keyName, keyName, 0, currentRev, false)
			require.NoError(t, err)
			require.Equal(t, newRev, rev)
			require.Equal(t, []*server.Event{event}, result)

			rev, result, err = f.List(ctx, "/abc/", "/registry/health", 0, 1, false)
			require.NoError(t, err)
			require.Empty(t, result)
			require.Equal(t, currentRev, rev)
		}
		fmt.Printf("%d\n", i)
	}

	keyName := fmt.Sprintf("/abc/key%04d", 0)
	deleteEvent := &server.Event{
		Create: false,
		Delete: true,
		KV: &server.KeyValue{
			Key:   keyName,
			Value: []byte(fmt.Sprintf("val%04d", 0)),
		},
		PrevKV: &server.KeyValue{
			ModRevision: 1,
			Value:       []byte(fmt.Sprintf("prevVal%04d", 0)),
		},
	}

	deleteRev, err := f.Append(ctx, deleteEvent)
	require.NoError(t, err)
	deleteEvent.KV.ModRevision = deleteRev

	currentRev = deleteRev
	events.Set(0, deleteEvent)
	history = append(history, deleteEvent)

	rev, result, err := f.List(ctx, "/abc/", "/abc/key", 0, 0, true)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.Equal(t, rev, currentRev)

	events.Delete(0)

	rev, result, err = f.List(ctx, "/abc/", "/abc/key", 0, currentRev, false)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.Equal(t, rev, currentRev)

	rev, result, err = f.List(ctx, "/abc/", "/abc/key", 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.Equal(t, rev, currentRev)

	rev, result, err = f.List(ctx, "/", "/", 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, valuesAsSlice(events), result)
	require.Equal(t, rev, currentRev)

	for i := 0; i < len(history); {
		fmt.Printf("Watch %d\n", i)
		select {
		case watchedEvents := <-watchAll:
			for _, watchedEvent := range watchedEvents {
				if i == len(history) {
					require.Fail(t, "I: %d", i)
				}
				require.Equal(t, history[i], watchedEvent)
				i++
			}
		case <-ctx.Done():
			require.Errorf(t, ctx.Err(), "context done")
		}
	}

	for i := 0; i < sameKeyN; i++ {
		select {
		case watchedEvents := <-watchKey:
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

func orEmpty(result []*server.Event) []*server.Event {
	if result == nil {
		result = make([]*server.Event, 0)
	}
	return result
}

func valuesAsSlice(events *orderedmap.OrderedMap[int, *server.Event]) []*server.Event {
	res := slices.Collect(events.Values())
	if res == nil {
		return make([]*server.Event, 0)
	} else {
		tmp := res[0]
		res = res[1:]
		res = append(res, tmp)
		return res
	}
}
