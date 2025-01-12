package sqllog

import (
	"context"
	"fmt"
	"github.com/elliotchance/orderedmap/v3"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"slices"
	"testing"
	"time"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
			(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name INTEGER,
				created INTEGER,
				deleted INTEGER,
				create_revision INTEGER,
				prev_revision INTEGER,
				lease INTEGER,
				value BLOB,
				old_value BLOB
			)`,
		`CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name)`,
		`CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id)`,
		`CREATE INDEX IF NOT EXISTS kine_id_deleted_index ON kine (id,deleted)`,
		`CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
		`PRAGMA wal_checkpoint(TRUNCATE)`,
	}
)

func TestSqlite(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)
	n := 4
	sameKeyN := 1
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(1000)*time.Second)
	defer cancelCtx()
	config := &drivers.Config{
		ConnectionPoolConfig: generic.ConnectionPoolConfig{
			MaxIdle:     0,
			MaxOpen:     0,
			MaxLifetime: time.Hour,
		},
		MetricsRegisterer: prometheus.NewRegistry(),
	}

	err := os.RemoveAll("./db")
	require.NoError(t, err)

	err = os.MkdirAll("./db", 0700)
	require.NoError(t, err)
	dataSourceName := "./db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate"

	dialect, err := generic.Open(ctx, "sqlite3", dataSourceName, config.ConnectionPoolConfig, "?", false, config.MetricsRegisterer)
	require.NoError(t, err)
	dialect.LastInsertID = true
	dialect.GetSizeSQL = `SELECT SUM(pgsize) FROM dbstat`
	dialect.CompactSQL = `
		DELETE FROM kine AS kv
		WHERE
			kv.id IN (
				SELECT kp.prev_revision AS id
				FROM kine AS kp
				WHERE
					kp.name != 'compact_rev_key' AND
					kp.prev_revision != 0 AND
					kp.id <= ?
				UNION
				SELECT kd.id AS id
				FROM kine AS kd
				WHERE
					kd.deleted != 0 AND
					kd.id <= ?
			)`
	dialect.PostCompactSQL = `PRAGMA wal_checkpoint(FULL)`
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(sqlite3.Error); ok {
			return fmt.Sprint(err.ExtendedCode)
		}
		return err.Error()
	}
	for _, stmt := range schema {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		_, err := dialect.DB.Exec(stmt)
		require.NoError(t, err)
	}

	dialect.Migrate(context.Background())
	f := New(dialect)
	f.ctx = ctx

	err = f.Start(ctx)
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

	events.Delete(0)
	history = append(history, deleteEvent)
	currentRev = deleteRev

	rev, result, err := f.List(ctx, "/abc/", "/abc/key", 0, currentRev, false)
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
