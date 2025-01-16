package fdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/util/workqueue"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// Ensure Backend implements server.Backend.
	// _ logstructured.Log = (&FdbStructured{})
	_ server.Backend = (&FdbStructured{})
)

const (
	retryInterval = 250 * time.Millisecond
)

type Fdb struct {
	logstructured.LogStructured
}

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	return false, NewFdbStructured(cfg.DataSourceName), nil
}

func init() {
	drivers.Register("fdb", New)
}

type ttlEventKV struct {
	key         string
	modRevision int64
	expiredAt   time.Time
}

type FdbStructured struct {
	connectionString   string
	db                 fdb.Database
	kine               directory.DirectorySubspace
	byRevision         subspace.Subspace
	byKeysAndRevisions subspace.Subspace
	sequence           subspace.Subspace

	notify      chan int64
	broadcaster broadcaster.Broadcaster
	ctx         context.Context
	currentRev  int64
}

func NewFdbStructured(connectionString string) *FdbStructured {
	return &FdbStructured{
		connectionString: connectionString,
		notify:           make(chan int64, 1024),
	}
}

func (f *FdbStructured) Start(ctx context.Context) error {
	//logrus.Tracef("Starting")
	f.ctx = ctx
	fdb.MustAPIVersion(730)

	db, err := fdb.OpenWithConnectionString(f.connectionString)

	if err != nil {
		return err
	}
	f.db = db

	kine, err := directory.CreateOrOpen(db, []string{"kine"}, nil)
	if err != nil {
		return err
	}
	f.kine = kine

	_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(kine)
		return
	})

	//var tr fdb.Transaction
	//_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
	//	key := fdb.Key("firstVersionstamp")
	//	value := tr.Get(key).MustGet()
	//	if value == nil {
	//		versionstamp := tuple.IncompleteVersionstamp(0)
	//		tr.SetVersionstampedValue(key, versionstamp)
	//	}
	//
	//	return tr.GetVersionstamp(), nil
	//})

	if err != nil {
		return err
	}

	f.byRevision = kine.Sub("byRevision")
	f.byKeysAndRevisions = kine.Sub("byKeysAndRevisions")
	f.sequence = kine.Sub("sequence")

	//logrus.Tracef("Started")

	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := f.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}
	go f.ttl(ctx)

	return nil
}

func (l *FdbStructured) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("GET %s, rev=%d => rev=%d, kv=%v, err=%v", key, revision, revRet, kvRet != nil, errRet)
	}()

	rev, event, err := l.get(ctx, nil, key, rangeEnd, limit, revision, false)
	if event == nil {
		return rev, nil, err
	}
	return rev, event.KV, err
}

func (l *FdbStructured) get(ctx context.Context, tr *fdb.Transaction, key, rangeEnd string, limit, revision int64, includeDeletes bool) (int64, *server.Event, error) {
	rev, events, err := l.InnerList(ctx, tr, key, rangeEnd, limit, revision, includeDeletes)
	if err != nil {
		return 0, nil, err
	}
	if revision != 0 {
		rev = revision
	}
	if len(events) == 0 {
		return rev, nil, nil
	}
	return rev, events[0], nil
}

func (l *FdbStructured) adjustRevision(ctx context.Context, rev *int64) {
	if *rev != 0 {
		return
	}

	if newRev, err := l.CurrentRevision(ctx); err == nil {
		*rev = newRev
	}
}

func (l *FdbStructured) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	rev, err := l.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, prevEvent, err := l.get(ctx, &tr, key, "", 1, 0, true)
		if err != nil {
			return int64(0), err
		}
		createEvent := &server.Event{
			Create: true,
			KV: &server.KeyValue{
				Key:   key,
				Value: value,
				Lease: lease,
			},
			PrevKV: &server.KeyValue{
				ModRevision: rev,
			},
		}
		if prevEvent != nil {
			if !prevEvent.Delete {
				logrus.Tracef("ERR_KEY_EXISTS %s, prevEvent=%+v", key, prevEvent)
				return int64(0), server.ErrKeyExists
			}
			createEvent.PrevKV = prevEvent.KV
		}

		return l.Append(ctx, &tr, createEvent)
	})
	return rev.(int64), err
}

func (l *FdbStructured) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v", key, revision, revRet, kvRet != nil, deletedRet, errRet)
	}()

	type Result struct {
		revRet     int64
		kvRet      *server.KeyValue
		deletedRet bool
		errRet     error
	}

	res, _ := l.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, event, err := l.get(ctx, &tr, key, "", 1, 0, true)
		if err != nil {
			return &Result{0, nil, false, err}, nil
		}

		if event == nil {
			return &Result{rev, nil, true, nil}, nil
		}

		if event.Delete {
			return &Result{rev, event.KV, true, nil}, nil
		}

		if revision != 0 && event.KV.ModRevision != revision {
			return &Result{rev, event.KV, false, nil}, nil
		}

		deleteEvent := &server.Event{
			Delete: true,
			KV:     event.KV,
			PrevKV: event.KV,
		}

		rev, err = l.Append(ctx, &tr, deleteEvent)
		if err != nil {
			return &Result{0, nil, false, err}, nil
		}
		deleteEvent.KV.ModRevision = rev
		return &Result{rev, event.KV, true, err}, nil
	})
	castedRes := res.(*Result)

	return castedRes.revRet, castedRes.kvRet, castedRes.deletedRet, castedRes.errRet
}

func (l *FdbStructured) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	defer func() {
		logrus.Tracef("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%v, err=%v", prefix, startKey, limit, revision, revRet, kvRet, errRet)
	}()

	rev, events, err := l.InnerList(ctx, nil, prefix, startKey, limit, revision, false)
	if err != nil {
		return rev, nil, err
	}
	if revision == 0 && len(events) == 0 {
		// if no revision is requested and no events are returned, then
		// get the current revision and relist.  Relist is required because
		// between now and getting the current revision something could have
		// been created.
		currentRev, err := l.CurrentRevision(ctx)
		if err != nil {
			return currentRev, nil, err
		}
		return l.List(ctx, prefix, startKey, limit, currentRev)
	} else if revision != 0 {
		rev = revision
	}

	kvs := make([]*server.KeyValue, 0, len(events))
	for _, event := range events {
		kvs = append(kvs, event.KV)
	}
	return rev, kvs, nil
}

func (l *FdbStructured) Count(ctx context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	defer func() {
		logrus.Tracef("COUNT %s, rev=%d => rev=%d, count=%d, err=%v", prefix, revision, revRet, count, err)
	}()
	rev, count, err := l.InnerCount(ctx, prefix, startKey, revision)
	if err != nil {
		return 0, 0, err
	}

	if count == 0 {
		// if count is zero, then so is revision, so now get the current revision and re-count at that revision
		currentRev, err := l.CurrentRevision(ctx)
		if err != nil {
			return 0, 0, err
		}
		rev, rows, err := l.List(ctx, prefix, prefix, 1000, currentRev)
		return rev, int64(len(rows)), err
	}
	return rev, count, nil
}

func (l *FdbStructured) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, kvRev, updateRet, errRet)
	}()

	type Result struct {
		revRet    int64
		kvRet     *server.KeyValue
		updateRet bool
		errRet    error
	}

	res, _ := l.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		rev, event, err := l.get(ctx, &tr, key, "", 1, 0, false)
		if err != nil {
			return &Result{0, nil, false, err}, nil
		}

		if event == nil {
			return &Result{0, nil, false, nil}, nil
		}

		if event.KV.ModRevision != revision {
			return &Result{rev, event.KV, false, nil}, nil
		}

		updateEvent := &server.Event{
			KV: &server.KeyValue{
				Key:            key,
				CreateRevision: event.KV.CreateRevision,
				Value:          value,
				Lease:          lease,
			},
			PrevKV: event.KV,
		}

		rev, err = l.Append(ctx, &tr, updateEvent)
		if err != nil {
			return &Result{0, nil, false, err}, nil
		}

		updateEvent.KV.ModRevision = rev
		return &Result{rev, updateEvent.KV, true, err}, nil
	})

	castedRes := res.(*Result)

	return castedRes.revRet, castedRes.kvRet, castedRes.updateRet, castedRes.errRet
}

func (l *FdbStructured) ttl(ctx context.Context) {
	queue := workqueue.NewDelayingQueue()
	rwMutex := &sync.RWMutex{}
	ttlEventKVMap := make(map[string]*ttlEventKV)
	go func() {
		for l.handleTTLEvents(ctx, rwMutex, queue, ttlEventKVMap) {
		}
	}()

	for {
		select {
		case <-ctx.Done():
			queue.ShutDown()
			return
		default:
		}

		for event := range l.ttlEvents(ctx) {
			if event.Delete {
				continue
			}

			eventKV := loadTTLEventKV(rwMutex, ttlEventKVMap, event.KV.Key)
			if eventKV == nil {
				expires := storeTTLEventKV(rwMutex, ttlEventKVMap, event.KV)
				logrus.Tracef("TTL add event key=%v, modRev=%v, ttl=%v", event.KV.Key, event.KV.ModRevision, expires)
				queue.AddAfter(event.KV.Key, expires)
			} else {
				if event.KV.ModRevision > eventKV.modRevision {
					expires := storeTTLEventKV(rwMutex, ttlEventKVMap, event.KV)
					logrus.Tracef("TTL update event key=%v, modRev=%v, ttl=%v", event.KV.Key, event.KV.ModRevision, expires)
					queue.AddAfter(event.KV.Key, expires)
				}
			}
		}
	}
}

func (l *FdbStructured) handleTTLEvents(ctx context.Context, rwMutex *sync.RWMutex, queue workqueue.DelayingInterface, store map[string]*ttlEventKV) bool {
	key, shutdown := queue.Get()
	if shutdown {
		logrus.Info("TTL events work queue has shut down")
		return false
	}
	defer queue.Done(key)

	eventKV := loadTTLEventKV(rwMutex, store, key.(string))
	if eventKV == nil {
		logrus.Errorf("TTL event not found for key=%v", key)
		return true
	}

	if expires := time.Until(eventKV.expiredAt); expires > 0 {
		logrus.Tracef("TTL has not expired for key=%v, ttl=%v, requeuing", key, expires)
		queue.AddAfter(key, expires)
		return true
	}

	l.deleteTTLEvent(ctx, rwMutex, queue, store, eventKV)
	return true
}

func (l *FdbStructured) deleteTTLEvent(ctx context.Context, rwMutex *sync.RWMutex, queue workqueue.DelayingInterface, store map[string]*ttlEventKV, preEventKV *ttlEventKV) {
	logrus.Tracef("TTL delete key=%v, modRev=%v", preEventKV.key, preEventKV.modRevision)
	_, _, _, err := l.Delete(ctx, preEventKV.key, preEventKV.modRevision)

	rwMutex.Lock()
	defer rwMutex.Unlock()
	curEventKV := store[preEventKV.key]
	if expires := time.Until(preEventKV.expiredAt); expires > 0 {
		logrus.Tracef("TTL changed for key=%v, ttl=%v, requeuing", curEventKV.key, expires)
		queue.AddAfter(curEventKV.key, expires)
		return
	}
	if err != nil {
		logrus.Errorf("TTL delete trigger failed for key=%v: %v, requeuing", curEventKV.key, err)
		queue.AddAfter(curEventKV.key, retryInterval)
		return
	}

	delete(store, curEventKV.key)
}

// ttlEvents starts a goroutine to do a ListWatch on the root prefix. First it lists
// all non-deleted keys with a page size of 1000, then it starts watching at the
// revision returned by the initial list. Any keys that have a Lease associated with
// them are sent into the result channel for deferred handling of TTL expiration.
func (l *FdbStructured) ttlEvents(ctx context.Context) chan *server.Event {
	result := make(chan *server.Event)

	go func() {
		defer close(result)

		rev, events, err := l.InnerList(ctx, nil, "/", "", 1000, 0, false)
		for len(events) > 0 {
			if err != nil {
				logrus.Errorf("TTL event list failed: %v", err)
				return
			}

			for _, event := range events {
				if event.KV.Lease > 0 {
					result <- event
				}
			}

			_, events, err = l.InnerList(ctx, nil, "/", events[len(events)-1].KV.Key, 1000, rev, false)
		}

		wr := l.Watch(ctx, "/", rev)
		if wr.CompactRevision != 0 {
			logrus.Errorf("TTL event watch failed: %v", server.ErrCompacted)
			return
		}
		for events := range wr.Events {
			for _, event := range events {
				if event.KV.Lease > 0 {
					result <- event
				}
			}
		}
		logrus.Info("TTL events watch channel closed")
	}()

	return result
}

func loadTTLEventKV(rwMutex *sync.RWMutex, store map[string]*ttlEventKV, key string) *ttlEventKV {
	rwMutex.RLock()
	defer rwMutex.RUnlock()
	return store[key]
}

func storeTTLEventKV(rwMutex *sync.RWMutex, store map[string]*ttlEventKV, eventKV *server.KeyValue) time.Duration {
	rwMutex.Lock()
	defer rwMutex.Unlock()
	expires := time.Duration(eventKV.Lease) * time.Second
	store[eventKV.Key] = &ttlEventKV{
		key:         eventKV.Key,
		modRevision: eventKV.ModRevision,
		expiredAt:   time.Now().Add(expires),
	}
	return expires
}

func (l *FdbStructured) Watch(ctx context.Context, prefix string, revision int64) server.WatchResult {
	logrus.Tracef("WATCH %s, revision=%d", prefix, revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := l.InnerWatch(ctx, prefix)

	// include the current revision in list
	if revision > 0 {
		revision--
	}

	result := make(chan []*server.Event, 100)
	wr := server.WatchResult{Events: result}

	rev, kvs, err := l.After(ctx, prefix, revision, 0)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Errorf("Failed to list %s for revision %d: %v", prefix, revision, err)
		}
		if err == server.ErrCompacted {
			compact, _ := l.CompactRevision(ctx)
			wr.CompactRevision = compact
			wr.CurrentRevision = rev
		}
		cancel()
	}

	logrus.Tracef("WATCH LIST key=%s rev=%d => rev=%d kvs=%d", prefix, revision, rev, len(kvs))

	go func() {
		lastRevision := revision
		if len(kvs) > 0 {
			lastRevision = rev
		}

		if len(kvs) > 0 {
			result <- kvs
		}

		// always ensure we fully read the channel
		for i := range readChan {
			result <- filterWatch(i, lastRevision)
		}
		close(result)
		cancel()
	}()

	return wr
}

func filterWatch(events []*server.Event, rev int64) []*server.Event {
	for len(events) > 0 && events[0].KV.ModRevision <= rev {
		events = events[1:]
	}

	return events
}

func incrKey(tr fdb.Transaction, k fdb.KeyConvertible) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int64(1))
	if err != nil {
		return err
	}
	one := buf.Bytes()
	tr.Add(k, one)
	return nil
}

func getKey(tr fdb.Transaction, k fdb.KeyConvertible) (int64, error) {
	byteVal, err := tr.Get(k).Get()
	if err != nil {
		return 0, err
	}
	if byteVal == nil {
		return 0, nil
	}
	var numVal int64
	err = binary.Read(bytes.NewReader(byteVal), binary.LittleEndian, &numVal)
	if err != nil {
		return 0, err
	} else {
		return numVal, nil
	}
}

func (f *FdbStructured) CurrentRevision(ctx context.Context) (int64, error) {
	//if f.currentRev != 0 {
	//	return f.currentRev, nil
	//}
	key, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		return f.getCurrentRevision(tr)
	})
	return key.(int64), err
}

func (f *FdbStructured) getCurrentRevision(tr fdb.Transaction) (int64, error) {
	return getKey(tr, f.sequence)

	//values, err := tr.GetRange(f.byRevision, fdb.RangeOptions{Limit: 1, Reverse: true}).GetSliceWithError()
	//if err != nil {
	//	return int64(0), err
	//} else if len(values) == 0 {
	//	return int64(0), nil
	//} else if unpackedKey, err := f.byRevision.Unpack(values[0].Key); err != nil {
	//	return int64(0), err
	//} else {
	//	//versionstamp := unpackedKey[0].(tuple.Versionstamp)
	//	//return versionstampToInt64(&versionstamp), nil
	//	return unpackedKey[0].(int64), nil
	//}
}

func (f *FdbStructured) Append(ctx context.Context, tr *fdb.Transaction, event *server.Event) (retRev int64, retErr error) {
	// if strings.HasSuffix(event.KV.Key, "pod1") || strings.HasSuffix(event.KV.Key, "pod2") || strings.HasSuffix(event.KV.Key, "pod3") {
	// 	start := time.Now()
	// 	defer func() {
	// 		dur := time.Since(start)
	// 		logrus.Errorf("Append %s, duration=%s retRev=%d", event.KV.Key, dur, retRev)
	// 	}()
	// }

	record := eventToTuple(event).Pack()

	// revisionF, err := f.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
	//revision := tuple.IncompleteVersionstamp(0)
	err := incrKey(*tr, f.sequence)
	if err != nil {
		return 0, err
	}
	newRev, err := f.getCurrentRevision(*tr)
	//lastRev, err := f.getCurrentRevision(tr)
	if err != nil {
		return 0, err
	}
	//newRev := lastRev + 1
	tr.Set(f.byRevision.Pack(tuple.Tuple{newRev}), record)
	tr.Set(f.byKeysAndRevisions.Pack(tuple.Tuple{event.KV.Key, newRev}), record)

	//if revisionKey, err := f.byRevision.PackWithVersionstamp(tuple.Tuple{revision}); err != nil {
	//	return nil, err
	//} else {
	//	tr.SetVersionstampedKey(revisionKey, record)
	//}

	//if revisionKey, err := f.byKeysAndRevisions.PackWithVersionstamp(tuple.Tuple{event.KV.Key, revision}); err != nil {
	//	return nil, err
	//} else {
	//	tr.SetVersionstampedKey(revisionKey, record)
	//}

	//return tr.GetVersionstamp(), nil
	// return newRev, nil
	// })
	if err != nil {
		return 0, err
	}
	//revision, err := revisionF.(fdb.FutureKey).Get()
	//if err != nil {
	//	return 0, err
	//}
	//revisionInt64 := versionstampBytesToInt64(revision)
	// revisionInt64 := revisionF.(int64)
	revisionInt64 := newRev
	select {
	case f.notify <- revisionInt64:
	default:
	}
	//logrus.Tracef("Append end")
	return revisionInt64, err
}

type InFlight struct {
	revision int64
	f        fdb.FutureByteSlice
}

func (f *FdbStructured) InnerList(ctx context.Context, tr *fdb.Transaction, prefix, startKey string, limit, maxRevision int64, includeDeletes bool) (resRev int64, resEvents []*server.Event, resErr error) {
	// if strings.HasSuffix(prefix, "pod1") || strings.HasSuffix(prefix, "pod2") || strings.HasSuffix(prefix, "pod3") {
	// 	start := time.Now()
	// 	defer func() {
	// 		dur := time.Since(start)
	// 		logrus.Errorf("Inner List %s, maxRevision=%d, duration=%s, includeDeletes=%v, res=%v, resRev=%d", prefix, maxRevision, dur, includeDeletes, resEvents, resRev)
	// 	}()
	// }

	//  Examples:
	//  prefix=/bootstrap/, startKey=/bootstrap
	//  prefix=/bootstrap/abcd, startKey=/bootstrap/abcd
	//  prefix=/registry/secrets/, startKey=/registry/secrets/
	//  prefix=/registry/ranges/servicenodeports, startKey=""
	//  prefix=/, startKey=/registry/health
	//  prefix=/registry/podtemplates/chunking-6414/, startKey=/registry/podtemplates/chunking-6414/template-0016

	keyPrefix := f.byKeysAndRevisions.Pack(tuple.Tuple{prefix})
	if strings.HasSuffix(prefix, "/") {
		// Removing 0x00 from the string encoding in the tuple to have a prefixed search
		// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/2
		keyPrefix = keyPrefix[:len(keyPrefix)-1]

		// Searching for equality
		if prefix == startKey {
			startKey = ""
		}
	} else {
		// Searching for equality
		startKey = ""
	}

	exec := func(tr fdb.Transaction) (interface{}, error) {
		keysToRevisionsRange, err := fdb.PrefixRange(keyPrefix)
		if err != nil {
			return nil, err
		}

		it := tr.GetRange(keysToRevisionsRange, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		result := make([]*server.Event, 0, limit)

		prevKey := ""
		var candidateEvent *server.Event = nil
		var candidateRevision int64 = math.MinInt64
		for (int64(len(result)) < limit || limit == 0) && it.Advance() {
			newKey, newRevision, newEvent, err := f.getNextByKeyToRevisionEntry(it)
			if err != nil {
				return nil, err
			}

			if candidateEvent != nil && candidateEvent.KV.Key != newEvent.KV.Key {
				if !candidateEvent.Delete || includeDeletes {
					result = append(result, candidateEvent)
				}
				candidateEvent = nil
				candidateRevision = math.MinInt64
			}

			if (maxRevision == 0 || newRevision <= maxRevision) && newKey > startKey {
				if newRevision < candidateRevision {
					logrus.Errorf("SMALLER REVISION! %s, newKey=%s  newEvent=%s prevEvent=%s prevKey=%s , candidateRevision=%d, newRevisions=%d", prefix, newKey, prevKey, candidateEvent.KV.Key, newEvent.KV.Key, candidateRevision, newRevision)
				}
				candidateRevision = newRevision
				candidateEvent = newEvent
			}

			// prevKey = newKey
		}

		if candidateEvent != nil && (!candidateEvent.Delete || includeDeletes) {
			result = append(result, candidateEvent)
			//if newEvent.Delete && !includeDeletes {
			//	candidateEvent = nil
			//} else {
			//result = append(result, candidateEvent)
		}

		//inFlightGets := make([]*InFlight, 0, limit)
		//kv, err := it.Get()
		//if err != nil {
		//	return nil, err
		//}
		//revisionAndIsDelete, err := tuple.Unpack(kv.Value)
		//if err != nil {
		//	return nil, err
		//}
		//for i := len(revisionAndIsDelete) - 1; i >= 0; i -= 2 {
		//	prefixedRevision := revisionAndIsDelete[i-1].(fdb.Key)
		//	unpackedRevision, err := f.byRevision.Unpack(prefixedRevision)
		//	if err != nil {
		//		return nil, err
		//	}
		//	versionstamp := unpackedRevision[0].(tuple.Versionstamp)
		//	versionstampUint64 := versionstampToInt64(&versionstamp)
		//
		//	isDelete := revisionAndIsDelete[i].(bool)
		//	if versionstampUint64 <= maxRevisionUint64 && (includeDeletes || !isDelete && !includeDeletes) {
		//		inFlightGets = append(inFlightGets, &InFlight{
		//			lastRevision: int64(versionstampUint64),
		//			//f:        tr.Get(f.byRevision.Pack(tuple.Tuple{lastRevision})),
		//			f: tr.Get(prefixedRevision),
		//		})
		//		break
		//	}
		//}

		//for _, get := range inFlightGets {
		//	value, err := get.f.Get()
		//	if err != nil {
		//		return nil, err
		//	}
		//	event, err := parseEvent(value, get.revision)
		//	if err != nil {
		//		return nil, err
		//	}
		//	result = append(result, event)
		//}

		rev := int64(0)
		if maxRevision > 0 || len(result) != 0 {
			if rev, err = f.getCurrentRevision(tr); err != nil {
				return nil, err
			}
		}

		return &RevResult{currentRevision: rev, events: result}, nil
		//return result, nil
	}

	var result interface{}
	var err error
	if tr != nil {
		result, err = exec(*tr)
	} else {
		result, err = f.db.Transact(exec)
	}
	if err != nil {
		return 0, nil, err
	}

	revResult := result.(*RevResult)
	if maxRevision > revResult.currentRevision {
		return revResult.currentRevision, nil, server.ErrFutureRev
	}

	//events := result.([]*server.Event)
	//rev := int64(0)
	//if len(events) != 0 {
	//	rev = events[len(events)-1].KV.ModRevision
	//} else if maxRevision > 0 {
	//	if rev, err = f.CurrentRevision(ctx); err != nil {
	//		return 0, nil, err
	//	}
	//}

	select {
	case f.notify <- revResult.currentRevision:
	default:
	}
	return revResult.currentRevision, revResult.events, nil
}

func (f *FdbStructured) After(ctx context.Context, prefix string, minRevision, limit int64) (int64, []*server.Event, error) {
	//start := time.Now()
	//defer func() {
	//	dur := time.Since(start)
	//logrus.Tracef("After %s, minRevision=%d, duration=%s", prefix, minRevision, dur)
	//}()

	//keyRange, err := f.int64ToRevisionPrefixKeyRange(minRevision)
	//if err != nil {
	//	return 0, nil, err
	//}

	//begin := f.byRevision.Sub(int64ToVersionstamp(minRevision))
	begin := f.byRevision.Sub(minRevision)
	_, end := f.byRevision.FDBRangeKeys()

	// https://forums.foundationdb.org/t/ranges-without-explicit-end-go/773/11
	// https://forums.foundationdb.org/t/foundation-db-go-lang-pagination/1305/17
	// https://forums.foundationdb.org/t/cant-get-last-pair-in-fdbkeyvalue-array/1252/2
	selector := fdb.SelectorRange{
		Begin: fdb.FirstGreaterThan(begin),
		End:   fdb.FirstGreaterOrEqual(end),
	}

	result, err := f.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		it := tr.GetRange(selector, fdb.RangeOptions{Mode: fdb.StreamingModeIterator}).Iterator()

		result := make([]*server.Event, 0, limit)
		//println("here1", minRevision, limit)
		for (int64(len(result)) < limit || limit == 0) && it.Advance() {
			_, event, err := f.getNextByRevisionEntry(it)
			if err != nil {
				return nil, err
			}

			if doesEventHasPrefix(event, prefix) {
				result = append(result, event)
			}
		}

		//inFlightGets := make([]*InFlight, 0, limit)
		//for (int64(len(inFlightGets)) < limit || limit == 0) && it.Advance() {

		//event := tupleToEvent(key, &versionstamp, v)
		//event := tupleToEvent(versionstampUint64, v)
		//result = append(result, event)
		//}

		//logrus.Errorf("After res: %v", result)

		//kv, err := it.Get()
		//if err != nil {
		//	return nil, err
		//}
		//
		//revisionAndIsDelete, err := tuple.Unpack(kv.Value)
		//if err != nil {
		//	return nil, err
		//}
		//for i := 0; i < len(revisionAndIsDelete); i += 2 {
		//	prefixedRevision := revisionAndIsDelete[i].([]byte)
		//	unpackedRevision, err := f.byRevision.Unpack(fdb.Key(prefixedRevision))
		//	if err != nil {
		//		return nil, err
		//	}
		//	versionstamp := unpackedRevision[0].(tuple.Versionstamp)
		//	versionstampUint64 := versionstampToInt64(&versionstamp)
		//
		//	if versionstampUint64 > minRevisionUint64 {
		//		inFlightGets = append(inFlightGets, &InFlight{
		//			//revision: revision,
		//			//f:        tr.Get(f.byRevision.Pack(tuple.Tuple{revision})),
		//			revision: int64(versionstampUint64),
		//			//f:        tr.Get(f.byRevision.Pack(tuple.Tuple{revision})),
		//			f: tr.Get(fdb.Key(prefixedRevision)),
		//		})
		//	}
		//}

		//result := make([]*server.Event, 0, len(inFlightGets))
		//for _, get := range inFlightGets {
		//	value, err := get.f.Get()
		//	if err != nil {
		//		return nil, err
		//	}
		//	event, err := parseEvent(value, get.revision)
		//	if err != nil {
		//		return nil, err
		//	}
		//	result = append(result, event)
		//}

		//rev := int64(0)
		//if len(result) != 0 {
		//	rev = result[len(result)-1].KV.ModRevision
		//} else if minRevision > 0 {
		//	if rev, err = f.getCurrentRevision(tr); err != nil {
		//		return nil, err
		//	}
		//}
		//return &RevResult{currentRevision: rev, events: result}, nil
		//return result, nil

		resultRev := int64(0)
		if minRevision > 0 || len(result) != 0 {
			rev, err := f.getCurrentRevision(tr)
			if err != nil {
				return nil, err
			}
			resultRev = rev
		}

		return &RevResult{currentRevision: resultRev, events: result}, nil
	})
	if err != nil {
		return 0, nil, err
	}

	//events := result.([]*server.Event)
	//rev := int64(0)
	//if len(events) != 0 {
	//	rev = events[len(events)-1].KV.ModRevision
	//} else if minRevision > 0 {
	//	if rev, err = f.CurrentRevision(ctx); err != nil {
	//		return 0, nil, err
	//	}
	//}

	return result.(*RevResult).currentRevision, result.(*RevResult).events, err
	//return rev, events, err
}

func (f *FdbStructured) getNextByKeyToRevisionEntry(it *fdb.RangeIterator) (string, int64, *server.Event, error) {
	kv, err := it.Get()
	if err != nil {
		return "", 0, nil, err
	}
	k, err := f.byKeysAndRevisions.Unpack(kv.Key)
	if err != nil {
		return "", 0, nil, err
	}
	key := k[0].(string)
	versionstampInt64 := k[1].(int64)
	//versionstamp := k[1].(tuple.Versionstamp)
	//versionstampInt64 := versionstampToInt64(&versionstamp)
	unpackedTuple, err := tuple.Unpack(kv.Value)
	if err != nil {
		return "", 0, nil, err
	}
	event := tupleToEvent(versionstampInt64, unpackedTuple)
	return key, versionstampInt64, event, nil
}

func (f *FdbStructured) getNextByRevisionEntry(it *fdb.RangeIterator) ([]byte, *server.Event, error) {
	kv, err := it.Get()
	if err != nil {
		return nil, nil, err
	}
	k, err := f.byRevision.Unpack(kv.Key)
	if err != nil {
		return nil, nil, err
	}
	versionstampInt64 := k[0].(int64)
	//versionstamp := k[0].(tuple.Versionstamp)
	//versionstampInt64 := versionstampToInt64(&versionstamp)
	unpackedTuple, err := tuple.Unpack(kv.Value)
	if err != nil {
		return nil, nil, err
	}
	event := tupleToEvent(versionstampInt64, unpackedTuple)
	return kv.Key, event, nil
}

func tupleToEvent(versionstamp int64, t tuple.Tuple) *server.Event {
	event := &server.Event{
		Create: t[0].(bool),
		Delete: t[1].(bool),
		KV: &server.KeyValue{
			ModRevision:    versionstamp,
			Key:            t[2].(string),
			CreateRevision: t[3].(int64),
			Lease:          t[4].(int64),
			Value:          t[5].([]byte),
		},
		PrevKV: &server.KeyValue{
			ModRevision: t[6].(int64),
			Value:       t[7].([]byte),
		},
	}
	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	}
	return event
}

func eventToTuple(event *server.Event) tuple.Tuple {
	if event.KV == nil {
		event.KV = &server.KeyValue{}
	}
	if event.PrevKV == nil {
		event.PrevKV = &server.KeyValue{}
	}
	return tuple.Tuple{
		event.Create,
		event.Delete,
		event.KV.Key,
		event.KV.CreateRevision,
		event.KV.Lease,
		event.KV.Value,
		event.PrevKV.ModRevision,
		event.PrevKV.Value,
	}
}

func versionstampBytesToInt64(bytes []byte) int64 {
	return int64(binary.BigEndian.Uint64(bytes[:8]))
}

func versionstampToInt64(versionstamp *tuple.Versionstamp) int64 {
	return versionstampBytesToInt64(versionstamp.TransactionVersion[:8])
}

func int64ToVersionstamp(minRevision int64) tuple.Versionstamp {
	beginVersionstamp := tuple.Versionstamp{}
	binary.BigEndian.PutUint64(beginVersionstamp.TransactionVersion[:], uint64(minRevision))
	return beginVersionstamp
}

func (f *FdbStructured) InnerCount(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	rev, events, err := f.InnerList(ctx, nil, prefix, startKey, 0, revision, false)
	return rev, int64(len(events)), err
}

type RevResult struct {
	currentRevision int64
	events          []*server.Event
}

func (f *FdbStructured) InnerWatch(ctx context.Context, prefix string) <-chan []*server.Event {
	res := make(chan []*server.Event, 100)
	values, err := f.broadcaster.Subscribe(ctx, f.startWatch)
	if err != nil {
		return nil
	}

	go func() {
		defer close(res)
		for batch := range values {
			events := filter(batch.([]*server.Event), prefix)
			if len(events) > 0 {
				res <- events
			}
		}
	}()
	return res
}

func filter(events []*server.Event, prefix string) []*server.Event {
	filteredEventList := make([]*server.Event, 0, len(events))
	for _, event := range events {
		if doesEventHasPrefix(event, prefix) {
			filteredEventList = append(filteredEventList, event)
		}
	}
	return filteredEventList
}

func doesEventHasPrefix(event *server.Event, prefix string) bool {
	return (strings.HasSuffix(prefix, "/") && strings.HasPrefix(event.KV.Key, prefix)) ||
		event.KV.Key == prefix
}

var wId atomic.Uint64

func (f *FdbStructured) startWatch() (chan interface{}, error) {
	pollStart, err := f.CurrentRevision(nil)
	if err != nil {
		return nil, err
	}

	c := make(chan interface{})
	go f.poll(c, pollStart)
	return c, nil
}

func (f *FdbStructured) poll(result chan interface{}, pollStart int64) {
	//watchId := wId.Add(1)
	f.currentRev = pollStart

	lastBatchSize := math.MaxInt

	wait := time.NewTicker(1 * time.Second)
	defer wait.Stop()
	defer close(result)

	for {
		//logrus.Errorf("poll")
		if lastBatchSize < 100 {
			select {
			case <-f.ctx.Done():
				//logrus.Errorf("timeout")
				return
			case check := <-f.notify:
				if check <= f.currentRev {
					//logrus.Tracef("already received an update")
					continue
				}
			case <-wait.C:
				//logrus.Tracef("Sleeping")
			}
		}

		_, events, err := f.After(f.ctx, "/", f.currentRev, 500)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logrus.Errorf("fail to list latest changes: %v", err)
			}
			continue
		}

		//logrus.Tracef("POLL AFTER watchId=%d, currentRev=%d, events=%d, start=%d, lastSeenRevision=%d", watchId, f.currentRev, len(events), pollStart, lastSeenRevision)

		if len(events) == 0 {
			continue
		}

		f.currentRev = events[len(events)-1].KV.ModRevision
		//logrus.Errorf("poll push events")
		//f.currentRev = newRevision
		lastBatchSize = len(events)
		result <- events
		//logrus.Errorf("poll pushed events")
	}
}

func (f *FdbStructured) DbSize(ctx context.Context) (int64, error) {
	result, err := f.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.GetEstimatedRangeSizeBytes(f.kine).Get()
	})
	return result.(int64), err
}

// CompactRevision returns the oldest revision
func (f *FdbStructured) CompactRevision(ctx context.Context) (int64, error) {
	return 0, nil
}

func (f *FdbStructured) Compact(ctx context.Context, revision int64) (int64, error) {
	return 0, nil
}
