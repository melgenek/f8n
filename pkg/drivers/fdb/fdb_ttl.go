package fdb

import (
	"context"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/util/workqueue"
	"sync"
	"time"
)

// All TTL is copy-pasted from the logstructured.go

const (
	retryInterval = 250 * time.Millisecond
)

type ttlEventKV struct {
	key         string
	modRevision int64
	expiredAt   time.Time
}

func (f *FDB) ttl(ctx context.Context) {
	logrus.Info("Starting TTL")

	queue := workqueue.NewDelayingQueue()
	rwMutex := &sync.RWMutex{}
	ttlEventKVMap := make(map[string]*ttlEventKV)
	go func() {
		f.backgroundReadWg.Add(1)
		defer func() {
			logrus.Warn("Done1")
			f.backgroundReadWg.Done()
		}()
		for f.handleTTLEvents(ctx, rwMutex, queue, ttlEventKVMap) {
		}
	}()

	f.backgroundReadWg.Add(1)
	defer func() {
		logrus.Warn("Done2")
		f.backgroundReadWg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			queue.ShutDown()
			return
		default:
		}

		for event := range f.ttlEvents(ctx) {
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

func (f *FDB) handleTTLEvents(ctx context.Context, rwMutex *sync.RWMutex, queue workqueue.DelayingInterface, store map[string]*ttlEventKV) bool {
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

	f.deleteTTLEvent(ctx, rwMutex, queue, store, eventKV)
	return true
}

func (f *FDB) deleteTTLEvent(ctx context.Context, rwMutex *sync.RWMutex, queue workqueue.DelayingInterface, store map[string]*ttlEventKV, preEventKV *ttlEventKV) {
	logrus.Tracef("TTL delete key=%v, modRev=%v", preEventKV.key, preEventKV.modRevision)
	_, _, _, err := f.Delete(ctx, preEventKV.key, preEventKV.modRevision)

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
// revision returned by the initial listKeyValue. Any keys that have a Lease associated with
// them are sent into the result channel for deferred handling of TTL expiration.
func (f *FDB) ttlEvents(ctx context.Context) chan *server.Event {
	result := make(chan *server.Event)

	go func() {
		defer close(result)

		collector := newListCollector(f, 1000, true)
		rev, err := f.listWithCollector("ttl1", "/", "", 0, collector)
		revRecords := collector.records
		for len(revRecords) > 0 {
			if err != nil {
				logrus.Errorf("TTL events listKeyValue failed: %v", err)
				return
			}

			for _, revRecord := range revRecords {
				if revRecord.Record.Lease > 0 {
					result <- revRecordToEvent(revRecord)
				}
			}

			collector = newListCollector(f, 1000, true)
			_, err = f.listWithCollector("ttl2", "/", revRecords[len(revRecords)-1].Record.Key, rev, collector)
			revRecords = collector.records
		}

		wr := f.Watch(ctx, "/", rev)
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
