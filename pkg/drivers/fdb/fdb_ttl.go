package fdb

import (
	"context"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/util/workqueue"
	"sync"
	"time"
)

// All TTL is purely copy-pasted from the logstructured.go

func (l *FDB) ttl(ctx context.Context) {
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

func (l *FDB) handleTTLEvents(ctx context.Context, rwMutex *sync.RWMutex, queue workqueue.DelayingInterface, store map[string]*ttlEventKV) bool {
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

func (l *FDB) deleteTTLEvent(ctx context.Context, rwMutex *sync.RWMutex, queue workqueue.DelayingInterface, store map[string]*ttlEventKV, preEventKV *ttlEventKV) {
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
func (l *FDB) ttlEvents(ctx context.Context) chan *server.Event {
	result := make(chan *server.Event)

	go func() {
		defer close(result)

		rev, events, err := l.list(nil, "/", "", 1000, 0, false)
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

			_, events, err = l.list(nil, "/", events[len(events)-1].KV.Key, 1000, rev, false)
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
