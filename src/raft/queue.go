package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	QueueOpen   = 0
	QueueClosed = 1
)

type BlockQueue struct {
	mu           sync.Mutex
	cond         *sync.Cond
	que          []interface{}
	closed       int32
	closeHandler CloseHandler
}

func (q *BlockQueue) Append(e interface{}) bool {
	q.mu.Lock()
	if atomic.LoadInt32(&q.closed) == QueueClosed {
		q.mu.Unlock()
		return false
	}

	q.que = append(q.que, e)
	sz := len(q.que)
	q.mu.Unlock()

	go func() {
		if sz < 2 {
			time.Sleep(512 * time.Microsecond)
		}
		q.cond.Broadcast()
	}()

	return true
}

type CloseHandler func(o interface{})

func (q *BlockQueue) Closed() bool {
	return atomic.LoadInt32(&q.closed) == 1
}

// TODO with timeout? how to?
func (q *BlockQueue) TakeAll() []interface{} {
	q.mu.Lock()
	for len(q.que) == 0 && !q.Closed() {
		// fmt.Printf("queue waiting\n")
		q.cond.Wait()
	}

	// fmt.Printf("all size: %d\n", len(q.que))
	ret := q.que
	q.que = nil
	q.mu.Unlock()

	return ret
}

func (q *BlockQueue) Close() {
	DPrintf("queue closing...\n")
	q.mu.Lock()
	atomic.StoreInt32(&q.closed, QueueClosed)
	// invoke callback
	if q.closeHandler != nil {
		for i := range q.que {
			o := q.que[i]
			q.closeHandler(o)
		}
	}
	q.que = nil
	q.mu.Unlock()
	q.cond.Broadcast()
	DPrintf("queueu closed!\n")
}

func newQueue(h CloseHandler) *BlockQueue {
	que := &BlockQueue{
		closed:       QueueOpen,
		closeHandler: h,
	}
	que.cond = sync.NewCond(&que.mu)

	return que
}
