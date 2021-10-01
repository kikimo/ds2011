package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type BlockQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	que    []interface{}
	closed int32
}

func (q *BlockQueue) Append(e interface{}) {
	q.mu.Lock()
	q.que = append(q.que, e)
	sz := len(q.que)
	q.mu.Unlock()

	go func() {
		if sz < 2 {
			time.Sleep(512 * time.Microsecond)
		}
		q.cond.Broadcast()
	}()
}

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
	fmt.Printf("queue closing...\n")
	q.mu.Lock()
	atomic.StoreInt32(&q.closed, 1)
	q.mu.Unlock()
	q.cond.Broadcast()
	fmt.Printf("queueu closed!\n")
}

func newQueue() *BlockQueue {
	que := &BlockQueue{
		closed: 0,
	}
	que.cond = sync.NewCond(&que.mu)

	return que
}
