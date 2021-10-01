package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	que := newQueue()

	clients := 4
	var wg sync.WaitGroup
	wg.Add(clients)

	// start consumer
	go func() {
		for !que.Closed() {
			// que.TakeAll()
			objs := que.TakeAll()
			fmt.Printf("take size: %d\n", len(objs))
		}
	}()

	// start producer
	for i := 0; i < clients; i++ {
		go func(id int) {
			for j := 0; j < 10000; j++ {
				que.Append(fmt.Sprintf("hello %d: %d", id, time.Now().UnixNano()))
				// time.Sleep(500 * time.Microsecond)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	que.Close()
}
