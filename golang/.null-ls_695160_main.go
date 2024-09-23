package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type TaskQueue struct {
	queue        chan int
	active_tasks int
	wg           sync.WaitGroup
}

func (tq *TaskQueue) enqueue(n int) {
	tq.wg.Add(1)
	go promise_factory(n, tq)

}

func newTaskQueue() *TaskQueue {
	return &TaskQueue{
		queue: make(chan int, 3),
		wg:    sync.WaitGroup{},
	}
}

func promise_factory(n int, tq *TaskQueue) {
	defer tq.wg.Done()
	number := rand.Intn(n)
	fmt.Println("Start promise", n)
	time.Sleep(time.Duration(number) * time.Second)
	fmt.Println("End promise", n, ". Took", number, "seconds")
	tq.queue <- n
}

func main() {
	q := newTaskQueue()

	for i := 0; i < 10; i++ {
		q.enqueue(i)
	}
}
