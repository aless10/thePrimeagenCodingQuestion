package main

import (
	"fmt"
	"math/rand"
	"time"
)

type TaskQueue struct {
	channel chan int
	queue   chan int
}

func (tq *TaskQueue) enqueue(n int) {
	select {
	case tq.channel <- n:
		go promise_factory(n, tq)
	default:
		tq.queue <- n
	}
}

func (tq *TaskQueue) tryToRunTask() {
	if len(tq.channel) == 3 {
		return
	}
	n := tq.dequeue()
	tq.channel <- n
	go promise_factory(n, tq)
}

func (tq *TaskQueue) dequeue() int {
	return <-tq.queue
}

func (tq *TaskQueue) run() {

	for i := 1; i < 10; i++ {
		tq.enqueue(i)
	}

	for len(tq.channel) != 0 || len(tq.queue) != 0 {
		go tq.tryToRunTask()
	}

}
func newTaskQueue() *TaskQueue {
	return &TaskQueue{
		channel: make(chan int, 3),
		queue:   make(chan int, 100),
	}
}

func promise_factory(n int, tq *TaskQueue) {
	number := rand.Intn(n) + 1
	fmt.Println("Start promise", n)
	time.Sleep(time.Duration(number) * time.Second)
	fmt.Println("End promise", n, "Took", number, "seconds")
	<-tq.channel
}

func main() {
	q := newTaskQueue()

	q.run()
}
