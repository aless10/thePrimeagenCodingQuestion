package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"
)

type TaskQueue struct {
	channel  chan int
	queue    chan int
	capacity int
	tasks    int
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
	if len(tq.channel) == tq.capacity {
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

	for i := 1; i < tq.tasks+1; i++ {
		tq.enqueue(i)
	}

	for len(tq.channel) != 0 || len(tq.queue) != 0 {
		go tq.tryToRunTask()
	}

}
func newTaskQueue(tasks, capacity int) *TaskQueue {
	return &TaskQueue{
		channel:  make(chan int, capacity),
		queue:    make(chan int, 100),
		capacity: capacity,
		tasks:    tasks,
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

	tasks := flag.Int("tasks", 10, "Number of tasks to run")
	capacity := flag.Int("capacity", 3, "Task capacity")
	flag.Parse() // parse the flags

	q := newTaskQueue(*tasks, *capacity)
	q.run()
}
