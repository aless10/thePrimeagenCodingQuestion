package main

import (
	"fmt"
	"math/rand"
	"time"
)

type TaskQueue struct {
	queue chan int
}

func (tq *TaskQueue) enqueue(n int) {
	tq.queue <- n
	go promise_factory(n)

}

func newTaskQueue() *TaskQueue {
	return &TaskQueue{
		queue: make(chan int, 10),
	}
}

func promise_factory(n int) {
	number := rand.Intn(n) + 1
	fmt.Println("Start promise", n)
	time.Sleep(time.Duration(number) * time.Second)
	fmt.Println("End promise", n, ". Took", number, "seconds")
}

func main() {
	q := newTaskQueue()

	for i := 1; i < 10; i++ {
		q.enqueue(i)
	}

	for item := range q.queue {
		fmt.Println(item)
	}
}
