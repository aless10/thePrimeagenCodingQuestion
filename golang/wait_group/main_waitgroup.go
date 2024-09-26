package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type TaskQueue struct {
	wg           sync.WaitGroup
	active_tasks int
	capacity     int
	tasks        int
	queue        []int
}

func (tq *TaskQueue) enqueue(n int) {

	if tq.active_tasks < tq.capacity {
		tq.increaseActiveTasks()
		go promise_factory(n, tq)
	} else {
		tq.queue = append(tq.queue, n)
	}
}

func (tq *TaskQueue) tryToRunTask() {
	if tq.active_tasks == tq.capacity {
		return
	}
	n := tq.dequeue()
	tq.increaseActiveTasks()
	go promise_factory(n, tq)
}

func (tq *TaskQueue) dequeue() int {
	var n int
	n, tq.queue = tq.queue[0], tq.queue[1:]
	return n
}

func (tq *TaskQueue) run() {

	for i := 1; i < tq.tasks+1; i++ {
		tq.wg.Add(1)
		tq.enqueue(i)
	}

	for len(tq.queue) != 0 {
		tq.tryToRunTask()
	}
	tq.wg.Wait()
}

func (tq *TaskQueue) decreaseActiveTasks() {
	tq.active_tasks -= 1
}

func (tq *TaskQueue) increaseActiveTasks() {
	tq.active_tasks += 1
}

func newTaskQueue(tasks, capacity int) *TaskQueue {
	return &TaskQueue{
		wg:       sync.WaitGroup{},
		queue:    make([]int, 0, tasks-capacity),
		capacity: capacity,
		tasks:    tasks,
	}
}

func promise_factory(n int, tq *TaskQueue) {
	defer tq.wg.Done()
	defer tq.decreaseActiveTasks()
	number := rand.Intn(n) + 1
	fmt.Println("Start promise", n)
	time.Sleep(time.Duration(number) * time.Second)
	fmt.Println("End promise", n, "Took", number, "seconds")
}

func main() {

	tasks := flag.Int("tasks", 10, "Number of tasks to run")
	capacity := flag.Int("capacity", 3, "Task capacity")
	flag.Parse() // parse the flags

	q := newTaskQueue(*tasks, *capacity)
	q.run()
}
