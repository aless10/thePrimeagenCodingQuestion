"""
Async request queue

1. Queue that returns a promise
2. hands it to a promise factory
3. when the promise factory is called, the task gets executed
4. limit of 3 tasks running at a time
"""

import asyncio
import random
from collections import deque


class TaskQueue:

    def __init__(self):
        self._queue = deque()
        self._active_tasks = 0
        self._concurrent_task_limit = 3

    def __str__(self):
        return f"TaskQueue len={len(self._queue)}, active_tasks={self._active_tasks}"

    async def try_to_run_task(self):
        if self._active_tasks >= self._concurrent_task_limit or len(self._queue) == 0:
            return
        else:
            task = self.deque()
            await task()

    async def enqueue(self, _promise_factory):
        async def task():
            self._active_tasks += 1
            try:
                await _promise_factory()
            finally:
                self._active_tasks -= 1
                await self.try_to_run_task()

        if self._active_tasks < self._concurrent_task_limit:
            await task()
        else:
            self._queue.append(task)

    def deque(self):
        return self._queue.popleft()


def promise_factory(n):
    async def promise():
        number = random.randrange(1, 5)
        print(f"Start promise {n}")
        await asyncio.sleep(number)
        print(f"End promise {n}: took {number} seconds")

    return promise


async def main():
    q = TaskQueue()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(q.enqueue(promise_factory(1)))
        tg.create_task(q.enqueue(promise_factory(2)))
        tg.create_task(q.enqueue(promise_factory(3)))
        tg.create_task(q.enqueue(promise_factory(4)))
        tg.create_task(q.enqueue(promise_factory(5)))
        tg.create_task(q.enqueue(promise_factory(6)))
        tg.create_task(q.enqueue(promise_factory(7)))
        tg.create_task(q.enqueue(promise_factory(8)))
        tg.create_task(q.enqueue(promise_factory(9)))


if __name__ == '__main__':
    asyncio.run(main())
