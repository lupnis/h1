import asyncio
from typing import List

from base import TaskRunnerBase


class ExecutorLogicChain:
    def __init__(self, *runners, **middlewares):
        self.middlewares = middlewares
        self.runners: List[TaskRunnerBase] = list(runners)

    def __getattr__(self, name):
        return self.middlewares.get(name, self.__nonsense)

    async def __nonsense(self, *args, **kwargs):
        ...

    async def main_loop(self):
        single_task = await self.redis_utils.lpop("queued_tasks")
        task_dict = {}
        while single_task:
            await asyncio.sleep(1)
            task_prefix, task_uuid = single_task.split(":")
            task_dict[task_prefix] = task_dict.get(task_prefix, [])
            task_dict[task_prefix].append(task_uuid)
            single_task = await self.redis_utils.lpop("queued_tasks")
        for task_runner in self.runners:
            await task_runner.fetch_and_emit_dedicated_tasks(task_dict.get(task_runner.__task_matcher_prefix__, []))
