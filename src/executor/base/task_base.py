import asyncio
import json

from utils import *


class EmittedExecutorBase(object):
    def __init__(self, request, **middlewares):
        self.request = request
        self.middlewares = middlewares

    async def main_logic(self):
        ...

    def __getattr__(self, name):
        return self.middlewares.get(name, self.__nonsense)

    async def __nonsense(self, *args, **kwargs):
        ...


class TaskRunnerBase(object):
    __task_matcher_prefix__ = "<task_xxx>"  # list in redis
    __task_params_prefix__ = "<params_xxx>"  # k-v in redis
    __task_status_prefix__ = "<status_xxx>"  # k-v in redis
    __task_streams_prefix__ = "<streams_xxx>"  # stream in redis
    __task_results_prefix__ = "<results_xxx>"  # k-v in redis
    __name__ = "<task runner name>"
    __doc__ = "<task runner documetation>"

    def __init__(self, executor_cls, **middlewares):
        self.executor_cls = executor_cls
        self.middlewares = middlewares
        self.tasks = set()

    def __getattr__(self, name):
        return self.middlewares.get(name, self.__nonsense)

    async def __nonsense(self, *args, **kwargs):
        ...

    async def fetch_and_emit_dedicated_tasks(self, dedicated_task_uuids):
        redis_utils: RedisUtils = self.redis_utils
        logger: Logger = self.logger
        if len(dedicated_task_uuids) == 0:
            return
        await logger.info("{} | loading batched tasks...", Styled(f">>{self.__name__}", Styles.GREEN_BG))
        for task_uuid in dedicated_task_uuids:
            await asyncio.sleep(0)
            await logger.info("{} {} | appending task...", Styled(f">>{self.__name__}", Styles.GREEN_BG), Styled(task_uuid, Styles.CYAN))
            task_params = await redis_utils.get(f"{self.__task_params_prefix__}:{task_uuid}")
            task_params = json.loads(task_params)
            task_params = {
                "base": {
                    "task_uuid": task_uuid,
                    "task_status_key": f"{self.__task_status_prefix__}:{task_uuid}",
                    "task_results_key": f"{self.__task_results_prefix__}:{task_uuid}",
                    "task_streams_key": f"{self.__task_streams_prefix__}:{task_uuid}"
                },
                "body": task_params
            }
            await redis_utils.delete(f"{self.__task_params_prefix__}:{task_uuid}")
            exec_cls = self.executor_cls(task_params, **self.middlewares)
            task = asyncio.create_task(exec_cls.main_logic())
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)
        await logger.info("{} | batched tasks appended!", Styled(f">>{self.__name__}", Styles.GREEN_BG))
