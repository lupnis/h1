from copy import deepcopy
import traceback

from base import *
from utils import *


class EmittedExecutor(EmittedExecutorBase):
    __name__ = "example request task"
    __doc__ = "description of example request task"

    def __init__(self, request, **middlewares):
        super(EmittedExecutor, self).__init__(request, **middlewares)
        self.request = request
        self.result_context = {}

    async def step1(self):  # pending -> processing
        await self.logger.info("{} {} | task initiating...", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN))
        await self.redis_utils.rpush(self.request["base"]["task_streams_key"], stream_chunk(step=1, step_info="task initiating..."))
        ...
        await self.logger.info("{} {} | task initiated!", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN))
        await self.redis_utils.rpush(self.request["base"]["task_streams_key"], stream_chunk(step=1, step_info="task initiated!"))
        await self.redis_utils.set(self.request["base"]["task_results_key"], self.result_context)

    async def step2(self):
        await self.logger.info("{} {} | applying data...", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN))
        await self.redis_utils.rpush(self.request["base"]["task_streams_key"], stream_chunk(step=2, step_info="applying data..."))
        await self.redis_utils.set(self.request["base"]["task_results_key"], self.result_context)
        ...
        self.result_context = {
            "name": self.request["body"]["name"],
            "age": self.request["body"]["age"],
            "hobbies": self.request["body"]["hobbies"],
            "details": self.request["body"]["details"]
        }
        ...
        await self.logger.info("{} {} | data applied!", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN))
        await self.redis_utils.rpush(self.request["base"]["task_streams_key"], stream_chunk(step=2, step_info="data applied!", data=self.result_context))
        await self.redis_utils.set(self.request["base"]["task_results_key"], self.result_context)

    async def main_logic(self):
        try:
            await self.logger.info("{} {} | updating task status...", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN))
            await self.redis_utils.rpush(self.request["base"]["task_streams_key"], stream_chunk(step=1, step_info="updating task status..."))
            await self.redis_utils.set(self.request["base"]["task_status_key"], "processing")
            await self.redis_utils.rpush(self.request["base"]["task_streams_key"], stream_chunk(step=1, step_info="task status updated!"))
            await self.step1()
            await self.step2()
            await self.step3()
        except Exception as e:
            await self.logger.warning("{} {} | task failed!", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN))
            await self.logger.warning("{} {} | exception: {}", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN), Styled(str(e), Styles.YELLOW))
            await self.logger.warning("{} {} | trace: {}", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN), Styled(traceback.format_exc(), Styles.YELLOW))
            await self.redis_utils.rpush(self.request["base"]["task_streams_key"], stream_chunk(step=-1, step_info="task failed", finish_reason="fail"))
            await self.redis_utils.set(self.request["base"]["task_status_key"], "failed")
        else:
            await self.logger.info("{} {} | task accomplished!", Styled(self.__name__, Styles.MAGENTA_BG), Styled(self.request["base"]["task_uuid"], Styles.CYAN))
            await self.redis_utils.rpush(self.request["base"]["task_streams_key"], stream_chunk(step=3, step_info="task accomplished!", finish_reason="stop"))
            await self.redis_utils.set(self.request["base"]["task_status_key"], "done")
        finally:
            await self.redis_utils.expire(self.request["base"]["task_results_key"], 24 * 60 * 60)
            await self.redis_utils.expire(self.request["base"]["task_streams_key"], 24 * 60 * 60)
            await self.redis_utils.expire(self.request["base"]["task_status_key"], 24 * 60 * 60)


class TaskRunner(TaskRunnerBase):
    __task_matcher_prefix__ = "task_example_request"  # val in redis queued_tasks list
    __task_params_prefix__ = "params_example_request"  # k-v in redis
    __task_status_prefix__ = "status_example_request"  # k-v in redis
    __task_streams_prefix__ = "streams_example_request"  # stream in redis
    __task_results_prefix__ = "results_example_request"  # k-v in redis
    __name__ = "example request task"
    __doc__ = "description of example request task"

    def __init__(self, **middlewares):
        super(TaskRunner, self).__init__(EmittedExecutor, **middlewares)
