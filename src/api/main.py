from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware
import uuid
from fastapi import FastAPI, Request
import traceback

from core import *
from utils import *


def App():
    config_path = "configs.json"
    config_handler = AsyncConfig(config_path)
    middlewares = {
        # aioredis may crash sometimes if not recreated
        "redis_utils": RedisUtils(**config_handler.config["redis_utils"])
    }
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"]
    )

    @app.post("/api/v1/submit_example_request")
    async def v1_submit_example_request(data: SubmitExampleRequestBody):
        return StreamingResponse(_v1_submit_example_request(data), media_type="text/event-stream")

    @app.get("/api/v1/status_example_request")
    async def v1_status_example_request(uuid: Optional[str] = None):
        return StreamingResponse(_v1_status_example_request(uuid), media_type="text/event-stream")

    @app.get("/v1/fetch_example_request")
    async def v1_etch_example_request(uuid: Optional[str] = None):
        return StreamingResponse(_v1_etch_example_request(uuid), media_type="text/event-stream")

    async def _v1_submit_example_request(data: SubmitExampleRequestBody):
        middlewares["redis_utils"] = RedisUtils(
            **config_handler.config["redis_utils"])
        yield stream_chunk(step_info="submitting example request task...")
        task_uuid = str(uuid.uuid4())
        task = f"task_example_request:{task_uuid}"
        task_params = {
            "name": data.name,
            "age": data.age,
            "hobbies": data.hobbies,
            "details": data.details
        }
        params_key = f"params_example_request:{task_uuid}"
        status_key = f"status_example_request:{task_uuid}"

        await middlewares["redis_utils"].rpush("queued_tasks", task)
        await middlewares["redis_utils"].set(params_key, json.dumps(task_params))
        await middlewares["redis_utils"].set(status_key, "pending")
        yield stream_chunk(step_info="task created", data={"uuid": task_uuid})
        yield stream_chunk(finish_reason='stop')
        yield stream_end()

    async def _v1_status_example_request(uuid: Optional[str] = None):
        middlewares["redis_utils"] = RedisUtils(
            **config_handler.config["redis_utils"])
        status_key = f"status_example_request:{uuid}"
        status = await middlewares["redis_utils"].get(status_key)
        if status is None:
            yield stream_chunk(step_info="task not exist")
            yield stream_chunk(finish_reason='stop')
            yield stream_end()
            return
        yield stream_chunk(step_info="get task status succeeded", data={"status": status})
        yield stream_chunk(finish_reason='stop')
        yield stream_end()

    async def _v1_etch_example_request(uuid: Optional[str] = None):
        middlewares["redis_utils"] = RedisUtils(
            **config_handler.config["redis_utils"])
        streams_key = f"streams_example_request:{uuid}"
        status_key = f"status_example_request:{uuid}"
        results_key = f"results_example_request:{uuid}"

        async def fetch_till_stream_empty():
            data_chunk = await middlewares["redis_utils"].lpop(streams_key)
            while data_chunk is not None:
                yield data_chunk + "\n\n"
                data_chunk = await middlewares["redis_utils"].lpop(streams_key)
                await asyncio.sleep(0)

        status = await middlewares["redis_utils"].get(status_key)
        if status is None:
            yield stream_chunk(step_info="task not exist")
            yield stream_chunk(finish_reason='stop')
            yield stream_end()
            return
        if status == "done":
            results = await middlewares["redis_utils"].get(results_key)
            yield stream_chunk(step_info="task accomplished", data=json.loads(results) if results is not None else None)
            yield stream_chunk(finish_reason='stop')
            yield stream_end()
            return
        if status == "failed":
            results = await middlewares["redis_utils"].get(results_key)
            yield stream_chunk(step_info="task failed", data=json.loads(results) if results is not None else None)
            yield stream_chunk(finish_reason='stop')
            yield stream_end()
            return
        if status == "pending":
            yield stream_chunk(step_info="task pending")
            yield stream_chunk(finish_reason='stop')
            yield stream_end()
            return

        results = await middlewares["redis_utils"].get(results_key)
        yield stream_chunk(step_info="partial result", data=json.loads(results) if results is not None else None)
        while True:
            await asyncio.sleep(0)
            status = await middlewares["redis_utils"].get(status_key)
            if status is None:
                yield stream_chunk(step=6, step_info="task not exist")
                yield stream_chunk(finish_reason='stop')
                yield stream_end()
                return
            if status == "done":
                results = await middlewares["redis_utils"].get(results_key)
                async for chunk in fetch_till_stream_empty():
                    yield chunk
                yield stream_chunk(step=6, step_info="task accomplished", data=json.loads(results) if results is not None else None)
                yield stream_chunk(finish_reason='stop')
                yield stream_end()
                return
            if status == "failed":
                async for chunk in fetch_till_stream_empty():
                    yield chunk
                results = await middlewares["redis_utils"].get(results_key)
                yield stream_chunk(step=6, step_info="task failed", data=json.loads(results) if results is not None else None)
                yield stream_chunk(finish_reason='stop')
                yield stream_end()
                return
            if status == "pending":
                yield stream_chunk(step=6, step_info="task pending")
                yield stream_chunk(finish_reason='stop')
                yield stream_end()
                return

            async for chunk in fetch_till_stream_empty():
                yield chunk
                await asyncio.sleep(0)

    return app


app = App()
