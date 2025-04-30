from core import ExecutorLogicChain

# import tasks here
import tasks.ExampleRequestTask as ExampleRequestTask


from utils import *
import argparse
from concurrent.futures import ProcessPoolExecutor


def parse_args():
    parser = argparse.ArgumentParser(description="Executor Node")
    parser.add_argument("--config", type=str,
                        default="configs.json", help="Path to the config file")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of workers")
    return parser.parse_args()


async def main(config_path="configs.json", worker_id=None):
    print("[---------- --:--:--] - initiating executor node...")
    config_handler = AsyncConfig(config_path)
    middlewares = {
        "logger": Logger(LoggerConfig.DEFAULT_CONFIG, **config_handler.config["logger"]),
        "redis_utils": RedisUtils(**config_handler.config["redis_utils"])
    }
    registered_tasks = [
        ExampleRequestTask.TaskRunner(**middlewares),
        ...  # also add tasks here
    ]
    logic_chain = ExecutorLogicChain(*registered_tasks, **middlewares)
    await middlewares["logger"].notice("executor node initiated!")
    while True:
        await logic_chain.main_loop()
        await asyncio.sleep(1)


def main_emitter(config_path="configs.json", worker_id=None):
    asyncio.run(main(config_path, worker_id))


if __name__ == "__main__":
    args = parse_args()
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for i in range(args.workers):
            executor.submit(main_emitter, args.config, i)
