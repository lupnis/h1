import os
import json
import asyncio
import aiofiles


class AsyncConfig:
    def __init__(self, file_path, template: dict = {}):
        self.file_path = file_path
        self.template = template
        self.config = self.load_sync()

    def load_sync(self) -> dict:
        if not os.path.exists(self.file_path):
            config = self.template.copy()
            self.write_config_sync(config)
        else:
            try:
                with open(self.file_path, 'r') as f:
                    config = json.load(f)
            except (json.JSONDecodeError, IOError):
                # If file is corrupted or unreadable, reinitialize with template
                config = self.template.copy()
                self.write_config_sync(config)
        return config

    def write_config_sync(self, config: dict) -> None:
        self.config = config
        with open(self.file_path, 'w') as f:
            json.dump(config, f, indent=4)

    async def load(self) -> dict:
        if not os.path.exists(self.file_path):
            self.config = self.template.copy()
            await self.write_config(self.config)
        else:
            async with aiofiles.open(self.file_path, mode='r') as f:
                content = await f.read()
                try:
                    self.config = json.loads(content)
                except json.JSONDecodeError:
                    self.config = self.template.copy()
                    await self.write_config(self.config)
        return self.config

    async def write_config(self, config: dict) -> None:
        self.config = config
        async with aiofiles.open(self.file_path, mode='w') as f:
            await f.write(json.dumps(config, indent=4))

    async def update_config(self, key: str, value) -> None:
        if self.config is None:
            await self.load()
        self.config[key] = value
        await self.write_config(self.config)
