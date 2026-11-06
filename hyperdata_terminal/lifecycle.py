"""Local cooperative shutdown for supervised processes, including Windows."""
import asyncio
import os
from pathlib import Path


async def run_managed(coroutine):
    marker = os.getenv("HYPERDATA_STOP_FILE")
    if not marker:
        return await coroutine

    async def wait_stop():
        while not Path(marker).is_file():
            await asyncio.sleep(.2)

    task = asyncio.create_task(coroutine)
    watcher = asyncio.create_task(wait_stop())
    try:
        done, _ = await asyncio.wait((task, watcher), return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            return await task
        task.cancel()
        try:
            return await task
        except asyncio.CancelledError:
            return None
    finally:
        watcher.cancel()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, watcher, return_exceptions=True)
