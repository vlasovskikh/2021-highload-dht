import asyncio
from asyncio.subprocess import Process
import json
from typing import Any, AsyncIterator, Coroutine, Iterable, TypeVar
from aiohttp import web, ClientSession, ClientConnectionError

from pydht.settings import Settings


T = TypeVar("T")


async def cluster_context(app: web.Application) -> AsyncIterator:
    settings = Settings.from_app(app)
    if settings.cluster:
        timeout = 5.0
        serve_ports = list(
            range(settings.port + 1, settings.port + settings.num_shards)
        )
        all_ports = [settings.port] + serve_ports
        settings.cluster_urls = {f"http://localhost:{port}" for port in all_ports}
        topology = json.dumps(list(settings.cluster_urls))
        procs = []
        for port in serve_ports:
            cmd = ["pydht", "serve", "--port", str(port), "--cluster-urls", topology]
            if settings.access_log:
                cmd.append("--access-log")
            procs.append(await asyncio.create_subprocess_exec(*cmd))
        async with ClientSession() as session:
            ready = await parallel_map(
                wait_until_ready(session, port, timeout) for port in serve_ports
            )
            if not all(ready):
                await parallel_map(terminate_process(proc, timeout) for proc in procs)
                ports = [port for port, ok in zip(serve_ports, ready) if not ok]
                raise TimeoutError(f"Servers on ports {ports} are not ready")
        yield
        await parallel_map(terminate_process(proc, timeout) for proc in procs)
    else:
        yield


async def parallel_map(aws: Iterable[Coroutine[Any, Any, T]]) -> list[T]:
    tasks = [asyncio.create_task(a) for a in aws]
    return list(await asyncio.gather(*tasks))


async def terminate_process(process: Process, timeout: float) -> bool:
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout)
    except TimeoutError:
        process.kill()
        return False
    else:
        return True


async def wait_until_ready(session: ClientSession, port: int, timeout: float) -> bool:
    spent = 0.0
    inc = 0.1
    while spent < timeout:
        if await is_ready(session, port):
            return True
        await asyncio.sleep(inc)
        spent += inc
    return False


async def is_ready(session: ClientSession, port: int) -> bool:
    try:
        async with session.get(f"http://localhost:{port}/v0/status") as response:
            return response.status // 100 == 2
    except ClientConnectionError:
        return False
