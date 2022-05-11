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
        port = settings.port
        num_shards = settings.num_shards
        urls = get_cluster_urls(port, num_shards)
        settings.cluster_urls = urls
        procs = await spawn_servers(
            port, num_shards, urls, timeout=timeout, access_log=settings.access_log
        )
        yield
        await parallel_map(terminate_process(proc, timeout) for proc in procs)
    else:
        yield


def get_cluster_urls(port: int, num_shards: int) -> set[str]:
    all_ports = [port] + get_ports_to_serve(port, num_shards)
    return {f"http://localhost:{port}" for port in all_ports}


def get_ports_to_serve(port: int, num_shards: int) -> list[int]:
    return list(range(port + 1, port + num_shards))


async def spawn_servers(
    port: int,
    num_shards: int,
    cluster_urls: set[str],
    *,
    timeout: float,
    access_log: bool,
) -> list[Process]:
    topology = json.dumps(list(cluster_urls))
    procs = []
    ports = get_ports_to_serve(port, num_shards)
    for port in ports:
        cmd = ["pydht", "serve", "--port", str(port), "--cluster-urls", topology]
        if access_log:
            cmd.append("--access-log")
        procs.append(await asyncio.create_subprocess_exec(*cmd))
    async with ClientSession() as session:
        await check_all_ready(session, procs, ports, timeout=timeout)
    return procs


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


async def check_all_ready(
    session: ClientSession, procs: list[Process], ports: list[int], *, timeout: float
) -> None:
    ready = await parallel_map(check_ready(session, port, timeout) for port in ports)
    if all(ready):
        return
    await parallel_map(terminate_process(proc, timeout) for proc in procs)
    not_ready = [port for port, ok in zip(ports, ready) if not ok]
    raise TimeoutError(f"Servers on ports {not_ready} are not ready")


async def check_ready(session: ClientSession, port: int, timeout: float) -> bool:
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
