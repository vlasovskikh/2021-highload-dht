import asyncio
from asyncio.subprocess import Process
import contextlib
import json
import logging
import multiprocessing
import os
from pathlib import Path
from typing import Any, AsyncIterator, Coroutine, Iterable, TypeVar
from aiohttp import ClientSession, ClientConnectionError
from asyncio.exceptions import TimeoutError

from pydht.settings import Settings


T = TypeVar("T")
logger = logging.getLogger("pydht.cluser")


async def run_cluster(settings: Settings) -> None:
    workers = multiprocessing.cpu_count()
    timeout = 5.0
    port = settings.port
    num_shards = settings.num_shards
    urls = get_cluster_urls(port, num_shards)
    async with ClientSession() as session, spawn_servers(
        port,
        num_shards,
        urls,
        session=session,
        timeout=timeout,
        access_log=settings.access_log,
        profile_path=settings.profile_path,
    ) as servers, spawn_balancer(
        port,
        urls,
        session=session,
        workers=workers,
        timeout=timeout,
        profile_path=settings.profile_path,
    ) as balancer:
        procs = [balancer] + servers
        await asyncio.gather(*[proc.wait() for proc in procs])


@contextlib.asynccontextmanager
async def spawn_balancer(
    port: int,
    cluster_urls: list[str],
    *,
    session: ClientSession,
    workers: int,
    timeout: float,
    profile_path: Path | None,
) -> AsyncIterator[Process]:
    cmd = [
        "gunicorn",
        "--bind",
        f"0.0.0.0:{port}",
        "--worker-class",
        "aiohttp.GunicornUVLoopWebWorker",
        "--workers",
        str(workers),
        "pydht.app:async_create_app",
    ]
    env = os.environ.copy()
    env["PYDHT_CLUSTER_URLS"] = json.dumps(cluster_urls)
    if profile_path:
        env["PYDHT_PROFILE_PATH"] = str(profile_path)
    proc = await asyncio.create_subprocess_exec(*cmd, env=env)
    try:
        if not await check_ready(proc, port, session=session, timeout=timeout):
            raise TimeoutError(f"Balancer on port {port} is not ready")
        yield proc
    finally:
        await terminate_process(proc, timeout=timeout)


def get_cluster_urls(port: int, num_shards: int) -> list[str]:
    ports = get_ports_to_serve(port, num_shards)
    return [f"http://localhost:{port}" for port in ports]


def get_ports_to_serve(port: int, num_shards: int) -> list[int]:
    return list(range(port + 1, port + num_shards + 1))


@contextlib.asynccontextmanager
async def spawn_servers(
    port: int,
    num_shards: int,
    cluster_urls: list[str],
    *,
    session: ClientSession,
    timeout: float,
    access_log: bool,
    profile_path: Path | None,
) -> AsyncIterator[list[Process]]:
    ports = get_ports_to_serve(port, num_shards)
    procs: dict[int, Process] = {}
    for port in ports:
        cmd = [
            "pydht",
            "serve",
            "--port",
            str(port),
            "--cluster-urls",
            json.dumps(cluster_urls),
        ]
        if access_log:
            cmd.append("--access-log")
        if profile_path:
            cmd.extend(["--profile", str(profile_path)])
        proc = await asyncio.create_subprocess_exec(*cmd)
        procs[port] = proc
        logger.info(f"Starting server with pid {proc.pid} at port {port}")
    try:
        ready = await in_parallel(
            check_ready(proc, port, session=session, timeout=timeout)
            for port, proc in procs.items()
        )
        if not all(ready):
            not_ready = [port for port, ok in zip(ports, ready) if not ok]
            raise TimeoutError(f"Servers on ports {not_ready} are not ready")
        yield list(procs.values())
    finally:
        await in_parallel(
            terminate_process(proc, timeout=timeout) for proc in procs.values()
        )


async def in_parallel(coroutines: Iterable[Coroutine[Any, Any, T]]) -> list[T]:
    tasks = [asyncio.create_task(c) for c in coroutines]
    return list(await asyncio.gather(*tasks))


async def terminate_process(process: Process, *, timeout: float) -> bool:
    try:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout)
        except TimeoutError:
            process.kill()
            return False
        else:
            return True
    except ProcessLookupError:
        return True


async def check_ready(
    process: Process, port: int, *, session: ClientSession, timeout: float
) -> bool:
    spent = 0.0
    inc = 0.1
    while spent < timeout:
        try:
            await asyncio.wait_for(process.wait(), inc)
        except TimeoutError:
            pass
        else:
            return False
        if await is_ready(port, session=session):
            return True
        await asyncio.sleep(inc)
        spent += inc
    return False


async def is_ready(port: int, *, session: ClientSession) -> bool:
    try:
        async with session.get(f"http://localhost:{port}/v0/status") as response:
            return response.status // 100 == 2
    except ClientConnectionError:
        return False
