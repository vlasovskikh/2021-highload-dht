"""DHT highload storage for https://github.com/polis-vk/2021-highload-dht

Usage:
    pydht serve [--port=PORT] [--directory=DIR] [--cluster-urls=URLS] [--access-log]
                [--profile=FILE]
    pydht cluster [--port=PORT] [--num-shards=NUM] [--access-log] [--profile=FILE]
    pydht -h|--help

Commands:
    serve           Start a key-value storage server that persists data in a temporary
                    directory and serves it via an HTTP service.
    cluster         Start a local cluster serving sharded data from temporary
                    directories.

HTTP resources:
    GET /v0/status

    GET /v0/entity?id=ID
    PUT /v0/entity?id=ID BODY
    DELETE /v0/entity?id=ID

Options:
    -d --directory=DIR      Persist data in DIR instead of a temporary directory.
    -n --num-shards=NUM     Start a local cluster with NUM shards. [default: 2]
    -p --port=PORT          Listen at the given port. [default: 8000]
    --access-log            Show access log.
    --cluster-urls=URLS     URLs of nodes running in the cluster for sharding
                            (including this node), encoded as a JSON list.
    --profile=FILE          Profile with Pyinstrument and write output to FILE when the
                            server stops.

"""

import asyncio
from asyncio.subprocess import Process
import contextlib
import dbm.gnu as gdbm
import hashlib
import json
import logging
from pathlib import Path
import shutil
import sys
import tempfile
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    TypeVar,
    TypedDict,
    cast,
)
from urllib.parse import urljoin, urlparse
import docopt
from aiohttp import web, ClientSession, ClientConnectionError, DummyCookieJar
from aiohttp.log import access_logger
import pydantic
import uvloop


T = TypeVar("T")
Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]
logger = logging.getLogger("pydht")


class DAO:
    def __init__(self, path: Path | None) -> None:
        if path:
            p = path
            path.mkdir(parents=True, exist_ok=True)
            self.tempdir = None
            logger.info(f"Serving data from {path.absolute()}")
        else:
            p = Path(tempfile.mkdtemp(prefix="pydht"))
            self.tempdir = p
            logger.info(f"Serving data from a temporary path {p}")
        self.db = gdbm.open(str(p / "pydht.db"), "c")

    async def get(self, key: bytes) -> bytes:
        try:
            _, value = await anext(self.range(key, None))
        except StopAsyncIteration:
            raise KeyError(f"Key {key!r} not found")
        return value

    async def range(
        self, from_key: bytes, to_key: bytes | None
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        key: bytes | None = from_key
        while key is not None and key != to_key:
            try:
                yield key, self.db[key]
            except KeyError:
                return
            key = self.db.nextkey(key)

    async def upsert(self, key: bytes, value: bytes | None) -> None:
        if value is not None:
            self.db[key] = value
        else:
            with contextlib.suppress(KeyError):
                del self.db[key]

    async def close_and_compact(self) -> None:
        self.db.reorganize()
        await self.aclose()

    async def aclose(self) -> None:
        self.db.close()
        if self.tempdir:
            shutil.rmtree(self.tempdir)


class ShardedDAO:
    def __init__(self, path: Path | None, port: int, cluster_urls: set[str]) -> None:
        self.cluster_urls = cluster_urls
        self.url = next(
            (url for url in cluster_urls if self.is_our_url(url, port)), None
        )
        if cluster_urls and not self.url:
            raise ValueError(
                f"Cannot find our host URL for port {port} among {cluster_urls}"
            )
        self.dao = DAO(path)
        self.session = ClientSession(cookie_jar=DummyCookieJar())

    async def get(self, key: bytes) -> bytes:
        if shard_url := self.rendezvous_hash_url(key):
            url = urljoin(shard_url, f"/v0/entity?id={key.decode()}")
            async with self.session.get(url) as response:
                if response.status == 404:
                    raise KeyError("Key {key!r} is not found")
                response.raise_for_status()
                return await response.read()
        else:
            return await self.dao.get(key)

    async def upsert(self, key: bytes, value: bytes | None) -> None:
        if shard_url := self.rendezvous_hash_url(key):
            url = urljoin(shard_url, f"/v0/entity?id={key.decode()}")
            if value is not None:
                async with self.session.put(url, data=value) as response:
                    response.raise_for_status()
            else:
                async with self.session.delete(url) as response:
                    response.raise_for_status()
        else:
            await self.dao.upsert(key, value)

    async def close_and_compact(self) -> None:
        await self.session.close()
        await self.dao.close_and_compact()

    async def aclose(self) -> None:
        await self.session.close()
        await self.dao.aclose()

    def rendezvous_hash_url(self, key: bytes) -> str | None:
        if not self.cluster_urls:
            return None
        url = max(
            self.cluster_urls,
            key=lambda url: hashlib.sha1(key + url.encode()).digest(),
        )
        return url if url != self.url else None

    @staticmethod
    def is_our_url(url: str, port: int) -> bool:
        parsed = urlparse(url)
        return parsed.port == port and parsed.hostname in ["localhost", "127.0.0.1"]


class Settings(pydantic.BaseModel):
    cluster: bool = False
    db_path: Path | None = None
    port: int = 8000
    profile_path: Path | None = None
    access_log: bool = False
    cluster_urls: set[str] = set()
    num_shards: int = 2


class AppDict(TypedDict):
    settings: Settings
    sharded_dao: ShardedDAO


def app_config(app: web.Application) -> AppDict:
    return cast(AppDict, app)


async def dao_context(app: web.Application) -> AsyncIterator:
    config = app_config(app)
    settings = config["settings"]
    config["sharded_dao"] = ShardedDAO(
        settings.db_path, settings.port, settings.cluster_urls
    )
    yield
    await config["sharded_dao"].aclose()


routes = web.RouteTableDef()


@routes.view("/v0/status")
class StatusView(web.View):
    async def get(self) -> web.Response:
        return web.Response(text="I'm OK")


class EntityQuery(pydantic.BaseModel):
    id: bytes = pydantic.Field(min_length=1)


@routes.view("/v0/entity")
class EntityView(web.View):
    async def get(self) -> web.Response:
        try:
            value = await self.sharded_dao.get(self.get_query().id)
        except KeyError:
            raise not_found()
        return web.Response(body=value)

    async def put(self) -> web.Response:
        body = await self.request.read()
        await self.sharded_dao.upsert(self.get_query().id, body)
        return web.Response(status=201, text="Created")

    async def delete(self) -> web.Response:
        await self.sharded_dao.upsert(self.get_query().id, None)
        return web.Response(status=202, text="Accepeted")

    @property
    def sharded_dao(self) -> ShardedDAO:
        return app_config(self.request.app)["sharded_dao"]

    def get_query(self) -> EntityQuery:
        return EntityQuery(**self.request.query)  # type: ignore


def not_found() -> web.HTTPNotFound:
    e = web.HTTPNotFound(text="Not found")
    e["preserve"] = True
    return e


@web.middleware
async def exceptions_middleware(
    request: web.Request, handler: Handler
) -> web.StreamResponse:
    try:
        return await handler(request)
    except pydantic.ValidationError as e:
        data = {"detail": "Validation failed", "errors": e.errors()}
        return web.json_response(data, status=400)
    except web.HTTPNotFound as e:
        if e.get("preserve", False):
            raise
        else:
            raise web.HTTPBadRequest()


async def pyinstrument_context(app: web.Application) -> AsyncIterator:
    settings = app_config(app)["settings"]
    path = settings.profile_path
    if not path:
        raise ValueError("Profile path is not set")

    import pyinstrument

    with pyinstrument.Profiler() as p:
        yield
    with open(path, "w") as fd:
        fd.write(p.output_html())


def parse_opts(argv: list[str]) -> Settings:
    opts = docopt.docopt(__doc__ or "", argv=argv)
    mapping = {
        "cluster": "cluster",
        "--num-shards": "num_shards",
        "--port": "port",
        "--directory": "db_path",
        "--profile": "profile_path",
        "--access-log": "access_log",
    }
    params = {v: opts[k] for k, v in mapping.items()}
    if (cluser_urls := opts["--cluster-urls"]) is not None:
        params["cluster_urls"] = json.loads(cluser_urls)
    return Settings(**params)


async def cluster_context(app: web.Application) -> AsyncIterator:
    settings = app_config(app)["settings"]
    if settings.cluster:
        timeout = 5.0
        serve_ports = get_serve_ports(settings.port, settings.num_shards)
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


async def terminate(processes: list[Process], timeout: float) -> None:
    for proc in processes:
        proc.terminate()
    waits = [proc.wait() for proc in processes]
    try:
        await asyncio.wait_for(asyncio.gather(*waits), timeout=timeout)
    except TimeoutError:
        for proc in processes:
            proc.kill()


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


def get_serve_ports(port: int, num_shards: int) -> list[int]:
    return list(range(port + 1, port + num_shards))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = parse_opts(sys.argv[1:])
    app = web.Application()
    app["settings"] = settings
    app.router.add_routes(routes)
    if settings.profile_path:
        app.cleanup_ctx.append(pyinstrument_context)
    app.cleanup_ctx.append(cluster_context)
    app.cleanup_ctx.append(dao_context)
    app.middlewares.append(exceptions_middleware)
    if settings.access_log:
        access_logger.level = logging.INFO
        logger = access_logger
    else:
        logger = None
    uvloop.install()
    web.run_app(app, port=settings.port, access_log=logger)


if __name__ == "__main__":
    main()
