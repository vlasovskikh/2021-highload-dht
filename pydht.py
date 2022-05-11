"""DHT highload storage for https://github.com/polis-vk/2021-highload-dht

Usage:
    pydht [options] PATH
    pydht -h|--help

Starts a storage server that persists data in PATH and serves an OpenAPI storage
service with docs at http://localhost:8000/docs by default.

Options:
    -p --port=PORT          Listen at the given port. [default: 8000]
    --cluster-urls=URLS     URLs of nodes in the cluster for sharding (including this
                            node), encoded as a JSON list.
    --access-log            Show access log.
    --profile=FILE          Profile with Pyinstrument and write output to FILE when the
                            server stops.
"""

import contextlib
from dataclasses import dataclass
import dbm.gnu as gdbm
import logging
import os
from pathlib import Path
import sys
from typing import AsyncIterator, Awaitable, Callable, TypedDict, cast
import docopt
from aiohttp import web
from aiohttp.log import access_logger
import pydantic
import uvloop


Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


class DAO:
    def __init__(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        self.db = gdbm.open(str(path / "pydht.db"), "c")

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


@dataclass
class ShardedDAO:
    dao: DAO
    port: int
    cluster_urls: set[str]

    async def get(self, key: bytes) -> bytes:
        return await self.dao.get(key)

    async def upsert(self, key: bytes, value: bytes | None) -> None:
        await self.dao.upsert(key, value)

    async def close_and_compact(self) -> None:
        await self.dao.close_and_compact()

    async def aclose(self) -> None:
        await self.dao.aclose()


class Settings(pydantic.BaseSettings):
    db_path: Path
    port: int = 8000
    profile_path: Path | None = None
    access_log: bool = False
    cluster_urls: set[str] = set()

    class Config:
        env_prefix = "pydht_"


class AppDict(TypedDict):
    settings: Settings
    sharded_dao: ShardedDAO


def app_config(app: web.Application) -> AppDict:
    return cast(AppDict, app)


async def dao_context(app: web.Application) -> AsyncIterator:
    config = app_config(app)
    settings = config["settings"]
    dao = DAO(settings.db_path)
    config["sharded_dao"] = ShardedDAO(dao, settings.port, settings.cluster_urls)
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
    config = app_config(app)["settings"]
    path = config.profile_path
    if not path:
        raise ValueError("Profile path is not set")

    import pyinstrument

    with pyinstrument.Profiler() as p:
        yield
    with open(path, "w") as fd:
        fd.write(p.output_html())


def update_environ_from_opts(argv: list[str]) -> None:
    opts = docopt.docopt(__doc__ or "", argv=argv)
    mapping = {
        "PATH": "PYDHT_DB_PATH",
        "--port": "PYDHT_PORT",
        "--profile": "PYDHT_PROFILE_PATH",
        "--access-log": "PYDHT_ACCESS_LOG",
        "--cluster-urls": "PYDHT_CLUSTER_URLS",
    }
    for k, v in mapping.items():
        if opts[k]:
            os.environ[v] = opts[k]


def main() -> None:
    update_environ_from_opts(sys.argv[1:])
    settings = Settings()  # type: ignore
    app = web.Application()
    app["settings"] = settings
    app.router.add_routes(routes)
    if settings.profile_path:
        app.cleanup_ctx.append(pyinstrument_context)
    app.cleanup_ctx.append(dao_context)
    app.middlewares.append(exceptions_middleware)
    if settings.access_log:
        logging.basicConfig(level=logging.INFO)
        access_logger.level = logging.INFO
        logger = access_logger
    else:
        logger = None
    uvloop.install()
    web.run_app(app, port=settings.port, access_log=logger)


if __name__ == "__main__":
    main()
