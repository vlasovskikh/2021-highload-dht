import contextlib
import hashlib
import logging
from pathlib import Path
import shutil
import tempfile
from typing import AsyncIterator
from urllib.parse import urljoin, urlparse
import dbm.gnu as gdbm
from aiohttp import web, ClientSession, DummyCookieJar

from pydht.settings import Settings


logger = logging.getLogger("pydht.dao")


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
    def __init__(self, path: Path | None, port: int, cluster_urls: list[str]) -> None:
        self.cluster_urls = cluster_urls
        self.url = next(
            (url for url in cluster_urls if self.is_our_url(url, port)), None
        )
        if cluster_urls and not self.url:
            logger.info(
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

    @staticmethod
    def from_app(app: web.Application) -> "ShardedDAO":
        return app["sharded_dao"]

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


async def dao_context(app: web.Application) -> AsyncIterator:
    settings = Settings.from_app(app)
    dao = ShardedDAO(settings.db_path, settings.port, settings.cluster_urls)
    app["sharded_dao"] = dao
    yield
    await dao.aclose()
