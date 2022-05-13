import hashlib
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import urljoin, urlparse
from aiohttp import web, DummyCookieJar, ClientSession

from pydht.dao import DAO, logger
from pydht.settings import Settings


class ReplicatedStorage:
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

    async def get(self, key: bytes, ack: int = 1, from_: int = 1) -> bytes:
        if shard_url := self.rendezvous_hash_url(key):
            url = urljoin(shard_url, f"/v0/entity?id={key.decode()}")
            async with self.session.get(url) as response:
                if response.status == 404:
                    raise KeyError("Key {key!r} is not found")
                response.raise_for_status()
                return await response.read()
        else:
            return await self.dao.get(key)

    async def upsert(
        self, key: bytes, value: bytes | None, ack: int = 1, from_: int = 1
    ) -> None:
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
    def from_app(app: web.Application) -> "ReplicatedStorage":
        return app["storage"]

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


async def replicated_storage_context(app: web.Application) -> AsyncIterator:
    settings = Settings.from_app(app)
    storage = ReplicatedStorage(settings.db_path, settings.port, settings.cluster_urls)
    app["storage"] = storage
    yield
    await storage.aclose()
