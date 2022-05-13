from datetime import datetime
import hashlib
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import urlparse
from aiohttp import web, DummyCookieJar, ClientSession
from pydht.client import EntityClient

from pydht.dao import DAO, Record, logger
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

    async def get(
        self, key: bytes, *, ack: int | None = None, from_: int | None = None
    ) -> Record:
        if shard_url := self.rendezvous_hash_url(key):
            client = EntityClient(shard_url, self.session)
            return await client.get(key, ack=ack, from_=from_)
        else:
            return await self.dao.get(key)

    async def upsert(
        self,
        key: bytes,
        value: bytes | None,
        *,
        ack: int | None = None,
        from_: int | None = None,
        timestamp: datetime | None,
    ) -> None:
        if shard_url := self.rendezvous_hash_url(key):
            client = EntityClient(shard_url, self.session)
            if value is not None:
                await client.put(key, value, ack=ack, from_=from_, timestamp=timestamp)
            else:
                await client.delete(key, ack=ack, from_=from_, timestamp=timestamp)
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
