import asyncio
import contextlib
from datetime import datetime
import hashlib
import logging
from pathlib import Path
from typing import AsyncIterator, Iterable
from urllib.parse import urlparse

from aiohttp import web, DummyCookieJar, ClientSession, ClientError

from pydht.client import EntityClient
from pydht.dao import DAO, Record
from pydht.settings import Settings


logger = logging.getLogger("pydht.replicated")


class ReplicatedStorage:
    def __init__(self, path: Path | None, port: int, cluster_urls: list[str]) -> None:
        self.cluster_urls = cluster_urls
        self.url = next(
            (url for url in cluster_urls if self.is_our_url(url, port)), None
        )
        self.dao = DAO(path)
        self.session = ClientSession(cookie_jar=DummyCookieJar())
        self.watcher = TasksWatcher()

    async def get(
        self,
        key: bytes,
        *,
        ack: int = 1,
        from_: int = 1,
        replicated: bool = False,
    ) -> Record:
        self.check_replicas_ranges(ack, from_)
        urls: list[str | None]
        if replicated:
            urls = [None]
        else:
            urls = self.rendezvous_urls(key, from_)
        tasks = [asyncio.create_task(self.get_url(url, key)) for url in urls]
        self.watcher.watch(tasks)
        replies: list[Record | None] = []
        for task in asyncio.as_completed(tasks):
            try:
                record = await task
            except KeyError:
                replies.append(None)
            except ClientError:
                pass
            else:
                replies.append(record)
            if len(replies) >= ack:
                break
        if len(replies) < ack:
            raise NotEnoughReplicasError(
                f"Got {len(replies)} successful gets out of {ack} required"
            )
        for task in tasks:
            # TODO: Restore bad replicas in the background
            task.cancel()
        if not any(replies):
            raise KeyError("Key {key!r} is not found")
        return max(filter(None, replies), key=lambda r: r.timestamp)

    async def upsert(
        self,
        key: bytes,
        value: bytes | None,
        *,
        ack: int = 1,
        from_: int = 1,
        timestamp: datetime | None,
        replicated: bool = False,
    ) -> None:
        self.check_replicas_ranges(ack, from_)
        if not timestamp:
            timestamp = datetime.utcnow()
        urls: list[str | None]
        if replicated:
            urls = [None]
        else:
            urls = self.rendezvous_urls(key, from_)
        success = 0
        tasks = [
            asyncio.create_task(self.upsert_url(url, key, value, timestamp=timestamp))
            for url in urls
        ]
        self.watcher.watch(tasks)
        for task in asyncio.as_completed(tasks):
            try:
                await task
                success += 1
            except ClientError:
                pass
            if success >= ack:
                break
        if success < ack:
            raise NotEnoughReplicasError(
                f"Got {success} successful upserts out of {ack} required"
            )

    async def compact(self) -> None:
        await self.dao.compact()

    async def aclose(self) -> None:
        await self.watcher.aclose()
        await self.session.close()
        await self.dao.aclose()

    @staticmethod
    def from_app(app: web.Application) -> "ReplicatedStorage":
        return app["storage"]

    async def get_url(self, url: str | None, key: bytes) -> Record:
        if url is not None:
            client = EntityClient(url, self.session)
            return await client.get(key, replicated=True)
        else:
            return await self.dao.get(key)

    async def upsert_url(
        self, url: str | None, key: bytes, value: bytes | None, *, timestamp: datetime
    ) -> None:
        if url is not None:
            client = EntityClient(url, self.session)
            if value is not None:
                await client.put(key, value, timestamp=timestamp, replicated=True)
            else:
                await client.delete(key, timestamp=timestamp, replicated=True)
        else:
            await self.dao.upsert(key, Record(value, timestamp))

    def rendezvous_urls(self, key: bytes, from_: int) -> list[str | None]:
        if not self.cluster_urls:
            return [None]
        ordered = sorted(
            self.cluster_urls,
            key=lambda url: hashlib.sha1(key + url.encode()).digest(),
        )
        return [url if url != self.url else None for url in ordered][:from_]

    def check_replicas_ranges(self, ack: int, from_: int) -> None:
        size = len(self.cluster_urls)
        if from_ < 1 or from_ > size:
            raise ValueError(f"FROM should be between 1 and {size}")
        if ack < 1 or ack > from_:
            raise ValueError(f"ACK should be between 1 and {from_}")

    @staticmethod
    def is_our_url(url: str, port: int) -> bool:
        parsed = urlparse(url)
        return parsed.port == port and parsed.hostname in ["localhost", "127.0.0.1"]


class TasksWatcher:
    def __init__(self) -> None:
        self.tasks: set[asyncio.Task] = set()
        self.runner = asyncio.create_task(self.run())

    def watch(self, tasks: Iterable[asyncio.Task]) -> None:
        self.tasks |= set(tasks)

    async def run(self) -> None:
        while True:
            if self.tasks:
                done, _ = await asyncio.wait(self.tasks)
                self.tasks -= done
            else:
                await asyncio.sleep(1.0)

    async def aclose(self) -> None:
        self.runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.runner
        for task in self.tasks:
            task.cancel()
        results = await asyncio.gather(*self.tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.exception(
                    "Exception occurred in background task", exc_info=result
                )


async def replicated_storage_context(app: web.Application) -> AsyncIterator:
    settings = Settings.from_app(app)
    storage = ReplicatedStorage(settings.db_path, settings.port, settings.cluster_urls)
    app["storage"] = storage
    yield
    await storage.aclose()


class NotEnoughReplicasError(Exception):
    pass
