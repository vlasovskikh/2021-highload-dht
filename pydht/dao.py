import contextlib
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
import shutil
import tempfile
from typing import AsyncIterator
import dbm.gnu as gdbm
from aiohttp import web

from pydht.settings import Settings


logger = logging.getLogger("pydht.dao")


@dataclass
class Record:
    value: bytes | None
    timestamp: datetime


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
        self.values = gdbm.open(str(p / "values.db"), "c")
        self.timestamps = gdbm.open(str(p / "timestamps.db"), "c")

    async def get(self, key: bytes) -> Record:
        try:
            _, value = await anext(self.range(key, None))
        except StopAsyncIteration:
            raise KeyError(f"Key {key!r} not found")
        return value

    async def range(
        self, from_key: bytes, to_key: bytes | None
    ) -> AsyncIterator[tuple[bytes, Record]]:
        key: bytes | None = from_key
        while key is not None and key != to_key:
            try:
                timestamp = datetime.fromisoformat(self.timestamps[key].decode())
                yield key, Record(self.values.get(key), timestamp)
            except KeyError:
                return
            key = self.timestamps.nextkey(key)

    async def upsert(self, key: bytes, value: bytes | None) -> None:
        if value is not None:
            self.values[key] = value
        else:
            with contextlib.suppress(KeyError):
                del self.values[key]
        self.timestamps[key] = datetime.utcnow().isoformat().encode()

    async def close_and_compact(self) -> None:
        self.timestamps.reorganize()
        self.values.reorganize()
        await self.aclose()

    async def aclose(self) -> None:
        self.timestamps.close()
        self.values.close()
        if self.tempdir:
            shutil.rmtree(self.tempdir)


async def dao_context(app: web.Application) -> AsyncIterator:
    settings = Settings.from_app(app)
    dao = DAO(settings.db_path)
    app["dao"] = dao
    yield
    await dao.aclose()
