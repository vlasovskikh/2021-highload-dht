import contextlib
import logging
from pathlib import Path
import shutil
import tempfile
from typing import AsyncIterator
import dbm.gnu as gdbm
from aiohttp import web

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


async def dao_context(app: web.Application) -> AsyncIterator:
    settings = Settings.from_app(app)
    dao = DAO(settings.db_path)
    app["dao"] = dao
    yield
    await dao.aclose()
