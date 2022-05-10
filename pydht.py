"""DHT highload storage for https://github.com/polis-vk/2021-highload-dht

Usage:
    pydht [options] PATH
    pydht -h|--help

Starts a storage server that persists data in PATH and serves an OpenAPI storage
service with docs at http://localhost:8000/docs by default.

Options:
    -p --port=PORT          Listen at the given port. [default: 8000]
    --reload                Enabled auto-reload.
    --profile=FILE          Profile with Pyinstrument and write output to FILE.
"""

import contextlib
import dbm.gnu as gdbm
import functools
import os
from pathlib import Path
import sys
from typing import AsyncIterator, Iterator
from fastapi import FastAPI, Depends, Request, status, HTTPException, Response, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseSettings
import uvicorn
import docopt


ENCODING = "utf-8"


class Settings(BaseSettings):
    db_path: Path

    class Config:
        env_prefix = "pydht_"


app = FastAPI()


class DAO:
    def __init__(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        self.db = gdbm.open(str(path / "pydht.db"), "c")

    def range(
        self, from_key: bytes, to_key: bytes | None
    ) -> Iterator[tuple[bytes, bytes]]:
        key: bytes | None = from_key
        while key is not None and key != to_key:
            try:
                yield key, self.db[key]
            except KeyError:
                return
            key = self.db.nextkey(key)

    def upsert(self, key: bytes, value: bytes | None) -> None:
        if value is not None:
            self.db[key] = value
        else:
            with contextlib.suppress(KeyError):
                del self.db[key]

    def close_and_compact(self) -> None:
        self.db.reorganize()
        self.close()

    def close(self) -> None:
        self.db.close()


global_dao: DAO | None = None


@app.on_event("startup")
def startup_event() -> None:
    global global_dao
    global_dao = DAO(get_settings_sync().db_path)


@app.on_event("shutdown")
def shutdown_event() -> None:
    if global_dao:
        global_dao.close()


@functools.lru_cache()
def get_settings_sync() -> Settings:
    return Settings()  # type: ignore


async def get_settings() -> Settings:
    return get_settings_sync()


async def get_dao() -> AsyncIterator[DAO]:
    if not global_dao:
        raise ValueError("DAO is not configured")
    yield global_dao


@app.exception_handler(status.HTTP_404_NOT_FOUND)
async def not_found_handler(_: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, HTTPException):
        return JSONResponse(
            content={"detail": exc.detail}, status_code=status.HTTP_404_NOT_FOUND
        )
    else:
        return JSONResponse(
            content={"detail": "Bad request"}, status_code=status.HTTP_400_BAD_REQUEST
        )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    _: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(content={"detail": exc.errors()}, status_code=400)


@app.get("/v0/status")
async def get_status() -> PlainTextResponse:
    return PlainTextResponse(content="I'm OK")


@app.get("/v0/entity")
async def get_entity(
    id: str = Query(..., min_length=1), dao: DAO = Depends(get_dao)
) -> Response:
    try:
        _, value = next(dao.range(id.encode(ENCODING), None))
    except StopIteration:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Not found",
            headers={"X-Preserve-Status": True},
        )
    else:
        return Response(content=value)


@app.put("/v0/entity", status_code=status.HTTP_201_CREATED)
async def put_entity(
    request: Request, id: str = Query(..., min_length=1), dao: DAO = Depends(get_dao)
) -> None:
    body = await request.body()
    dao.upsert(id.encode(ENCODING), body)


@app.delete("/v0/entity", status_code=status.HTTP_202_ACCEPTED)
async def delete_entity(
    id: str = Query(..., min_length=1), dao: DAO = Depends(get_dao)
) -> None:
    dao.upsert(id.encode(ENCODING), None)


def patch_uvicorn(path: str) -> None:
    import pyinstrument
    from uvicorn.server import Server

    original_serve = Server.serve

    async def patched_serve(*args, **kwargs):
        with pyinstrument.Profiler() as p:
            res = await original_serve(*args, **kwargs)
        with open(path, "w") as fd:
            fd.write(p.output_html())
        return res

    Server.serve = patched_serve


def main() -> None:
    opts = docopt.docopt(__doc__ or "", argv=sys.argv[1:])
    os.environ["PYDHT_DB_PATH"] = opts["PATH"]
    profile_file = opts["--profile"]
    if profile_file:
        patch_uvicorn(profile_file)
    uvicorn.run(
        "pydht:app",
        port=int(opts["--port"]),
        reload=opts["--reload"],
        loop="uvloop",
        access_log=False,
    )


if __name__ == "__main__":
    main()
