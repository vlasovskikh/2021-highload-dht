from pathlib import Path

from aiohttp import web
import pydantic


class Settings(pydantic.BaseSettings):
    cluster: bool = False
    db_path: Path | None = None
    port: int = 8000
    cluster_urls: list[str] = []
    num_shards: int = 2
    workers: int = 2
    profile_path: Path | None = None
    access_log: bool = False
    debug: bool = False

    @staticmethod
    def from_app(app: web.Application) -> "Settings":
        return app["settings"]

    class Config:
        env_prefix = "pydht_"
