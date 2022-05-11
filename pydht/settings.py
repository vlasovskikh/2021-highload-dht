from pathlib import Path
from aiohttp import web
import pydantic


class Settings(pydantic.BaseModel):
    cluster: bool = False
    db_path: Path | None = None
    port: int = 8000
    profile_path: Path | None = None
    access_log: bool = False
    cluster_urls: set[str] = set()
    num_shards: int = 2

    @staticmethod
    def from_app(app: web.Application) -> "Settings":
        return app["settings"]
