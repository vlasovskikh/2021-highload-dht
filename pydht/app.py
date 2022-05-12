import os
from typing import AsyncIterator
from aiohttp import web

from pydht.sharded import sharded_dao_context
from pydht.settings import Settings
from pydht.views import exceptions_middleware, routes


async def pyinstrument_context(app: web.Application) -> AsyncIterator:
    settings = Settings.from_app(app)
    path = settings.profile_path
    if not path:
        raise ValueError("Profile path is not set")

    import pyinstrument

    with pyinstrument.Profiler() as p:
        yield

    path.mkdir(parents=True, exist_ok=True)
    with open(path / f"{os.getpid()}.html", "w") as fd:
        fd.write(p.output_html())


def create_app(settings: Settings) -> web.Application:
    app = web.Application()
    app["settings"] = settings
    app.router.add_routes(routes)
    if settings.profile_path:
        app.cleanup_ctx.append(pyinstrument_context)
    app.cleanup_ctx.append(sharded_dao_context)
    app.middlewares.append(exceptions_middleware)
    return app


async def async_create_app() -> web.Application:
    return create_app(Settings())
