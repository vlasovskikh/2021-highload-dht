import logging

from aiohttp import web

from pydht.replicated import replicated_storage_context
from pydht.settings import Settings
from pydht.views import exceptions_middleware, routes


def create_app(settings: Settings) -> web.Application:
    setup_logging(settings)
    app = web.Application()
    app["settings"] = settings
    app.router.add_routes(routes)
    if settings.profile_path:
        from pydht.profile import pyinstrument_context

        app.cleanup_ctx.append(pyinstrument_context)
    app.cleanup_ctx.append(replicated_storage_context)
    app.middlewares.append(exceptions_middleware)
    return app


async def async_create_app() -> web.Application:
    return create_app(Settings())


def setup_logging(settings: Settings) -> None:
    logging.basicConfig(level=logging.DEBUG if settings.debug else logging.INFO)
