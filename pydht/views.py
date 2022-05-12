from typing import Awaitable, Callable
from aiohttp import web
import pydantic

from pydht.sharded import ShardedDAO


_Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


routes = web.RouteTableDef()


@routes.view("/v0/status")
class StatusView(web.View):
    async def get(self) -> web.Response:
        return web.Response(text="I'm OK")


class EntityQuery(pydantic.BaseModel):
    id: bytes = pydantic.Field(min_length=1)


@routes.view("/v0/entity")
class EntityView(web.View):
    async def get(self) -> web.Response:
        try:
            value = await self.sharded_dao.get(self.get_query().id)
        except KeyError:
            raise not_found()
        return web.Response(body=value)

    async def put(self) -> web.Response:
        body = await self.request.read()
        await self.sharded_dao.upsert(self.get_query().id, body)
        return web.Response(status=201, text="Created")

    async def delete(self) -> web.Response:
        await self.sharded_dao.upsert(self.get_query().id, None)
        return web.Response(status=202, text="Accepeted")

    @property
    def sharded_dao(self) -> ShardedDAO:
        return ShardedDAO.from_app(self.request.app)

    def get_query(self) -> EntityQuery:
        return EntityQuery(**self.request.query)  # type: ignore


@web.middleware
async def exceptions_middleware(
    request: web.Request, handler: _Handler
) -> web.StreamResponse:
    try:
        return await handler(request)
    except pydantic.ValidationError as e:
        data = {"detail": "Validation failed", "errors": e.errors()}
        return web.json_response(data, status=400)
    except web.HTTPNotFound as e:
        if e.get("preserve", False):
            raise
        else:
            raise web.HTTPBadRequest()


def not_found() -> web.HTTPNotFound:
    e = web.HTTPNotFound(text="Not found")
    e["preserve"] = True
    return e
