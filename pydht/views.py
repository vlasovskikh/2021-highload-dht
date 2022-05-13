from typing import Awaitable, Callable
from aiohttp import web
import pydantic
from pydht.client import X_LAST_MODIFIED, EntityHeaders, EntityQuery

from pydht.replicated import NotEnoughReplicasError, ReplicatedStorage


routes = web.RouteTableDef()


@routes.view("/v0/status")
class StatusView(web.View):
    async def get(self) -> web.Response:
        return web.Response(text="I'm OK")


@routes.view("/v0/entity")
class EntityView(web.View):
    async def get(self) -> web.Response:
        q = self.get_query()
        ack, from_ = q.replicas_pair
        try:
            record = await self.storage.get(q.id, ack=ack, from_=from_)
        except KeyError:
            raise not_found()
        except NotEnoughReplicasError as e:
            raise web.HTTPGatewayTimeout(text=str(e))
        if record.value is None:
            raise not_found()
        return web.Response(
            body=record.value,
            headers={
                X_LAST_MODIFIED: record.timestamp.isoformat(),
            },
        )

    async def put(self) -> web.Response:
        q = self.get_query()
        headers = self.get_headers()
        body = await self.request.read()
        ack, from_ = q.replicas_pair
        try:
            await self.storage.upsert(
                q.id, body, ack=ack, from_=from_, timestamp=headers.x_last_modified
            )
        except NotEnoughReplicasError as e:
            raise web.HTTPGatewayTimeout(text=str(e))
        return web.Response(status=201, text="Created")

    async def delete(self) -> web.Response:
        q = self.get_query()
        headers = self.get_headers()
        ack, from_ = q.replicas_pair
        try:
            await self.storage.upsert(
                q.id, None, ack=ack, from_=from_, timestamp=headers.x_last_modified
            )
        except NotEnoughReplicasError as e:
            raise web.HTTPGatewayTimeout(text=str(e))
        return web.Response(status=202, text="Accepeted")

    @property
    def storage(self) -> ReplicatedStorage:
        return ReplicatedStorage.from_app(self.request.app)

    def get_query(self) -> EntityQuery:
        return EntityQuery.from_request(self.request)

    def get_headers(self) -> EntityHeaders:
        return EntityHeaders.from_request(self.request)


_Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


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
