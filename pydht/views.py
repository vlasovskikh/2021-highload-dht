import re
from typing import Awaitable, Callable
from aiohttp import web
import pydantic

from pydht.replicated import ReplicatedStorage


_Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


routes = web.RouteTableDef()


@routes.view("/v0/status")
class StatusView(web.View):
    async def get(self) -> web.Response:
        return web.Response(text="I'm OK")


class EntityQuery(pydantic.BaseModel):
    id: bytes = pydantic.Field(min_length=1)
    replicas: tuple[int, int] | None

    @pydantic.validator("replicas", pre=True)
    def check_replicas(cls, value: str | None) -> tuple[int, int] | None:
        if value is None:
            return None
        m = re.match(r"(\d+)/(\d+)", value)
        if not m:
            raise ValueError(f"{value!r} does not follow ACK/FROM syntax")
        ack_s, from_s = m.groups()
        ack, from_ = int(ack_s), int(from_s)
        if from_ <= 0 or ack <= 0:
            raise ValueError(f"Both ACK and FROM in {value!r} should be positive")
        elif ack > from_:
            raise ValueError(f"ACK in {value!r} should be not greater than FROM")
        return ack, from_

    def replicas_args(self) -> tuple:
        return self.replicas or ()


@routes.view("/v0/entity")
class EntityView(web.View):
    async def get(self) -> web.Response:
        q = self.parse_query()
        try:
            value = await self.storage.get(q.id, *q.replicas_args())
        except KeyError:
            raise not_found()
        return web.Response(body=value)

    async def put(self) -> web.Response:
        q = self.parse_query()
        body = await self.request.read()
        await self.storage.upsert(q.id, body, *q.replicas_args())
        return web.Response(status=201, text="Created")

    async def delete(self) -> web.Response:
        q = self.parse_query()
        await self.storage.upsert(q.id, None, *q.replicas_args())
        return web.Response(status=202, text="Accepeted")

    @property
    def storage(self) -> ReplicatedStorage:
        return ReplicatedStorage.from_app(self.request.app)

    def parse_query(self) -> EntityQuery:
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
