from datetime import datetime
import logging
import re
from urllib.parse import urlencode, urljoin

import pydantic
from aiohttp import web, ClientSession, ClientResponse, ClientConnectionError

from pydht.dao import Record


logger = logging.getLogger("pydht.client")


class ReadyClient:
    def __init__(self, base_url: str, session: ClientSession) -> None:
        self.base_url = base_url
        self.session = session

    async def get(self) -> bool:
        url = urljoin(self.base_url, "/v0/status")
        try:
            async with self.session.get(url) as response:
                return response.status // 100 == 2
        except ClientConnectionError:
            return False


class EntityClient:
    def __init__(self, base_url: str, session: ClientSession) -> None:
        self.base_url = base_url
        self.session = session

    async def get(
        self,
        key: bytes,
        *,
        ack: int | None = None,
        from_: int | None = None,
        replicated: bool = False,
    ) -> Record:
        req_headers = {}
        if replicated:
            req_headers[X_REPLICATED] = "yes"
        async with self.session.get(
            self.url_for(key, ack, from_), headers=req_headers
        ) as response:
            self._debug_log_response(response)
            resp_headers = EntityHeaders.from_response(response)
            timestamp = resp_headers.x_last_modified
            if response.status == 404:
                if timestamp:
                    return Record(None, timestamp)
                else:
                    raise KeyError("Key {key!r} is not found")
            response.raise_for_status()
            return Record(await response.read(), self.effective_timestamp(timestamp))

    async def put(
        self,
        key: bytes,
        value: bytes,
        *,
        ack: int | None = None,
        from_: int | None = None,
        timestamp: datetime | None = None,
        replicated: bool = False,
    ) -> None:
        headers = {}
        if timestamp:
            headers[X_LAST_MODIFIED] = timestamp.isoformat()
        if replicated:
            headers[X_REPLICATED] = "yes"
        async with self.session.put(
            self.url_for(key, ack, from_), data=value, headers=headers
        ) as response:
            self._debug_log_response(response)
            response.raise_for_status()

    async def delete(
        self,
        key: bytes,
        *,
        ack: int | None = None,
        from_: int | None = None,
        timestamp: datetime | None = None,
        replicated: bool = False,
    ) -> None:
        headers = {}
        if timestamp:
            headers[X_LAST_MODIFIED] = timestamp.isoformat()
        if replicated:
            headers[X_REPLICATED] = "yes"
        async with self.session.delete(
            self.url_for(key, ack, from_), headers=headers
        ) as response:
            self._debug_log_response(response)
            response.raise_for_status()

    def url_for(self, key: bytes, ack: int | None, from_: int | None) -> str:
        query = {"id": key.decode()}
        if ack is not None and from_ is not None:
            query["replicas"] = f"{ack}/{from_}"
        return urljoin(self.base_url, f"/v0/entity?{urlencode(query)}")

    @staticmethod
    def effective_timestamp(timestamp: datetime | None) -> datetime:
        return timestamp or datetime.utcnow()

    def _debug_log_response(self, response: ClientResponse) -> None:
        logger.debug(
            f"HTTP {response.method} {response.url}, "
            f"got {response.status} {response.reason}"
        )


class EntityQuery(pydantic.BaseModel):
    id: bytes = pydantic.Field(min_length=1)
    replicas: tuple[int, int] | None

    @pydantic.validator("replicas", pre=True)
    def check_replicas(cls, value: str) -> tuple[int, int]:
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

    @staticmethod
    def from_request(request: web.Request) -> "EntityQuery":
        return EntityQuery(**request.query)  # type: ignore

    @property
    def replicas_pair(self) -> tuple[int, int]:
        return self.replicas or (1, 1)


X_LAST_MODIFIED = "x-last-modified"
X_REPLICATED = "x-replicated"


class EntityHeaders(pydantic.BaseModel):
    x_last_modified: datetime | None = pydantic.Field(alias=X_LAST_MODIFIED)
    x_replicated: bool = pydantic.Field(False, alias=X_REPLICATED)

    @staticmethod
    def from_response(response: ClientResponse) -> "EntityHeaders":
        return EntityHeaders(**response.headers)  # type: ignore

    @staticmethod
    def from_request(request: web.Request) -> "EntityHeaders":
        return EntityHeaders(**request.headers)  # type: ignore
