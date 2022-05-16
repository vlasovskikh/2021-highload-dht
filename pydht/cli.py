"""DHT highload storage for https://github.com/polis-vk/2021-highload-dht

HTTP resources:
    GET /v0/status

    GET /v0/entity?id=ID
    PUT /v0/entity?id=ID BODY
    DELETE /v0/entity?id=ID
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from aiohttp import web
from aiohttp.log import access_logger
import typer
import uvloop

from pydht.app import create_app
from pydht.settings import Settings
from pydht.cluster import run_cluster


cli_app = typer.Typer(add_completion=False)


class CommonOpts:
    port: int = typer.Option(8000, "-p", "--port", help="Listen at the given port.")
    access_log: bool = typer.Option(False, help="Show access log.")
    profile_path: Optional[Path] = typer.Option(
        None,
        "--profile",
        help="Profile with Pyinstrument and write output to PATH/{PID}.html for every "
        "process PID when the server stops.",
    )
    debug: bool = typer.Option(False, help="Enable debug logging.")


@cli_app.command()
def serve(
    db_path: Optional[Path] = typer.Option(
        None,
        "-d",
        "--directory",
        help="Persist data in DIR instead of a temporary directory.",
    ),
    cluster_urls: Optional[str] = typer.Option(
        None,
        help="URLs of nodes running in the cluster for sharding (including this node), "
        "encoded as a JSON list.",
    ),
    port: int = CommonOpts.port,
    access_log: bool = CommonOpts.access_log,
    profile_path: Optional[Path] = CommonOpts.profile_path,
    debug: bool = CommonOpts.debug,
) -> None:
    """Start a key-value storage server that persists data in a temporary directory and
    serves it via an HTTP service.
    """
    urls = [] if cluster_urls is None else json.loads(cluster_urls)
    settings = Settings(
        db_path=db_path,
        cluster_urls=urls,
        port=port,
        access_log=access_log,
        profile_path=profile_path,
        debug=debug,
    )
    setup_logging(settings)
    app = create_app(settings)
    if settings.access_log:
        access_logger.level = logging.INFO
        logger = access_logger
    else:
        logger = None
    uvloop.install()
    web.run_app(app, port=settings.port, access_log=logger)


@cli_app.command()
def cluster(
    num_shards: int = typer.Option(
        2,
        "-n",
        "--num-shards",
        min=1,
        help="Start a local cluster with NUM shards.",
    ),
    workers: int = typer.Option(
        2, min=1, help="Number of gunicorn workers to handle incoming connections."
    ),
    port: int = CommonOpts.port,
    access_log: bool = CommonOpts.access_log,
    profile_path: Optional[Path] = CommonOpts.profile_path,
    debug: bool = CommonOpts.debug,
) -> None:
    """Start a local cluster serving sharded data from temporary directories."""
    settings = Settings(
        num_shards=num_shards,
        workers=workers,
        port=port,
        access_log=access_log,
        profile_path=profile_path,
        debug=debug,
    )
    setup_logging(settings)
    try:
        asyncio.run(run_cluster(settings))
    except KeyboardInterrupt:
        print("Interrupted")


def setup_logging(settings: Settings) -> None:
    logging.basicConfig(level=logging.DEBUG if settings.debug else logging.INFO)


def main() -> None:
    cli_app()


if __name__ == "__main__":
    main()
