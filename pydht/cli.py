"""DHT highload storage for https://github.com/polis-vk/2021-highload-dht

Usage:
    pydht serve [--port=PORT] [--directory=DIR] [--cluster-urls=URLS] [--access-log]
                [--profile=DIR]
    pydht cluster [--port=PORT] [--num-shards=NUM] [--access-log] [--profile=DIR]
    pydht -h|--help

Commands:
    serve           Start a key-value storage server that persists data in a temporary
                    directory and serves it via an HTTP service.
    cluster         Start a local cluster serving sharded data from temporary
                    directories.

HTTP resources:
    GET /v0/status

    GET /v0/entity?id=ID
    PUT /v0/entity?id=ID BODY
    DELETE /v0/entity?id=ID

Options:
    -d --directory=DIR      Persist data in DIR instead of a temporary directory.
    -n --num-shards=NUM     Start a local cluster with NUM shards. [default: 2]
    -p --port=PORT          Listen at the given port. [default: 8000]
    --access-log            Show access log.
    --cluster-urls=URLS     URLs of nodes running in the cluster for sharding
                            (including this node), encoded as a JSON list.
    --profile=DIR           Profile with Pyinstrument and write output to
                            DIR/{PID}.html for every process PID when the server stops.

"""

import asyncio
import json
import logging
import sys
from aiohttp import web
from aiohttp.log import access_logger
import docopt
import uvloop
from pydht.app import create_app

from pydht.settings import Settings
from pydht.cluster import run_cluster


def parse_opts(argv: list[str]) -> Settings:
    opts = docopt.docopt(__doc__ or "", argv=argv)
    mapping = {
        "cluster": "cluster",
        "--num-shards": "num_shards",
        "--port": "port",
        "--directory": "db_path",
        "--profile": "profile_path",
        "--access-log": "access_log",
    }
    params = {v: opts[k] for k, v in mapping.items()}
    if (cluser_urls := opts["--cluster-urls"]) is not None:
        params["cluster_urls"] = json.loads(cluser_urls)
    return Settings(**params)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = parse_opts(sys.argv[1:])
    if settings.cluster:
        try:
            asyncio.run(run_cluster(settings))
        except KeyboardInterrupt:
            print("Interrupted")
    else:
        app = create_app(settings)
        if settings.access_log:
            access_logger.level = logging.INFO
            logger = access_logger
        else:
            logger = None
        uvloop.install()
        web.run_app(app, port=settings.port, access_log=logger)


if __name__ == "__main__":
    main()
