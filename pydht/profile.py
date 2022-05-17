import asyncio
import os
from typing import Iterable, AsyncIterator

import pyinstrument
from pyinstrument.processors import (
    aggregate_repeated_calls,
    remove_importlib,
    ProcessorType,
    ProcessorOptions,
)
from pyinstrument.frame import BaseFrame, SelfTimeFrame
from pyinstrument.session import Session
from pyinstrument.renderers.base import Renderer
from aiohttp import web

from pydht.settings import Settings


async def pyinstrument_context(app: web.Application) -> AsyncIterator:
    settings = Settings.from_app(app)
    path = settings.profile_path
    if not path:
        raise ValueError("Profile path is not set")

    with pyinstrument.Profiler() as p:
        yield

    path.mkdir(parents=True, exist_ok=True)

    with open(path / f"{os.getpid()}.html", "w") as fd:
        fd.write(p.output_html())

    output = p.output(FlameGraphRenderer())
    cmd = "flamegraph.pl --colors java --countname ms".split()
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate(input=output.encode())
    if proc.returncode != 0:
        raise Exception(stderr)
    with open(path / f"{os.getpid()}.svg", "wb") as fd:
        fd.write(stdout)


class FlameGraphRenderer(Renderer):
    def default_processors(self) -> list[ProcessorType]:
        return [
            remove_importlib,
            aggregate_repeated_calls,
            drop_self_nodes,
        ]

    def render(self, session: Session) -> str:
        root = self.preprocess(session.root_frame())
        if not root:
            raise ValueError("No base frame found")
        return "\n".join(self.render_frames(frames) for frames in self.linearize(root))

    def linearize(self, frame: BaseFrame) -> Iterable[list[BaseFrame]]:
        yield [frame]
        for child in frame.children:
            for rest in self.linearize(child):
                yield [frame, *rest]

    def render_frames(self, frames: list[BaseFrame]) -> str:
        last = frames[-1]
        path = ";".join([self.frame_text(f) for f in frames])
        return f"{path} {last.self_time * 1000}"

    def frame_text(self, frame: BaseFrame) -> str:
        name = frame.function or "null"

        if name == "[await]":
            color_suffix = "_[i]"
        elif frame.is_application_code:
            color_suffix = "_[j]"
        else:
            color_suffix = ""

        file_path_short = frame.file_path_short
        line_no = frame.line_no
        if name not in (["[await]", "[self]"]) and file_path_short and line_no:
            loc = f"({file_path_short}:{line_no})"
        else:
            loc = ""

        return " ".join([name, loc, color_suffix])


def drop_self_nodes(
    frame: BaseFrame | None, options: ProcessorOptions
) -> BaseFrame | None:
    if frame is None:
        return None
    for child in frame.children:
        if isinstance(child, SelfTimeFrame):
            frame.self_time += child.self_time
            child.remove_from_parent()
    for child in frame.children:
        drop_self_nodes(child, options)
    return frame
