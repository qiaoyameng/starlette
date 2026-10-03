from __future__ import annotations

import hashlib
import http.cookies
import json
import os
import stat
import sys
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from email.utils import format_datetime, formatdate
from functools import partial
from mimetypes import guess_type
from secrets import token_hex
from typing import Any, Literal
from urllib.parse import quote

import anyio
import anyio.to_thread

from starlette._utils import create_collapsing_task_group
from starlette.background import BackgroundTask
from starlette.concurrency import iterate_in_threadpool
from starlette.datastructures import URL, Headers, MutableHeaders
from starlette.requests import ClientDisconnect
from starlette.types import Message, Receive, Scope, Send


class Response:
    media_type = None
    charset = "utf-8"

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        self.status_code = status_code
        if media_type is not None:
            self.media_type = media_type
        self.background = background
        self.body = self.render(content)
        self.init_headers(headers)

    def render(self, content: Any) -> bytes | memoryview:
        if content is None:
            return b""
        if isinstance(content, bytes | memoryview):
            return content
        return content.encode(self.charset)  # type: ignore

    def init_headers(self, headers: Mapping[str, str] | None = None) -> None:
        if headers is None:
            raw_headers: list[tuple[bytes, bytes]] = []
            populate_content_length = True
            populate_content_type = True
        else:
            raw_headers = [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()]
            keys = [h[0] for h in raw_headers]
            populate_content_length = b"content-length" not in keys
            populate_content_type = b"content-type" not in keys

        body = getattr(self, "body", None)
        if (
            body is not None
            and populate_content_length
            and not (self.status_code < 200 or self.status_code in (204, 304))
        ):
            content_length = str(len(body))
            raw_headers.append((b"content-length", content_length.encode("latin-1")))

        content_type = self.media_type
        if content_type is not None and populate_content_type:
            if content_type.startswith("text/") and "charset=" not in content_type.lower():
                content_type += "; charset=" + self.charset
            raw_headers.append((b"content-type", content_type.encode("latin-1")))

        self.raw_headers = raw_headers

    @property
    def headers(self) -> MutableHeaders:
        if not hasattr(self, "_headers"):
            self._headers = MutableHeaders(raw=self.raw_headers)
        return self._headers

    def set_cookie(
        self,
        key: str,
        value: str = "",
        max_age: int | None = None,
        expires: datetime | str | int | None = None,
        path: str | None = "/",
        domain: str | None = None,
        secure: bool = False,
        httponly: bool = False,
        samesite: Literal["lax", "strict", "none"] | None = "lax",
        partitioned: bool = False,
    ) -> None:
        cookie: http.cookies.BaseCookie[str] = http.cookies.SimpleCookie()
        cookie[key] = value
        if max_age is not None:
            cookie[key]["max-age"] = max_age
        if expires is not None:
            if isinstance(expires, datetime):
                cookie[key]["expires"] = format_datetime(expires, usegmt=True)
            else:
                cookie[key]["expires"] = expires
        if path is not None:
            cookie[key]["path"] = path
        if domain is not None:
            cookie[key]["domain"] = domain
        if secure:
            cookie[key]["secure"] = True
        if httponly:
            cookie[key]["httponly"] = True
        if samesite is not None:
            assert samesite.lower() in [
                "strict",
                "lax",
                "none",
            ], "samesite must be either 'strict', 'lax' or 'none'"
            cookie[key]["samesite"] = samesite
        if partitioned:
            if sys.version_info < (3, 14):
                raise ValueError("Partitioned cookies are only supported in Python 3.14 and above.")  # pragma: no cover
            cookie[key]["partitioned"] = True  # pragma: no cover

        cookie_val = cookie.output(header="").strip()
        self.raw_headers.append((b"set-cookie", cookie_val.encode("latin-1")))

    def delete_cookie(
        self,
        key: str,
        path: str = "/",
        domain: str | None = None,
        secure: bool = False,
        httponly: bool = False,
        samesite: Literal["lax", "strict", "none"] | None = "lax",
        partitioned: bool = False,
    ) -> None:
        self.set_cookie(
            key,
            max_age=0,
            expires=0,
            path=path,
            domain=domain,
            secure=secure,
            httponly=httponly,
            samesite=samesite,
            partitioned=partitioned,
        )

    def _wrap_websocket_denial_send(self, send: Send) -> Send:
        async def wrapped(message: Message) -> None:
            message_type = message["type"]
            if message_type in {"http.response.start", "http.response.body"}:  # pragma: no branch
                message = {**message, "type": "websocket." + message_type}
            await send(message)

        return wrapped

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "websocket":
            send = self._wrap_websocket_denial_send(send)
        await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
        await send({"type": "http.response.body", "body": self.body})

        if self.background is not None:
            await self.background()


class HTMLResponse(Response):
    media_type = "text/html"


class PlainTextResponse(Response):
    media_type = "text/plain"


class JSONResponse(Response):
    media_type = "application/json"

    def __init__(
        self,
        content: Any,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        super().__init__(content, status_code, headers, media_type, background)

    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        ).encode("utf-8")


class RedirectResponse(Response):
    def __init__(
        self,
        url: str | URL,
        status_code: int = 307,
        headers: Mapping[str, str] | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        super().__init__(content=b"", status_code=status_code, headers=headers, background=background)
        self.headers["location"] = quote(str(url), safe=":/%#?=@[]!$&'()*+,;")


Content = str | bytes | memoryview
SyncContentStream = Iterable[Content]
AsyncContentStream = AsyncIterable[Content]
ContentStream = AsyncContentStream | SyncContentStream


class StreamingResponse(Response):
    body_iterator: AsyncContentStream

    def __init__(
        self,
        content: ContentStream,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
        max_buffer_size: int | None = None,
        buffer_timeout: float | None = None,
    ) -> None:
        if max_buffer_size is not None and max_buffer_size <= 0:
            raise ValueError("max_buffer_size must be a positive integer")
        if buffer_timeout is not None and buffer_timeout <= 0:
            raise ValueError("buffer_timeout must be a positive number of seconds")

        if isinstance(content, AsyncIterable):
            self.body_iterator = content
        else:
            self.body_iterator = iterate_in_threadpool(content)
        self.status_code = status_code
        self.media_type = self.media_type if media_type is None else media_type
        self.background = background
        self.max_buffer_size = max_buffer_size
        self.buffer_timeout = buffer_timeout
        self.init_headers(headers)

    async def listen_for_disconnect(self, receive: Receive) -> None:
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                break

    async def stream_response(self, send: Send, receive: Receive | None = None) -> None:
        await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})

        if self.max_buffer_size is None and self.buffer_timeout is None:
            await self._stream_response(send)
            return

        await self._coalesce_stream(send, receive)

    async def _stream_response(self, send: Send) -> None:
        async for chunk in self.body_iterator:
            if not isinstance(chunk, bytes | memoryview):
                chunk = chunk.encode(self.charset)
            await send({"type": "http.response.body", "body": chunk, "more_body": True})

        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def _coalesce_stream(self, send: Send, receive: Receive | None = None) -> None:
        chunk_sender, chunk_receiver = anyio.create_memory_object_stream[
            tuple[bytes | memoryview, anyio.Event] | None
        ](0)
        control_sender, control_receiver = anyio.create_memory_object_stream[str](0)

        max_buffer_size = self.max_buffer_size
        buffer_timeout = self.buffer_timeout
        buffer: list[bytes | memoryview] = []
        buffered_size = 0
        deadline: float | None = None
        body_iterator = self.body_iterator.__aiter__()

        async def produce() -> None:
            try:
                while True:
                    try:
                        chunk = await body_iterator.__anext__()
                    except StopAsyncIteration:
                        break

                    if not isinstance(chunk, bytes | memoryview):
                        chunk = chunk.encode(self.charset)
                    if chunk:
                        acknowledged = anyio.Event()
                        await chunk_sender.send((chunk, acknowledged))
                        await acknowledged.wait()
                    else:
                        await anyio.sleep(0)
            finally:
                aclose = getattr(body_iterator, "aclose", None)
                if aclose is not None:
                    with anyio.CancelScope(shield=True), suppress(StopAsyncIteration):
                        await aclose()

            await chunk_sender.send(None)

        async def disconnect(receive: Receive) -> None:
            await self.listen_for_disconnect(receive)
            await control_sender.send("disconnect")

        async def consume() -> None:
            nonlocal buffered_size, deadline

            while True:
                deadline_cancelled = False
                item: tuple[bytes | memoryview, anyio.Event] | None = None
                if deadline is None:
                    item = await chunk_receiver.receive()
                else:
                    remaining_time = deadline - anyio.current_time()
                    with anyio.move_on_after(remaining_time) as timeout_scope:
                        item = await chunk_receiver.receive()
                    deadline_cancelled = timeout_scope.cancel_called

                if deadline_cancelled:
                    if anyio.current_time() >= deadline:
                        await flush_buffer()
                        continue
                    raise anyio.get_cancelled_exc_class()()

                if item is None:
                    await flush_buffer()
                    await send({"type": "http.response.body", "body": b"", "more_body": False})
                    await control_sender.send("done")
                    return

                chunk, acknowledged = item
                chunk_size = len(chunk)
                try:
                    if max_buffer_size is not None and (
                        buffered_size >= max_buffer_size or chunk_size >= max_buffer_size
                    ):
                        await flush_buffer()
                        await send_body(chunk)
                    else:
                        buffer.append(chunk)
                        buffered_size += chunk_size
                        if deadline is None:
                            deadline = (
                                anyio.current_time() + buffer_timeout
                                if buffer_timeout is not None
                                else None
                            )
                        if max_buffer_size is not None and buffered_size >= max_buffer_size:
                            await flush_buffer()
                finally:
                    acknowledged.set()

        async def send_body(chunk: bytes | memoryview) -> None:
            await send({"type": "http.response.body", "body": chunk, "more_body": True})

        async def flush_buffer() -> None:
            nonlocal buffered_size, deadline
            if not buffer:
                deadline = None
                return

            body = b"".join(buffer) if len(buffer) > 1 else buffer[0]
            buffer.clear()
            buffered_size = 0
            deadline = None
            await send_body(body)

        try:
            async with create_collapsing_task_group() as task_group:
                if receive is not None:
                    task_group.start_soon(disconnect, receive)
                task_group.start_soon(produce)
                task_group.start_soon(consume)
                await control_receiver.receive()
                task_group.cancel_scope.cancel()
        finally:
            chunk_sender.close()
            chunk_receiver.close()
            control_sender.close()
            control_receiver.close()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "websocket":
            send = self._wrap_websocket_denial_send(send)
            await self.stream_response(send)
            if self.background is not None:
                await self.background()
            return

        spec_version = tuple(map(int, scope.get("asgi", {}).get("spec_version", "2.0").split(".")))
        coalescing_enabled = self.max_buffer_size is not None or self.buffer_timeout is not None

        if spec_version >= (2, 4):
            try:
                await self.stream_response(send)
            except OSError:
                raise ClientDisconnect()
        else:
            if not coalescing_enabled:
                async with create_collapsing_task_group() as task_group:

                    async def wrap(func: Callable[[], Awaitable[None]]) -> None:
                        await func()
                        task_group.cancel_scope.cancel()

                    task_group.start_soon(wrap, partial(self.stream_response, send))
                    await wrap(partial(self.listen_for_disconnect, receive))
            else:
                await self.stream_response(send, receive)

        if self.background is not None:
            await self.background()


class MalformedRangeHeader(Exception):
    def __init__(self, content: str = "Malformed range header.") -> None:
        self.content = content


class RangeNotSatisfiable(Exception):
    def __init__(self, max_size: int) -> None:
        self.max_size = max_size


class FileResponse(Response):
    chunk_size = 64 * 1024
    max_ranges = 100

    def __init__(
        self,
        path: str | os.PathLike[str],
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
        background: BackgroundTask | None = None,
        filename: str | None = None,
        stat_result: os.stat_result | None = None,
        content_disposition_type: str = "attachment",
    ) -> None:
        self.path = path
        self.status_code = status_code
        self.filename = filename
        if media_type is None:
            media_type = guess_type(filename or path)[0] or "application/octet-stream"
        self.media_type = media_type
        self.background = background
        self.init_headers(headers)
        self.headers.setdefault("accept-ranges", "bytes")
        if self.filename is not None:
            content_disposition_filename = quote(self.filename)
            if content_disposition_filename != self.filename:
                content_disposition = f"{content_disposition_type}; filename*=utf-8''{content_disposition_filename}"
            else:
                content_disposition = f'{content_disposition_type}; filename="{self.filename}"'
            self.headers.setdefault("content-disposition", content_disposition)
        self.stat_result = stat_result
        if stat_result is not None:
            self.set_stat_headers(stat_result)

    def set_stat_headers(self, stat_result: os.stat_result) -> None:
        content_length = str(stat_result.st_size)
        last_modified = formatdate(stat_result.st_mtime, usegmt=True)
        etag_base = str(stat_result.st_mtime) + "-" + str(stat_result.st_size)
        etag = f'"{hashlib.md5(etag_base.encode(), usedforsecurity=False).hexdigest()}"'

        self.headers.setdefault("content-length", content_length)
        self.headers.setdefault("last-modified", last_modified)
        self.headers.setdefault("etag", etag)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        scope_type = scope["type"]
        send_header_only = scope_type == "http" and scope["method"].upper() == "HEAD"
        send_pathsend = scope_type == "http" and "http.response.pathsend" in scope.get("extensions", {})
        if scope_type == "websocket":
            send = self._wrap_websocket_denial_send(send)

        if self.stat_result is None:
            try:
                stat_result = await anyio.to_thread.run_sync(os.stat, self.path)
                self.set_stat_headers(stat_result)
            except FileNotFoundError:
                raise RuntimeError(f"File at path {self.path} does not exist.")
            else:
                mode = stat_result.st_mode
                if not stat.S_ISREG(mode):
                    raise RuntimeError(f"File at path {self.path} is not a file.")
        else:
            stat_result = self.stat_result

        headers = Headers(scope=scope)
        http_range = headers.get("range")
        http_if_range = headers.get("if-range")

        if (
            self.status_code != 200
            or http_range is None
            or (http_if_range is not None and not self._should_use_range(http_if_range))
        ):
            send_file = partial(self._handle_simple, send, send_header_only, send_pathsend)
        else:
            try:
                ranges = self._parse_range_header(http_range, stat_result.st_size)
            except MalformedRangeHeader as exc:
                return await PlainTextResponse(exc.content, status_code=400)(scope, receive, send)
            except RangeNotSatisfiable as exc:
                response = PlainTextResponse(status_code=416, headers={"Content-Range": f"bytes */{exc.max_size}"})
                return await response(scope, receive, send)

            if len(ranges) == 0:
                send_file = partial(self._handle_simple, send, send_header_only, send_pathsend)
            elif len(ranges) == 1:
                start, end = ranges[0]
                send_file = partial(self._handle_single_range, send, start, end, stat_result.st_size, send_header_only)
                send_pathsend = False
            else:
                send_file = partial(self._handle_multiple_ranges, send, ranges, stat_result.st_size, send_header_only)
                send_pathsend = False

        spec_version = tuple(map(int, scope.get("asgi", {}).get("spec_version", "2.0").split(".")))
        if scope_type != "http" or send_header_only or send_pathsend or spec_version >= (2, 4):
            await send_file()
        else:
            async with create_collapsing_task_group() as task_group:

                async def stream_file() -> None:
                    await send_file()
                    task_group.cancel_scope.cancel()

                task_group.start_soon(stream_file)
                while True:
                    if (await receive())["type"] == "http.disconnect":
                        task_group.cancel_scope.cancel()
                        break

        if self.background is not None:
            await self.background()

    # TODO: Remove this wrapper once minimum AnyIO includes https://github.com/agronholm/anyio/pull/1314.
    @asynccontextmanager
    async def _open_file(self) -> AsyncIterator[anyio.AsyncFile[bytes]]:
        file = await anyio.open_file(self.path, mode="rb")
        try:
            yield file
        finally:
            # Closing must finish even when the transfer is cancelled.
            with anyio.CancelScope(shield=True):
                await file.aclose()

    async def _handle_simple(self, send: Send, send_header_only: bool, send_pathsend: bool) -> None:
        await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
        if send_header_only:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        elif send_pathsend:
            await send({"type": "http.response.pathsend", "path": str(self.path)})
        else:
            async with self._open_file() as file:
                more_body = True
                while more_body:
                    chunk = await file.read(self.chunk_size)
                    more_body = len(chunk) == self.chunk_size
                    await send({"type": "http.response.body", "body": chunk, "more_body": more_body})

    async def _handle_single_range(
        self, send: Send, start: int, end: int, file_size: int, send_header_only: bool
    ) -> None:
        headers = MutableHeaders(raw=list(self.raw_headers))
        headers["content-range"] = f"bytes {start}-{end - 1}/{file_size}"
        headers["content-length"] = str(end - start)
        await send({"type": "http.response.start", "status": 206, "headers": headers.raw})
        if send_header_only:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        else:
            async with self._open_file() as file:
                await file.seek(start)
                more_body = True
                while more_body:
                    chunk = await file.read(min(self.chunk_size, end - start))
                    start += len(chunk)
                    more_body = len(chunk) == self.chunk_size and start < end
                    await send({"type": "http.response.body", "body": chunk, "more_body": more_body})

    async def _handle_multiple_ranges(
        self,
        send: Send,
        ranges: list[tuple[int, int]],
        file_size: int,
        send_header_only: bool,
    ) -> None:
        # In firefox and chrome, they use boundary with 95-96 bits entropy (that's roughly 13 bytes).
        boundary = token_hex(13)
        content_length, header_generator = self.generate_multipart(
            ranges, boundary, file_size, self.headers["content-type"]
        )
        headers = MutableHeaders(raw=list(self.raw_headers))
        headers["content-type"] = f"multipart/byteranges; boundary={boundary}"
        headers["content-length"] = str(content_length)
        await send({"type": "http.response.start", "status": 206, "headers": headers.raw})
        if send_header_only:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        else:
            async with self._open_file() as file:
                for start, end in ranges:
                    await send({"type": "http.response.body", "body": header_generator(start, end), "more_body": True})
                    await file.seek(start)
                    while start < end:
                        chunk = await file.read(min(self.chunk_size, end - start))
                        if not chunk:
                            raise RuntimeError(f"File at path {self.path} is shorter than expected.")
                        start += len(chunk)
                        await send({"type": "http.response.body", "body": chunk, "more_body": True})
                    await send({"type": "http.response.body", "body": b"\r\n", "more_body": True})
                await send(
                    {
                        "type": "http.response.body",
                        "body": f"--{boundary}--".encode("latin-1"),
                        "more_body": False,
                    }
                )

    def _should_use_range(self, http_if_range: str) -> bool:
        if http_if_range.startswith("W/"):
            return False
        return http_if_range == self.headers["last-modified"] or http_if_range == self.headers["etag"]

    @classmethod
    def _parse_range_header(cls, http_range: str, file_size: int) -> list[tuple[int, int]]:
        ranges: list[tuple[int, int]] = []
        try:
            units, range_ = http_range.split("=", 1)
        except ValueError:
            raise MalformedRangeHeader()

        units = units.strip().lower()

        if units != "bytes":
            raise MalformedRangeHeader("Only support bytes range")

        if range_.count(",") + 1 > cls.max_ranges:
            return []

        ranges = cls._parse_ranges(range_, file_size)

        if len(ranges) == 0:
            raise MalformedRangeHeader("Range header: range must be requested")

        if any(not (0 <= start < file_size) for start, _ in ranges):
            raise RangeNotSatisfiable(file_size)

        if any(start >= end for start, end in ranges):
            raise MalformedRangeHeader("Range header: start must be less than end")

        if len(ranges) == 1:
            return ranges

        # Merge overlapping ranges
        ranges.sort()
        result: list[tuple[int, int]] = [ranges[0]]
        for start, end in ranges[1:]:
            last_start, last_end = result[-1]
            if start <= last_end:
                result[-1] = (last_start, max(last_end, end))
            else:
                result.append((start, end))

        return result

    @classmethod
    def _parse_ranges(cls, range_: str, file_size: int) -> list[tuple[int, int]]:
        ranges: list[tuple[int, int]] = []

        for part in range_.split(","):
            part = part.strip()

            # If the range is empty or a single dash, we ignore it.
            if not part or part == "-":
                continue

            # If the range is not in the format "start-end", we ignore it.
            if "-" not in part:
                continue

            start_str, end_str = part.split("-", 1)
            start_str = start_str.strip()
            end_str = end_str.strip()

            try:
                start = int(start_str) if start_str else max(file_size - int(end_str), 0)
                end = int(end_str) + 1 if start_str and end_str and int(end_str) < file_size else file_size
                ranges.append((start, end))
            except ValueError:
                # If the range is not numeric, we ignore it.
                continue

        return ranges

    def generate_multipart(
        self,
        ranges: Sequence[tuple[int, int]],
        boundary: str,
        max_size: int,
        content_type: str,
    ) -> tuple[int, Callable[[int, int], bytes]]:
        r"""
        Multipart response headers generator.

        ```
        --{boundary}\r\n
        Content-Type: {content_type}\r\n
        Content-Range: bytes {start}-{end-1}/{max_size}\r\n
        \r\n
        ..........content...........\r\n
        --{boundary}\r\n
        Content-Type: {content_type}\r\n
        Content-Range: bytes {start}-{end-1}/{max_size}\r\n
        \r\n
        ..........content...........\r\n
        --{boundary}--
        ```
        """
        boundary_len = len(boundary)
        static_header_part_len = 49 + boundary_len + len(content_type) + len(str(max_size))
        content_length = sum(
            (len(str(start)) + len(str(end - 1)) + static_header_part_len)  # Headers
            + (end - start)  # Content
            for start, end in ranges
        ) + (
            4 + boundary_len  # --boundary--
        )
        return (
            content_length,
            lambda start, end: (
                f"""\
--{boundary}\r
Content-Type: {content_type}\r
Content-Range: bytes {start}-{end - 1}/{max_size}\r
\r
"""
            ).encode("latin-1"),
        )
