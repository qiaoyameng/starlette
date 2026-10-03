from __future__ import annotations

import datetime as dt
import os
import re
import sys
import time
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from dataclasses import dataclass
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any, Literal

import anyio
import httpx2 as httpx
import pytest
from python_multipart import MultipartParser

from starlette import status
from starlette.background import BackgroundTask
from starlette.datastructures import Headers
from starlette.requests import ClientDisconnect, Request
from starlette.responses import FileResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from starlette.testclient import TestClient
from starlette.types import Message, Receive, Scope, Send
from tests.types import TestClientFactory


def test_text_response(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response("hello, world", media_type="text/plain")
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "hello, world"


def test_bytes_response(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response(b"xxxxx", media_type="image/png")
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.content == b"xxxxx"


def test_json_none_response(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = JSONResponse(None)
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.json() is None
    assert response.content == b"null"


def test_redirect_response(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["path"] == "/":
            response = Response("hello, world", media_type="text/plain")
        else:
            response = RedirectResponse("/")
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/redirect")
    assert response.text == "hello, world"
    assert response.url == "http://testserver/"


def test_quoting_redirect_response(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["path"] == "/I ♥ Starlette/":
            response = Response("hello, world", media_type="text/plain")
        else:
            response = RedirectResponse("/I ♥ Starlette/")
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/redirect")
    assert response.text == "hello, world"
    assert response.url == "http://testserver/I%20%E2%99%A5%20Starlette/"


def test_redirect_response_content_length_header(
    test_client_factory: TestClientFactory,
) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["path"] == "/":
            response = Response("hello", media_type="text/plain")  # pragma: no cover
        else:
            response = RedirectResponse("/")
        await response(scope, receive, send)

    client: TestClient = test_client_factory(app)
    response = client.request("GET", "/redirect", follow_redirects=False)
    assert response.url == "http://testserver/redirect"
    assert response.headers["content-length"] == "0"


def test_streaming_response(test_client_factory: TestClientFactory) -> None:
    filled_by_bg_task = ""

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        async def numbers(minimum: int, maximum: int) -> AsyncIterator[str]:
            for i in range(minimum, maximum + 1):
                yield str(i)
                if i != maximum:
                    yield ", "
                await anyio.sleep(0)

        async def numbers_for_cleanup(start: int = 1, stop: int = 5) -> None:
            nonlocal filled_by_bg_task
            async for thing in numbers(start, stop):
                filled_by_bg_task = filled_by_bg_task + thing

        cleanup_task = BackgroundTask(numbers_for_cleanup, start=6, stop=9)
        generator = numbers(1, 5)
        response = StreamingResponse(generator, media_type="text/plain", background=cleanup_task)
        await response(scope, receive, send)

    assert filled_by_bg_task == ""
    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "1, 2, 3, 4, 5"
    assert filled_by_bg_task == "6, 7, 8, 9"


def test_streaming_response_custom_iterator(
    test_client_factory: TestClientFactory,
) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        class CustomAsyncIterator:
            def __init__(self) -> None:
                self._called = 0

            def __aiter__(self) -> AsyncIterator[str]:
                return self

            async def __anext__(self) -> str:
                if self._called == 5:
                    raise StopAsyncIteration()
                self._called += 1
                return str(self._called)

        response = StreamingResponse(CustomAsyncIterator(), media_type="text/plain")
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "12345"


def test_streaming_response_custom_iterable(
    test_client_factory: TestClientFactory,
) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        class CustomAsyncIterable:
            async def __aiter__(self) -> AsyncIterator[str | bytes]:
                for i in range(5):
                    yield str(i + 1)

        response = StreamingResponse(CustomAsyncIterable(), media_type="text/plain")
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "12345"


def test_sync_streaming_response(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        def numbers(minimum: int, maximum: int) -> Iterator[str]:
            for i in range(minimum, maximum + 1):
                yield str(i)
                if i != maximum:
                    yield ", "

        generator = numbers(1, 5)
        response = StreamingResponse(generator, media_type="text/plain")
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "1, 2, 3, 4, 5"


def test_response_headers(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        headers = {"x-header-1": "123", "x-header-2": "456"}
        response = Response("hello, world", media_type="text/plain", headers=headers)
        response.headers["x-header-2"] = "789"
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.headers["x-header-1"] == "123"
    assert response.headers["x-header-2"] == "789"


def test_response_phrase(test_client_factory: TestClientFactory) -> None:
    app = Response(status_code=204)
    client = test_client_factory(app)
    response = client.get("/")
    assert response.reason_phrase == "No Content"

    app = Response(b"", status_code=123)
    client = test_client_factory(app)
    response = client.get("/")
    assert response.reason_phrase == ""


def test_file_response(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    path = tmp_path / "xyz"
    content = b"<file content>" * 1000
    path.write_bytes(content)

    filled_by_bg_task = ""

    async def numbers(minimum: int, maximum: int) -> AsyncIterator[str]:
        for i in range(minimum, maximum + 1):
            yield str(i)
            if i != maximum:
                yield ", "
            await anyio.sleep(0)

    async def numbers_for_cleanup(start: int = 1, stop: int = 5) -> None:
        nonlocal filled_by_bg_task
        async for thing in numbers(start, stop):
            filled_by_bg_task = filled_by_bg_task + thing

    cleanup_task = BackgroundTask(numbers_for_cleanup, start=6, stop=9)

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = FileResponse(path=path, filename="example.png", background=cleanup_task)
        await response(scope, receive, send)

    assert filled_by_bg_task == ""
    client = test_client_factory(app)
    response = client.get("/")
    expected_disposition = 'attachment; filename="example.png"'
    assert response.status_code == status.HTTP_200_OK
    assert response.content == content
    assert response.headers["content-type"] == "image/png"
    assert response.headers["content-disposition"] == expected_disposition
    assert "content-length" in response.headers
    assert "last-modified" in response.headers
    assert "etag" in response.headers
    assert filled_by_bg_task == "6, 7, 8, 9"


@pytest.mark.anyio
async def test_file_response_on_head_method(tmp_path: Path) -> None:
    path = tmp_path / "xyz"
    content = b"<file content>" * 1000
    path.write_bytes(content)

    app = FileResponse(path=path, filename="example.png")

    async def receive() -> Message:  # type: ignore[empty-body]
        ...  # pragma: no cover

    async def send(message: Message) -> None:
        if message["type"] == "http.response.start":
            assert message["status"] == status.HTTP_200_OK
            headers = Headers(raw=message["headers"])
            assert headers["content-type"] == "image/png"
            assert "content-length" in headers
            assert "content-disposition" in headers
            assert "last-modified" in headers
            assert "etag" in headers
        elif message["type"] == "http.response.body":  # pragma: no branch
            assert message["body"] == b""
            assert message["more_body"] is False

    # Since the TestClient drops the response body on HEAD requests, we need to test
    # this directly.
    await app({"type": "http", "method": "head", "headers": [(b"key", b"value")]}, receive, send)


def test_file_response_set_media_type(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    path = tmp_path / "xyz"
    path.write_bytes(b"<file content>")

    # By default, FileResponse will determine the `content-type` based on
    # the filename or path, unless a specific `media_type` is provided.
    app = FileResponse(path=path, filename="example.png", media_type="image/jpeg")
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert response.headers["content-type"] == "image/jpeg"


def test_file_response_default_media_type(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    path = tmp_path / "file.unknownext"
    path.write_bytes(b"<file content>")

    # When the media type cannot be guessed from the filename or path, it
    # falls back to "application/octet-stream" rather than "text/plain".
    app = FileResponse(path=path)
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert response.headers["content-type"] == "application/octet-stream"


def test_file_response_with_directory_raises_error(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    app = FileResponse(path=tmp_path, filename="example.png")
    client = test_client_factory(app)
    with pytest.raises(RuntimeError) as exc_info:
        client.get("/")
    assert "is not a file" in str(exc_info.value)


def test_file_response_with_missing_file_raises_error(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    path = tmp_path / "404.txt"
    app = FileResponse(path=path, filename="404.txt")
    client = test_client_factory(app)
    with pytest.raises(RuntimeError) as exc_info:
        client.get("/")
    assert "does not exist" in str(exc_info.value)


def test_file_response_with_chinese_filename(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    content = b"file content"
    filename = "你好.txt"  # probably "Hello.txt" in Chinese
    path = tmp_path / filename
    path.write_bytes(content)
    app = FileResponse(path=path, filename=filename)
    client = test_client_factory(app)
    response = client.get("/")
    expected_disposition = "attachment; filename*=utf-8''%E4%BD%A0%E5%A5%BD.txt"
    assert response.status_code == status.HTTP_200_OK
    assert response.content == content
    assert response.headers["content-disposition"] == expected_disposition


def test_file_response_with_inline_disposition(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    content = b"file content"
    filename = "hello.txt"
    path = tmp_path / filename
    path.write_bytes(content)
    app = FileResponse(path=path, filename=filename, content_disposition_type="inline")
    client = test_client_factory(app)
    response = client.get("/")
    expected_disposition = 'inline; filename="hello.txt"'
    assert response.status_code == status.HTTP_200_OK
    assert response.content == content
    assert response.headers["content-disposition"] == expected_disposition


def test_file_response_with_range_header(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    content = b"file content"
    filename = "hello.txt"
    path = tmp_path / filename
    path.write_bytes(content)
    etag = '"a_non_autogenerated_etag"'
    app = FileResponse(path=path, filename=filename, headers={"etag": etag})
    client = test_client_factory(app)
    response = client.get("/", headers={"range": "bytes=0-4", "if-range": etag})
    assert response.status_code == status.HTTP_206_PARTIAL_CONTENT
    assert response.content == content[:5]
    assert response.headers["etag"] == etag
    assert response.headers["content-length"] == "5"
    assert response.headers["content-range"] == f"bytes 0-4/{len(content)}"


def test_file_response_ignores_weak_if_range(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    content = b"file content"
    path = tmp_path / "hello.txt"
    path.write_bytes(content)
    etag = 'W/"a_weak_etag"'
    app = FileResponse(path=path, headers={"etag": etag})
    client = test_client_factory(app)

    response = client.get("/", headers={"range": "bytes=0-4", "if-range": etag})

    assert response.status_code == status.HTTP_200_OK
    assert response.content == content
    assert "content-range" not in response.headers


@pytest.mark.anyio
async def test_file_response_with_pathsend(tmpdir: Path) -> None:
    path = tmpdir / "xyz"
    content = b"<file content>" * 1000
    with open(path, "wb") as file:
        file.write(content)

    app = FileResponse(path=path, filename="example.png")

    async def receive() -> Message:  # type: ignore[empty-body]
        ...  # pragma: no cover

    async def send(message: Message) -> None:
        if message["type"] == "http.response.start":
            assert message["status"] == status.HTTP_200_OK
            headers = Headers(raw=message["headers"])
            assert headers["content-type"] == "image/png"
            assert "content-length" in headers
            assert "content-disposition" in headers
            assert "last-modified" in headers
            assert "etag" in headers
        elif message["type"] == "http.response.pathsend":  # pragma: no branch
            assert message["path"] == str(path)

    # Since the TestClient doesn't support `pathsend`, we need to test this directly.
    await app(
        {"type": "http", "method": "get", "headers": [], "extensions": {"http.response.pathsend": {}}},
        receive,
        send,
    )


class ZerocopySendRecorder:
    """ASGI send stand-in that really reads the requested file region.

    It mimics a server implementing the ``http.response.zerocopysend``
    extension by reading ``count`` bytes starting at ``offset`` from the file
    descriptor handed to it, and asserting the file is still open at that
    point. Boundary text arrives as ordinary ``http.response.body`` messages.
    """

    def __init__(self, raise_after: int | None = None) -> None:
        self.messages: list[Message] = []
        self.events: list[tuple[str, bool]] = []
        self._body = bytearray()
        self._sent = 0
        self._raise_after = raise_after

    @property
    def body(self) -> bytes:
        return bytes(self._body)

    async def __call__(self, message: Message) -> None:
        message_type = message["type"]
        self.messages.append(message)
        if message_type == "http.response.start":
            self.events.append(("start", False))
        elif message_type == "http.response.zerocopysend":
            file = message["file"]
            # The file must remain open until the server has consumed it.
            assert not file.closed
            offset = message["offset"]
            count = message["count"]
            data = os.pread(file.fileno(), count, offset)
            assert len(data) == count
            self._body.extend(data)
            self._sent += count
            self.events.append(("zerocopysend", message.get("more_body", False)))
            if self._raise_after is not None and self._sent >= self._raise_after:
                raise OSError("client disconnected")
        elif message_type == "http.response.body":
            data = message.get("body", b"")
            self._body.extend(data)
            self._sent += len(data)
            self.events.append(("body", message.get("more_body", False)))
            if self._raise_after is not None and data and self._sent >= self._raise_after:
                raise OSError("client disconnected")


def _scope(method: str = "GET", range_header: bytes | None = None, *, pathsend: bool = False) -> Scope:
    headers: list[tuple[bytes, bytes]] = []
    if range_header is not None:
        headers.append((b"range", range_header))
    extensions: dict[str, dict[str, Any]] = {"http.response.zerocopysend": {}}
    if pathsend:
        extensions["http.response.pathsend"] = {}
    return {"type": "http", "method": method, "headers": headers, "extensions": extensions}


def _make_receive() -> Any:
    """Receive callable mirroring a request with no further events.

    The first call reports the request; subsequent calls park until the server
    finishes sending, after which cancellation closes the listener.
    """
    called = False

    async def receive() -> Message:
        nonlocal called
        if not called:
            called = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover

    return receive


def _reassemble(messages: list[Message]) -> bytearray:
    """Reassemble a normal (non-zerocopy) response body from body messages."""
    body = bytearray()
    for message in messages:
        if message["type"] == "http.response.body":
            body.extend(message.get("body", b""))
    return body


def _reassemble_zerocopy(path: Path, messages: list[Message]) -> bytearray:
    """Reassemble a zerocopy response, reading each requested region from disk."""
    body = bytearray()
    for message in messages:
        if message["type"] == "http.response.body":
            body.extend(message.get("body", b""))
        elif message["type"] == "http.response.zerocopysend":
            with open(path, "rb") as source:
                body.extend(os.pread(source.fileno(), message["count"], message["offset"]))
    return body


def _boundary_of(messages: list[Message]) -> bytes | None:
    for message in messages:
        if message["type"] != "http.response.start":
            continue
        content_type = Headers(raw=message["headers"]).get("content-type", "")
        if "boundary=" in content_type:
            return content_type.split("boundary=", 1)[1].encode()
    return None


def _normalize_multipart(path: Path, messages: list[Message]) -> bytes:
    """Reassemble a response, canonicalizing a random multipart boundary."""
    body = bytes(_reassemble_zerocopy(path, messages))
    boundary = _boundary_of(messages)
    if boundary is not None:
        body = body.replace(boundary, b"BOUNDARY")
    return body


@pytest.mark.anyio
async def test_file_response_zerocopysend_simple(tmp_path: Path) -> None:
    content = bytes(range(256)) * 40
    path = tmp_path / "file.bin"
    path.write_bytes(content)
    app = FileResponse(path=path)

    recorder = ZerocopySendRecorder()
    await app(_scope(), _make_receive(), recorder)

    start, *rest = recorder.messages
    assert start["type"] == "http.response.start"
    assert start["status"] == status.HTTP_200_OK
    # Exactly one zerocopy message covering the whole file, no body messages.
    zero_messages = [m for m in rest if m["type"] == "http.response.zerocopysend"]
    assert len(zero_messages) == 1
    message = zero_messages[0]
    assert message["offset"] == 0
    assert message["count"] == len(content)
    assert message["more_body"] is False
    assert recorder.body == content
    assert [m["type"] for m in rest] == ["http.response.zerocopysend"]


@pytest.mark.anyio
async def test_file_response_zerocopysend_single_range(tmp_path: Path) -> None:
    content = b"".join(bytes([i % 256]) for i in range(10_000))
    path = tmp_path / "file.bin"
    path.write_bytes(content)
    app = FileResponse(path=path)

    start_offset = 100
    end_offset = 4000  # exclusive
    recorder = ZerocopySendRecorder()
    scope = _scope(range_header=f"bytes={start_offset}-{end_offset - 1}".encode())
    await app(scope, _make_receive(), recorder)

    start_message, *rest = recorder.messages
    assert start_message["status"] == status.HTTP_206_PARTIAL_CONTENT
    response_headers = Headers(raw=start_message["headers"])
    assert response_headers["content-range"] == f"bytes {start_offset}-{end_offset - 1}/{len(content)}"
    assert response_headers["content-length"] == str(end_offset - start_offset)

    zero_messages = [m for m in rest if m["type"] == "http.response.zerocopysend"]
    assert len(zero_messages) == 1
    region = zero_messages[0]
    assert region["offset"] == start_offset
    assert region["count"] == end_offset - start_offset
    assert region["more_body"] is False
    assert recorder.body == content[start_offset:end_offset]


@pytest.mark.anyio
async def test_file_response_zerocopysend_multiple_ranges(tmp_path: Path) -> None:
    content = bytes((i * 7) % 256 for i in range(20_000))
    path = tmp_path / "file.bin"
    path.write_bytes(content)
    app = FileResponse(path=path)

    ranges = [(10, 110), (5000, 5300), (15000, 15001)]
    range_spec = ",".join(f"{start}-{end - 1}" for start, end in ranges).encode()
    range_header = b"bytes=" + range_spec
    recorder = ZerocopySendRecorder()
    await app(_scope(range_header=range_header), _make_receive(), recorder)

    start_message, *rest = recorder.messages
    assert start_message["status"] == status.HTTP_206_PARTIAL_CONTENT
    content_type = Headers(raw=start_message["headers"])["content-type"]
    assert content_type.startswith("multipart/byteranges; boundary=")
    boundary = content_type.split("boundary=", 1)[1].encode()

    zero_messages = [m for m in rest if m["type"] == "http.response.zerocopysend"]
    # One zerocopy message per range with the correct offset/count; boundary
    # chunks in between still travel as ordinary body messages.
    assert [(m["offset"], m["count"], m["more_body"]) for m in zero_messages] == [
        (start, end - start, True) for start, end in ranges
    ]
    body_message_types = [m["type"] for m in rest if m["type"] != "http.response.zerocopysend"]
    assert body_message_types  # boundary text present
    assert body_message_types[0] == "http.response.body"

    # The reassembled multipart payload must match the chunked fallback exactly.
    fallback_messages: list[Message] = []

    async def fallback_send(message: Message) -> None:
        fallback_messages.append(message)

    fallback_scope = {
        "type": "http",
        "method": "GET",
        "headers": [(b"range", range_header)],
        "extensions": {},
    }
    await app(fallback_scope, _make_receive(), fallback_send)
    assert not any(m["type"] == "http.response.zerocopysend" for m in fallback_messages)
    assert _normalize_multipart(path, fallback_messages) == _normalize_multipart(path, recorder.messages)
    assert recorder.body.endswith(b"--" + boundary + b"--")


@pytest.mark.anyio
async def test_file_response_zerocopysend_head_has_no_body(tmp_path: Path) -> None:
    content = b"x" * 1000
    path = tmp_path / "file.bin"
    path.write_bytes(content)
    app = FileResponse(path=path)

    recorder = ZerocopySendRecorder()
    scope = _scope(method="HEAD", range_header=b"bytes=0-99")
    await app(scope, _make_receive(), recorder)

    start_message, *rest = recorder.messages
    assert start_message["status"] == status.HTTP_206_PARTIAL_CONTENT
    assert [m["type"] for m in rest] == ["http.response.body"]
    assert rest[0]["body"] == b""
    assert rest[0]["more_body"] is False
    assert recorder.body == b""

    # A HEAD request without a Range header likewise sends no file region.
    full = FileResponse(path=path)
    full_recorder = ZerocopySendRecorder()
    await full(_scope(method="HEAD"), _make_receive(), full_recorder)
    full_start, *full_rest = full_recorder.messages
    assert full_start["status"] == status.HTTP_200_OK
    assert [m["type"] for m in full_rest] == ["http.response.body"]
    assert full_rest[0]["body"] == b""
    assert not any(m["type"] == "http.response.zerocopysend" for m in full_recorder.messages)
    assert full_recorder.body == b""


@pytest.mark.anyio
async def test_file_response_zerocopysend_matches_chunked_fallback(tmp_path: Path) -> None:
    content = bytes((i * 13 + 5) % 256 for i in range(50_000))
    path = tmp_path / "file.bin"
    path.write_bytes(content)

    range_headers = [
        None,
        b"bytes=0-0",
        b"bytes=10-100",
        b"bytes=40000-",
        b"bytes=0-100, 500-600, 49900-49999",
        b"bytes=0-99, 200-299, 40000-40099, 49990-49999",
    ]
    for range_header in range_headers:
        app = FileResponse(path=path)
        recorder = ZerocopySendRecorder()
        await app(_scope(range_header=range_header), _make_receive(), recorder)

        fallback_messages: list[Message] = []

        async def fallback_send(message: Message) -> None:
            fallback_messages.append(message)

        fallback_scope = {
            "type": "http",
            "method": "GET",
            "headers": [] if range_header is None else [(b"range", range_header)],
            "extensions": {},
        }
        await app(fallback_scope, _make_receive(), fallback_send)

        assert recorder.messages[0]["status"] == fallback_messages[0]["status"]
        zero_headers = Headers(raw=recorder.messages[0]["headers"])
        chunk_headers = Headers(raw=fallback_messages[0]["headers"])
        assert zero_headers["content-length"] == chunk_headers["content-length"]
        assert zero_headers.get("content-range") == chunk_headers.get("content-range")
        zero_type = zero_headers["content-type"].split("; boundary=", 1)[0]
        chunk_type = chunk_headers["content-type"].split("; boundary=", 1)[0]
        assert zero_type == chunk_type
        assert _normalize_multipart(path, fallback_messages) == _normalize_multipart(path, recorder.messages)


@pytest.mark.anyio
async def test_file_response_zerocopysend_prefers_pathsend_for_full_file(tmp_path: Path) -> None:
    content = b"whole file contents"
    path = tmp_path / "file.bin"
    path.write_bytes(content)
    app = FileResponse(path=path)

    messages: list[Message] = []

    async def send(message: Message) -> None:
        messages.append(message)

    await app(_scope(pathsend=True), _make_receive(), send)
    types = [message["type"] for message in messages]
    assert "http.response.pathsend" in types
    assert not any(t == "http.response.zerocopysend" for t in types)


@pytest.mark.anyio
async def test_file_response_zerocopysend_used_when_pathsend_cannot_seek(
    tmp_path: Path,
) -> None:
    # A ranged response must not be served through a whole-file `pathsend`.
    content = b"abcdefghijklmnopqrstuvwxyz"
    path = tmp_path / "file.bin"
    path.write_bytes(content)
    app = FileResponse(path=path)

    messages: list[Message] = []

    async def send(message: Message) -> None:
        messages.append(message)

    await app(_scope(range_header=b"bytes=2-5", pathsend=True), _make_receive(), send)
    types = [message["type"] for message in messages]
    assert "http.response.pathsend" not in types
    zero_messages = [m for m in messages if m["type"] == "http.response.zerocopysend"]
    assert len(zero_messages) == 1
    assert (zero_messages[0]["offset"], zero_messages[0]["count"]) == (2, 4)


@pytest.mark.anyio
async def test_file_response_zerocopysend_closes_file_after_send(tmp_path: Path) -> None:
    path = tmp_path / "file.bin"
    path.write_bytes(b"z" * (4 * FileResponse.chunk_size))
    opened: list[anyio.AsyncFile[bytes]] = []
    original_open = anyio.open_file

    async def tracking_open(*args: Any, **kwargs: Any) -> anyio.AsyncFile[bytes]:
        file = await original_open(*args, **kwargs)
        opened.append(file)
        return file

    app = FileResponse(path=path)
    recorder = ZerocopySendRecorder()

    with anyio.fail_after(5):
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(anyio, "open_file", tracking_open)
            await app(_scope(range_header=b"bytes=0-"), _make_receive(), recorder)

    assert len(opened) == 1
    assert opened[0].closed
    assert recorder.body == path.read_bytes()


@pytest.mark.anyio
async def test_file_response_zerocopysend_send_error_closes_file(tmp_path: Path) -> None:
    path = tmp_path / "file.bin"
    path.write_bytes(b"z" * (8 * FileResponse.chunk_size))
    opened: list[anyio.AsyncFile[bytes]] = []
    original_open = anyio.open_file

    async def tracking_open(*args: Any, **kwargs: Any) -> anyio.AsyncFile[bytes]:
        file = await original_open(*args, **kwargs)
        opened.append(file)
        return file

    app = FileResponse(path=path)
    recorder = ZerocopySendRecorder(raise_after=1)

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    # spec 2.4 surfaces send failures directly; there is no disconnect listener.
    scope = _scope()
    scope["asgi"] = {"spec_version": "2.4"}
    with anyio.fail_after(5):
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(anyio, "open_file", tracking_open)
            with pytest.raises(OSError):
                await app(scope, receive, recorder)

    assert len(opened) == 1
    assert opened[0].closed


@pytest.mark.anyio
async def test_file_response_zerocopysend_background_after_send(tmp_path: Path) -> None:
    path = tmp_path / "file.bin"
    path.write_bytes(b"background ordering")
    order: list[str] = []

    async def background() -> None:
        order.append("background")

    app = FileResponse(path=path, background=BackgroundTask(background))
    recorder = ZerocopySendRecorder()

    async def wrapped_send(message: Message) -> None:
        if message["type"] == "http.response.zerocopysend":
            order.append("zerocopysend")
        await recorder(message)

    await app(_scope(), _make_receive(), wrapped_send)
    assert order == ["zerocopysend", "background"]


@pytest.fixture
def file_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, list[anyio.AsyncFile[bytes]]]:
    path = tmp_path / "file.bin"
    path.write_bytes(b"x" * (8 * FileResponse.chunk_size))
    files: list[anyio.AsyncFile[bytes]] = []
    open_file = anyio.open_file

    async def track_open_file(path: str | os.PathLike[str], mode: Literal["rb"]) -> anyio.AsyncFile[bytes]:
        file = await open_file(path, mode=mode)
        files.append(file)
        return file

    monkeypatch.setattr(anyio, "open_file", track_open_file)
    return path, files


@pytest.fixture(params=[None, b"bytes=0-", b"bytes=0-131071,196608-524287"])
def scope(request: pytest.FixtureRequest) -> Scope:
    return {
        "type": "http",
        "method": "GET",
        "headers": [] if request.param is None else [(b"range", request.param)],
        "extensions": {"http.response.pathsend": {}} if request.param is not None else {},
    }


@pytest.mark.anyio
@pytest.mark.parametrize("spec_version", [None, "2.3"])
async def test_file_response_stops_on_disconnect(
    file_path: tuple[Path, list[anyio.AsyncFile[bytes]]], scope: Scope, spec_version: str | None
) -> None:
    path, files = file_path
    if spec_version is not None:
        scope["asgi"] = {"spec_version": spec_version}
    disconnected = anyio.Event()
    received_request = False
    submitted = 0
    background_ran = False

    async def receive() -> Message:
        nonlocal received_request
        if not received_request:
            received_request = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message: Message) -> None:
        nonlocal submitted
        if message["type"] == "http.response.body":
            submitted += len(message["body"])
            if submitted >= FileResponse.chunk_size:
                disconnected.set()
                await anyio.sleep_forever()

    async def cleanup() -> None:
        nonlocal background_ran
        assert len(files) == 1
        assert files[0].closed
        await anyio.sleep(0)
        background_ran = True

    with anyio.fail_after(5):
        await FileResponse(path, background=BackgroundTask(cleanup))(scope, receive, send)

    assert disconnected.is_set()
    assert background_ran


@pytest.mark.anyio
@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
async def test_file_response_closes_on_cancellation(
    file_path: tuple[Path, list[anyio.AsyncFile[bytes]]], scope: Scope, spec_version: str
) -> None:
    path, files = file_path
    scope["asgi"] = {"spec_version": spec_version}

    async def receive() -> Message:
        await anyio.sleep_forever()
        pytest.fail("The disconnect listener should be cancelled")  # pragma: no cover - sleep never returns

    async def send(message: Message) -> None:
        if message["type"] == "http.response.body":
            cancel_scope.cancel()
            await anyio.sleep_forever()

    async def cleanup() -> None:
        pytest.fail(
            "Background tasks should not run after external cancellation"
        )  # pragma: no cover - failure sentinel

    with anyio.fail_after(5), anyio.CancelScope() as cancel_scope:
        await FileResponse(path, background=BackgroundTask(cleanup))(scope, receive, send)

    assert cancel_scope.cancelled_caught
    assert len(files) == 1
    assert files[0].closed


@pytest.mark.anyio
@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
async def test_file_response_closes_on_send_error(
    file_path: tuple[Path, list[anyio.AsyncFile[bytes]]], scope: Scope, spec_version: str
) -> None:
    path, files = file_path
    scope["asgi"] = {"spec_version": spec_version}
    error = OSError("Disconnected")

    async def receive() -> Message:
        assert spec_version != "2.4"
        await anyio.sleep_forever()
        pytest.fail("The disconnect listener should be cancelled")  # pragma: no cover - sleep never returns

    async def send(message: Message) -> None:
        if message["type"] == "http.response.body":
            raise error

    with anyio.fail_after(5), pytest.raises(OSError) as exc:
        await FileResponse(path)(scope, receive, send)

    assert exc.value is error
    assert len(files) == 1
    assert files[0].closed


def test_set_cookie(test_client_factory: TestClientFactory, monkeypatch: pytest.MonkeyPatch) -> None:
    # Mock time used as a reference for `Expires` by stdlib `SimpleCookie`.
    mocked_now = dt.datetime(2037, 1, 22, 12, 0, 0, tzinfo=dt.UTC)
    monkeypatch.setattr(time, "time", lambda: mocked_now.timestamp())

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response("Hello, world!", media_type="text/plain")
        response.set_cookie(
            "mycookie",
            "myvalue",
            max_age=10,
            expires=10,
            path="/",
            domain="localhost",
            secure=True,
            httponly=True,
            samesite="none",
            partitioned=True if sys.version_info >= (3, 14) else False,
        )
        await response(scope, receive, send)

    partitioned_text = "Partitioned; " if sys.version_info >= (3, 14) else ""

    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "Hello, world!"
    assert (
        response.headers["set-cookie"] == "mycookie=myvalue; Domain=localhost; expires=Thu, 22 Jan 2037 12:00:10 GMT; "
        f"HttpOnly; Max-Age=10; {partitioned_text}Path=/; SameSite=none; Secure"
    )


@pytest.mark.skipif(sys.version_info >= (3, 14), reason="Only relevant for <3.14")
def test_set_cookie_raises_for_invalid_python_version(
    test_client_factory: TestClientFactory,
) -> None:  # pragma: no cover
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response("Hello, world!", media_type="text/plain")
        with pytest.raises(ValueError):
            response.set_cookie("mycookie", "myvalue", partitioned=True)
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "Hello, world!"
    assert response.headers.get("set-cookie") is None


def test_set_cookie_path_none(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response("Hello, world!", media_type="text/plain")
        response.set_cookie("mycookie", "myvalue", path=None)
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "Hello, world!"
    assert response.headers["set-cookie"] == "mycookie=myvalue; SameSite=lax"


def test_set_cookie_samesite_none(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response("Hello, world!", media_type="text/plain")
        response.set_cookie("mycookie", "myvalue", samesite=None)
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "Hello, world!"
    assert response.headers["set-cookie"] == "mycookie=myvalue; Path=/"


@pytest.mark.parametrize(
    "expires",
    [
        pytest.param(dt.datetime(2037, 1, 22, 12, 0, 10, tzinfo=dt.UTC), id="datetime"),
        pytest.param("Thu, 22 Jan 2037 12:00:10 GMT", id="str"),
        pytest.param(10, id="int"),
    ],
)
def test_expires_on_set_cookie(
    test_client_factory: TestClientFactory,
    monkeypatch: pytest.MonkeyPatch,
    expires: str,
) -> None:
    # Mock time used as a reference for `Expires` by stdlib `SimpleCookie`.
    mocked_now = dt.datetime(2037, 1, 22, 12, 0, 0, tzinfo=dt.UTC)
    monkeypatch.setattr(time, "time", lambda: mocked_now.timestamp())

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        response = Response("Hello, world!", media_type="text/plain")
        response.set_cookie("mycookie", "myvalue", expires=expires)
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    cookie = SimpleCookie(response.headers.get("set-cookie"))
    assert cookie["mycookie"]["expires"] == "Thu, 22 Jan 2037 12:00:10 GMT"


def test_delete_cookie(test_client_factory: TestClientFactory) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope, receive)
        response = Response("Hello, world!", media_type="text/plain")
        if request.cookies.get("mycookie"):
            response.delete_cookie("mycookie")
        else:
            response.set_cookie("mycookie", "myvalue")
        await response(scope, receive, send)

    client = test_client_factory(app)
    response = client.get("/")
    assert response.cookies["mycookie"]
    response = client.get("/")
    assert not response.cookies.get("mycookie")


@pytest.mark.parametrize("partitioned", [False, True])
def test_delete_cookie_partitioned(partitioned: bool) -> None:
    response = Response()
    if partitioned and sys.version_info < (3, 14):  # pragma: no cover - Requires Python below 3.14.
        with pytest.raises(ValueError, match="Partitioned cookies are only supported in Python 3.14 and above"):
            response.delete_cookie("mycookie", secure=True, samesite="none", partitioned=partitioned)
        assert "set-cookie" not in response.headers
        return

    response.delete_cookie("mycookie", secure=True, samesite="none", partitioned=partitioned)

    header = response.headers["set-cookie"]
    assert ("Partitioned" in header) is partitioned
    cookie = SimpleCookie(header)["mycookie"]
    assert cookie["max-age"] == "0"
    assert cookie["secure"] is True


def test_populate_headers(test_client_factory: TestClientFactory) -> None:
    app = Response(content="hi", headers={}, media_type="text/html")
    client = test_client_factory(app)
    response = client.get("/")
    assert response.text == "hi"
    assert response.headers["content-length"] == "2"
    assert response.headers["content-type"] == "text/html; charset=utf-8"


def test_head_method(test_client_factory: TestClientFactory) -> None:
    app = Response("hello, world", media_type="text/plain")
    client = test_client_factory(app)
    response = client.head("/")
    assert response.text == ""


def test_empty_response(test_client_factory: TestClientFactory) -> None:
    app = Response()
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert response.content == b""
    assert response.headers["content-length"] == "0"
    assert "content-type" not in response.headers


def test_empty_204_response(test_client_factory: TestClientFactory) -> None:
    app = Response(status_code=204)
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert "content-length" not in response.headers


def test_non_empty_response(test_client_factory: TestClientFactory) -> None:
    app = Response(content="hi")
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert response.headers["content-length"] == "2"


def test_response_do_not_add_redundant_charset(
    test_client_factory: TestClientFactory,
) -> None:
    app = Response(media_type="text/plain; charset=utf-8")
    client = test_client_factory(app)
    response = client.get("/")
    assert response.headers["content-type"] == "text/plain; charset=utf-8"


def test_file_response_known_size(tmp_path: Path, test_client_factory: TestClientFactory) -> None:
    path = tmp_path / "xyz"
    content = b"<file content>" * 1000
    path.write_bytes(content)

    app = FileResponse(path=path, filename="example.png")
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert response.headers["content-length"] == str(len(content))


def test_streaming_response_unknown_size(
    test_client_factory: TestClientFactory,
) -> None:
    app = StreamingResponse(content=iter(["hello", "world"]))
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert "content-length" not in response.headers


def test_streaming_response_known_size(test_client_factory: TestClientFactory) -> None:
    app = StreamingResponse(content=iter(["hello", "world"]), headers={"content-length": "10"})
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert response.headers["content-length"] == "10"


def test_response_memoryview(test_client_factory: TestClientFactory) -> None:
    app = Response(content=memoryview(b"\xc0"))
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert response.content == b"\xc0"


def test_streaming_response_memoryview(test_client_factory: TestClientFactory) -> None:
    app = StreamingResponse(content=iter([memoryview(b"\xc0"), memoryview(b"\xf5")]))
    client: TestClient = test_client_factory(app)
    response = client.get("/")
    assert response.content == b"\xc0\xf5"


@pytest.mark.anyio
async def test_streaming_response_stops_if_receiving_http_disconnect() -> None:
    streamed = 0

    disconnected = anyio.Event()

    async def receive_disconnect() -> Message:
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message: Message) -> None:
        nonlocal streamed
        if message["type"] == "http.response.body":
            streamed += len(message.get("body", b""))
            # Simulate disconnection after download has started
            if streamed >= 16:
                disconnected.set()

    async def stream_indefinitely() -> AsyncIterator[bytes]:
        while True:
            # Need a sleep for the event loop to switch to another task
            await anyio.sleep(0)
            yield b"chunk "

    response = StreamingResponse(content=stream_indefinitely())

    with anyio.move_on_after(1) as cancel_scope:
        await response({"type": "http"}, receive_disconnect, send)
    assert not cancel_scope.cancel_called, "Content streaming should stop itself."


@pytest.mark.anyio
async def test_streaming_response_on_client_disconnects() -> None:
    chunks = bytearray()
    streamed = False

    async def receive_disconnect() -> Message:
        raise NotImplementedError

    async def send(message: Message) -> None:
        nonlocal streamed
        if message["type"] == "http.response.body":
            if not streamed:
                chunks.extend(message.get("body", b""))
                streamed = True
            else:
                raise OSError

    async def stream_indefinitely() -> AsyncGenerator[bytes, None]:
        while True:
            await anyio.sleep(0)
            yield b"chunk"

    stream = stream_indefinitely()
    response = StreamingResponse(content=stream)

    with anyio.move_on_after(1) as cancel_scope:
        with pytest.raises(ClientDisconnect):
            await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive_disconnect, send)
    assert not cancel_scope.cancel_called, "Content streaming should stop itself."
    assert chunks == b"chunk"
    await stream.aclose()


@pytest.mark.anyio
async def test_streaming_response_runs_background_on_websocket_scope() -> None:
    background_called = False
    sent: list[Message] = []

    async def receive() -> Message:
        return {}  # pragma: no cover

    async def send(message: Message) -> None:
        sent.append(message)

    def run_background() -> None:
        nonlocal background_called
        background_called = True

    async def stream() -> AsyncIterator[bytes]:
        yield b"chunk"

    response = StreamingResponse(stream(), background=BackgroundTask(run_background))

    await response({"type": "websocket"}, receive, send)

    assert background_called
    assert [message["type"] for message in sent] == [
        "websocket.http.response.start",
        "websocket.http.response.body",
        "websocket.http.response.body",
    ]


README = """\
# BáiZé

Powerful and exquisite WSGI/ASGI framework/toolkit.

The minimize implementation of methods required in the Web framework. No redundant implementation means that you can freely customize functions without considering the conflict with baize's own implementation.

Under the ASGI/WSGI protocol, the interface of the request object and the response object is almost the same, only need to add or delete `await` in the appropriate place. In addition, it should be noted that ASGI supports WebSocket but WSGI does not.
"""  # noqa: E501


@pytest.fixture
def readme_file(tmp_path: Path) -> Path:
    filepath = tmp_path / "README.txt"
    filepath.write_bytes(README.encode("utf8"))
    return filepath


@pytest.fixture
def file_response_client(readme_file: Path, test_client_factory: TestClientFactory) -> TestClient:
    return test_client_factory(app=FileResponse(str(readme_file)))


def test_file_response_without_range(file_response_client: TestClient) -> None:
    response = file_response_client.get("/")
    assert response.status_code == 200
    assert "content-range" not in response.headers
    assert response.headers["content-length"] == str(len(README.encode("utf8")))
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert response.text == README


def test_file_response_ignores_range_for_non_success_status(
    readme_file: Path, test_client_factory: TestClientFactory
) -> None:
    client = test_client_factory(app=FileResponse(str(readme_file), status_code=404))
    response = client.get("/", headers={"Range": "bytes=0-100"})

    assert response.status_code == 404
    assert "content-range" not in response.headers
    assert response.headers["content-length"] == str(len(README.encode("utf8")))
    assert response.content == README.encode("utf8")

    head_response = client.head("/", headers={"Range": "bytes=0-100"})
    assert head_response.status_code == 404
    assert "content-range" not in head_response.headers
    assert head_response.headers["content-length"] == str(len(README.encode("utf8")))
    assert head_response.content == b""


def test_file_response_head(file_response_client: TestClient) -> None:
    response = file_response_client.head("/")
    assert response.status_code == 200
    assert "content-range" not in response.headers
    assert response.headers["content-length"] == str(len(README.encode("utf8")))
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert response.content == b""


def test_file_response_range(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=0-100"})
    assert response.status_code == 206
    assert response.headers["content-range"] == f"bytes 0-100/{len(README.encode('utf8'))}"
    assert response.headers["content-length"] == "101"
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert response.content == README.encode("utf8")[:101]


def test_file_response_range_head(file_response_client: TestClient) -> None:
    response = file_response_client.head("/", headers={"Range": "bytes=0-100"})
    assert response.status_code == 206
    assert response.headers["content-range"] == f"bytes 0-100/{len(README.encode('utf8'))}"
    assert response.headers["content-length"] == str(101)
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert response.content == b""


def test_file_response_range_multi(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=0-100, 200-300"})
    assert response.status_code == 206
    assert "content-range" not in response.headers
    assert response.headers["content-length"] == "448"
    assert response.headers["content-type"].startswith("multipart/byteranges; boundary=")


def test_file_response_range_multi_head(file_response_client: TestClient) -> None:
    response = file_response_client.head("/", headers={"Range": "bytes=0-100, 200-300"})
    assert response.status_code == 206
    assert "content-range" not in response.headers
    assert response.headers["content-length"] == "448"
    assert response.headers["content-type"].startswith("multipart/byteranges; boundary=")
    assert response.content == b""

    response = file_response_client.head(
        "/",
        headers={"Range": "bytes=200-300", "if-range": response.headers["etag"][:-1]},
    )
    assert response.status_code == 200
    response = file_response_client.head(
        "/",
        headers={"Range": "bytes=200-300", "if-range": response.headers["etag"]},
    )
    assert response.status_code == 206


def test_file_response_range_count_limit(file_response_client: TestClient) -> None:
    ranges = ",".join(f"{index}-{index}" for index in range(0, 200, 2))
    response = file_response_client.get("/", headers={"Range": f"bytes={ranges}"})

    assert response.status_code == 206
    assert response.headers["content-type"].startswith("multipart/byteranges; boundary=")


def test_file_response_range_count_limit_exceeded(file_response_client: TestClient) -> None:
    ranges = ",".join(f"{index}-{index}" for index in range(0, 202, 2))
    response = file_response_client.get("/", headers={"Range": f"bytes={ranges}"})

    assert response.status_code == 200
    assert "content-range" not in response.headers
    assert response.text == README


def test_file_response_range_invalid(file_response_client: TestClient) -> None:
    response = file_response_client.head("/", headers={"Range": "bytes: 0-1000"})
    assert response.status_code == 400


def test_file_response_range_head_max(file_response_client: TestClient) -> None:
    response = file_response_client.head("/", headers={"Range": f"bytes=0-{len(README.encode('utf8')) + 1}"})
    assert response.status_code == 206


def test_file_response_range_416(file_response_client: TestClient) -> None:
    response = file_response_client.head("/", headers={"Range": f"bytes={len(README.encode('utf8')) + 1}-"})
    assert response.status_code == 416
    assert response.headers["Content-Range"] == f"bytes */{len(README.encode('utf8'))}"


def test_file_response_only_support_bytes_range(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "items=0-100"})
    assert response.status_code == 400
    assert response.text == "Only support bytes range"


def test_file_response_range_must_be_requested(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes="})
    assert response.status_code == 400
    assert response.text == "Range header: range must be requested"


def test_file_response_start_must_be_less_than_end(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=100-0"})
    assert response.status_code == 400
    assert response.text == "Range header: start must be less than end"


def test_file_response_inverted_single_byte_range(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=5-4"})
    assert response.status_code == 400
    assert response.text == "Range header: start must be less than end"


def test_file_response_single_byte_range(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=5-5"})
    assert response.status_code == 206
    assert response.headers["content-range"] == f"bytes 5-5/{len(README.encode('utf8'))}"
    assert response.headers["content-length"] == "1"


def test_file_response_merge_ranges(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=0-100, 50-200"})
    assert response.status_code == 206
    assert response.headers["content-length"] == "201"
    assert response.headers["content-range"] == f"bytes 0-200/{len(README.encode('utf8'))}"


@dataclass
class MultipartPart:
    headers: dict[bytes, bytes]
    data: bytes


def parse_multipart_data(data: bytes, boundary: bytes | str) -> list[MultipartPart]:
    parts: list[MultipartPart] = []
    done = False

    current_headers: dict[bytes, bytes] = {}
    current_header_field: bytes = b""

    def on_part_begin() -> None:
        nonlocal current_headers
        current_headers = {}

    def on_part_data(data: bytes, start: int, end: int) -> None:
        parts.append(MultipartPart(current_headers, data[start:end]))

    def on_header_field(data: bytes, start: int, end: int) -> None:
        nonlocal current_header_field
        current_header_field = data[start:end]

    def on_header_value(data: bytes, start: int, end: int) -> None:
        current_headers[current_header_field] = data[start:end]

    def on_end() -> None:
        nonlocal done
        done = True

    parser = MultipartParser(
        boundary,
        dict(
            on_part_begin=on_part_begin,
            on_part_data=on_part_data,
            on_header_field=on_header_field,
            on_header_value=on_header_value,
            on_end=on_end,
        ),
    )
    parser.write(data)
    parser.finalize()
    assert done
    return parts


def test_file_response_insert_ranges(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=100-200, 0-50"})

    assert response.status_code == 206
    assert "content-range" not in response.headers
    assert response.headers["content-type"].startswith("multipart/byteranges; boundary=")
    boundary = response.headers["content-type"].split("boundary=")[1]
    assert response.text.splitlines() == [
        f"--{boundary}",
        "Content-Type: text/plain; charset=utf-8",
        "Content-Range: bytes 0-50/526",
        "",
        "# BáiZé",
        "",
        "Powerful and exquisite WSGI/ASGI framewo",
        f"--{boundary}",
        "Content-Type: text/plain; charset=utf-8",
        "Content-Range: bytes 100-200/526",
        "",
        "ds required in the Web framework. No redundant implementation means that you can freely customize fun",
        f"--{boundary}--",
    ]

    parts = parse_multipart_data(response._content, boundary)
    assert all(
        value == b"text/plain; charset=utf-8"
        for part in parts
        for key, value in part.headers.items()
        if key == b"Content-Type"
    )
    assert len(parts) == 2
    assert parts[0].headers[b"Content-Range"] == b"bytes 0-50/526"
    assert parts[0].data == "# BáiZé\n\nPowerful and exquisite WSGI/ASGI framewo".encode()
    assert parts[1].headers[b"Content-Range"] == b"bytes 100-200/526"
    assert (
        parts[1].data
        == b"ds required in the Web framework. No redundant implementation means that you can freely customize fun"
    )


def test_file_response_range_without_dash(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=100, 0-50"})
    assert response.status_code == 206
    assert response.headers["content-range"] == f"bytes 0-50/{len(README.encode('utf8'))}"


def test_file_response_range_empty_start_and_end(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes= - , 0-50"})
    assert response.status_code == 206
    assert response.headers["content-range"] == f"bytes 0-50/{len(README.encode('utf8'))}"


def test_file_response_range_ignore_non_numeric(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=abc-def, 0-50"})
    assert response.status_code == 206
    assert response.headers["content-range"] == f"bytes 0-50/{len(README.encode('utf8'))}"


def test_file_response_suffix_range(file_response_client: TestClient) -> None:
    # Test suffix range (last N bytes) - line 523 with empty start_str
    response = file_response_client.get("/", headers={"Range": "bytes=-100"})
    assert response.status_code == 206
    file_size = len(README.encode("utf8"))
    assert response.headers["content-range"] == f"bytes {file_size - 100}-{file_size - 1}/{file_size}"
    assert response.headers["content-length"] == "100"
    assert response.content == README.encode("utf8")[-100:]


def test_file_response_suffix_range_larger_than_file(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=-1000"})

    file_size = len(README.encode("utf8"))
    assert response.status_code == 206
    assert response.headers["content-range"] == f"bytes 0-{file_size - 1}/{file_size}"
    assert response.headers["content-length"] == str(file_size)
    assert response.content == README.encode("utf8")


def test_file_response_multiple_calls(file_response_client: TestClient) -> None:
    response = file_response_client.get("/", headers={"Range": "bytes=0-100"})
    assert response.status_code == 206

    response = file_response_client.get("/")
    assert response.status_code == 200
    assert "content-range" not in response.headers
    assert response.headers["content-length"] == str(len(README.encode("utf8")))
    assert response.headers["content-type"] == "text/plain; charset=utf-8"


@pytest.mark.anyio
async def test_file_response_multi_small_chunk_size(readme_file: Path) -> None:
    class SmallChunkSizeFileResponse(FileResponse):
        chunk_size = 10

    app = SmallChunkSizeFileResponse(path=str(readme_file))

    received_chunks: list[bytes] = []
    start_message: dict[str, Any] = {}

    async def receive() -> Message:
        await anyio.sleep_forever()
        pytest.fail("The disconnect listener should be cancelled")  # pragma: no cover - sleep never returns

    async def send(message: Message) -> None:
        if message["type"] == "http.response.start":
            start_message.update(message)
        elif message["type"] == "http.response.body":  # pragma: no branch
            received_chunks.append(message["body"])

    with anyio.fail_after(5):
        await app({"type": "http", "method": "get", "headers": [(b"range", b"bytes=0-15,20-35,35-50")]}, receive, send)
    assert start_message["status"] == 206

    headers = Headers(raw=start_message["headers"])
    assert "content-range" not in headers
    assert headers.get("accept-ranges") == "bytes"
    assert "content-length" in headers
    assert "last-modified" in headers
    assert "etag" in headers
    assert headers["content-type"].startswith("multipart/byteranges; boundary=")
    boundary = headers["content-type"].split("boundary=")[1]

    assert received_chunks == [
        # Send the part headers.
        f"--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Range: bytes 0-15/526\r\n\r\n".encode(),
        # Send the first chunk (10 bytes).
        b"# B\xc3\xa1iZ\xc3\xa9\n",
        # Send the second chunk (6 bytes).
        b"\nPower",
        # Send the new line to separate the parts.
        b"\r\n",
        # Send the part headers. We merge the ranges 20-35 and 35-50 into a single part.
        f"--{boundary}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Range: bytes 20-50/526\r\n\r\n".encode(),
        # Send the first chunk (10 bytes).
        b"and exquis",
        # Send the second chunk (10 bytes).
        b"ite WSGI/A",
        # Send the third chunk (10 bytes).
        b"SGI framew",
        # Send the last chunk (1 byte).
        b"o",
        b"\r\n",
        f"--{boundary}--".encode(),
    ]


@pytest.mark.anyio
async def test_file_response_multi_range_unexpected_eof(tmp_path: Path) -> None:
    path = tmp_path / "file.txt"
    path.write_bytes(b"0123456789")
    response = FileResponse(path, stat_result=path.stat())
    path.write_bytes(b"012")

    transport = httpx.ASGITransport(app=response)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        with (
            anyio.fail_after(1),
            pytest.raises(
                RuntimeError,
                match=re.escape(f"File at path {path} is shorter than expected."),
            ),
        ):
            await client.get("/", headers={"Range": "bytes=0-2,5-7"})
