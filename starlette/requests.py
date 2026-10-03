from __future__ import annotations

import json
import sys
from collections.abc import AsyncGenerator, Iterator, Mapping
from http import cookies as http_cookies
from typing import TYPE_CHECKING, Any, Generic, NoReturn, cast

import anyio

from starlette._utils import AwaitableOrContextManager, AwaitableOrContextManagerWrapper
from starlette.datastructures import URL, Address, FormData, Headers, QueryParams, State
from starlette.exceptions import HTTPException
from starlette.formparsers import FormParser, MultiPartException, MultiPartParser
from starlette.types import Message, Receive, Scope, Send

if TYPE_CHECKING:
    from python_multipart.multipart import parse_options_header

    from starlette.applications import Starlette
    from starlette.middleware.sessions import Session
    from starlette.routing import Router
else:
    try:
        try:
            from python_multipart.multipart import parse_options_header
        except ModuleNotFoundError:  # pragma: no cover
            from multipart.multipart import parse_options_header
    except ModuleNotFoundError:  # pragma: no cover
        parse_options_header = None

if sys.version_info >= (3, 13):  # pragma: no cover
    from typing import TypeVar
else:  # pragma: no cover
    from typing_extensions import TypeVar

SERVER_PUSH_HEADERS_TO_COPY = {
    "accept",
    "accept-encoding",
    "accept-language",
    "cache-control",
    "user-agent",
}


def cookie_parser(cookie_string: str) -> dict[str, str]:
    """
    This function parses a ``Cookie`` HTTP header into a dict of key/value pairs.

    It attempts to mimic browser cookie parsing behavior: browsers and web servers
    frequently disregard the spec (RFC 6265) when setting and reading cookies,
    so we attempt to suit the common scenarios here.

    This function has been adapted from Django 3.1.0.
    Note: we are explicitly _NOT_ using `SimpleCookie.load` because it is based
    on an outdated spec and will fail on lots of input we want to support
    """
    cookie_dict: dict[str, str] = {}
    for chunk in cookie_string.split(";"):
        if "=" in chunk:
            key, val = chunk.split("=", 1)
        else:
            # Assume an empty name per
            # https://bugzilla.mozilla.org/show_bug.cgi?id=169091
            key, val = "", chunk
        key, val = key.strip(), val.strip()
        if key or val:
            # unquote using Python's algorithm.
            cookie_dict[key] = http_cookies._unquote(val)
    return cookie_dict


class ClientDisconnect(Exception):
    pass


StateT = TypeVar("StateT", bound=Mapping[str, Any] | State, default=State)


class HTTPConnection(Mapping[str, Any], Generic[StateT]):
    """
    A base class for incoming HTTP connections, that is used to provide
    any functionality that is common to both `Request` and `WebSocket`.
    """

    def __init__(self, scope: Scope, receive: Receive | None = None) -> None:
        assert scope["type"] in ("http", "websocket")
        self.scope = scope

    def __getitem__(self, key: str) -> Any:
        return self.scope[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.scope)

    def __len__(self) -> int:
        return len(self.scope)

    # Don't use the `abc.Mapping.__eq__` implementation.
    # Connection instances should never be considered equal
    # unless `self is other`.
    __eq__ = object.__eq__
    __hash__ = object.__hash__

    @property
    def app(self) -> Any:
        return self.scope["app"]

    @property
    def url(self) -> URL:
        if not hasattr(self, "_url"):  # pragma: no branch
            self._url = URL(scope=self.scope)
        return self._url

    @property
    def base_url(self) -> URL:
        if not hasattr(self, "_base_url"):
            base_url_scope = dict(self.scope)
            # This is used by request.url_for, it might be used inside a Mount which
            # would have its own child scope with its own root_path, but the base URL
            # for url_for should still be the top level app root path.
            app_root_path = base_url_scope.get("app_root_path", base_url_scope.get("root_path", ""))
            path = app_root_path
            if not path.endswith("/"):
                path += "/"
            base_url_scope["path"] = path
            base_url_scope["query_string"] = b""
            base_url_scope["root_path"] = app_root_path
            self._base_url = URL(scope=base_url_scope)
        return self._base_url

    @property
    def headers(self) -> Headers:
        if not hasattr(self, "_headers"):
            self._headers = Headers(scope=self.scope)
        return self._headers

    @property
    def query_params(self) -> QueryParams:
        if not hasattr(self, "_query_params"):  # pragma: no branch
            self._query_params = QueryParams(self.scope["query_string"])
        return self._query_params

    @property
    def path_params(self) -> dict[str, Any]:
        path_params: dict[str, Any] = self.scope.get("path_params", {})
        return path_params

    @property
    def cookies(self) -> dict[str, str]:
        if not hasattr(self, "_cookies"):
            cookies: dict[str, str] = {}
            cookie_headers = self.headers.getlist("cookie")

            for header in cookie_headers:
                cookies.update(cookie_parser(header))

            self._cookies = cookies
        return self._cookies

    @property
    def client(self) -> Address | None:
        # client is a 2 item tuple of (host, port), None if missing
        host_port = self.scope.get("client")
        if host_port is not None:
            return Address(*host_port)
        return None

    @property
    def session(self) -> dict[str, Any]:
        assert "session" in self.scope, "SessionMiddleware must be installed to access request.session"
        session: Session = self.scope["session"]
        # We keep the hasattr in case people actually use their own `SessionMiddleware` implementation.
        if hasattr(session, "mark_accessed"):  # pragma: no branch
            session.mark_accessed()
        return session

    @property
    def auth(self) -> Any:
        assert "auth" in self.scope, "AuthenticationMiddleware must be installed to access request.auth"
        return self.scope["auth"]

    @property
    def user(self) -> Any:
        assert "user" in self.scope, "AuthenticationMiddleware must be installed to access request.user"
        return self.scope["user"]

    @property
    def state(self) -> StateT:
        if not hasattr(self, "_state"):
            # Ensure 'state' has an empty dict if it's not already populated.
            self.scope.setdefault("state", {})
            # Create a state instance with a reference to the dict in which it should
            # store info
            self._state = State(self.scope["state"])
        return cast(StateT, self._state)

    def url_for(self, name: str, /, **path_params: Any) -> URL:
        url_path_provider: Router | Starlette | None = self.scope.get("router") or self.scope.get("app")
        if url_path_provider is None:
            raise RuntimeError("The `url_for` method can only be used inside a Starlette application or with a router.")
        url_path = url_path_provider.url_path_for(name, **path_params)
        return url_path.make_absolute_url(base_url=self.base_url)


async def empty_receive() -> NoReturn:
    raise RuntimeError("Receive channel has not been made available")


async def empty_send(message: Message) -> NoReturn:
    raise RuntimeError("Send channel has not been made available")


class Request(HTTPConnection[StateT]):
    _form: FormData | None

    def __init__(self, scope: Scope, receive: Receive = empty_receive, send: Send = empty_send):
        super().__init__(scope)
        assert scope["type"] == "http"
        self._receive = receive
        self._send = send
        self._stream_consumed = False
        self._is_disconnected = False
        self._disconnect_event = anyio.Event()
        # Serializes access to `receive()` so that the body-reading path and
        # any number of disconnect observers never call it concurrently.
        self._receive_lock = anyio.Lock()
        # A single `http.request` message peeked by an observer, waiting to be
        # handed off to the body-reading path. Buffering is bounded to this
        # one message, so observing a disconnect never accumulates the body.
        self._stashed_message: Message | None = None
        # Set whenever there is no stashed message waiting to be collected.
        # Replaced with a fresh event on each stash, since `anyio.Event`
        # cannot be cleared.
        self._stash_empty = anyio.Event()
        self._stash_empty.set()
        # Number of body readers currently waiting for `_receive_lock`.
        # Observers defer to them instead of polling while a stashed body
        # message is still waiting to be collected.
        self._body_readers_waiting = 0
        self._form = None

    @property
    def method(self) -> str:
        return cast(str, self.scope["method"])

    @property
    def receive(self) -> Receive:
        return self._receive

    def _set_disconnected(self) -> None:
        self._is_disconnected = True
        self._disconnect_event.set()

    async def _next_message(self) -> Message:
        """
        Return the next ASGI message for the body-reading path.

        A message stashed by a disconnect observer is replayed first: it was
        received before anything that follows, and replaying it never calls
        `receive()`, so it is collected synchronously without `_receive_lock`
        (there is no `await` between reading and clearing the stash). This
        guarantees the body-reading path can always make progress, even while
        an observer is blocked in `receive()` holding the lock. Actual calls
        to `receive()` are serialized through `_receive_lock`, and once the
        disconnect state is known an `http.disconnect` message is synthesized
        instead of consuming another message from the server.
        """
        if self._stashed_message is not None:
            message = self._stashed_message
            self._stashed_message = None
            self._stash_empty.set()
            return message
        if not self._is_disconnected:
            self._body_readers_waiting += 1
            try:
                async with self._receive_lock:
                    if self._stashed_message is not None:
                        message = self._stashed_message
                        self._stashed_message = None
                        self._stash_empty.set()
                        return message
                    if self._is_disconnected:
                        return {"type": "http.disconnect"}
                    message = await self._receive()
                    if message["type"] == "http.disconnect":
                        self._set_disconnected()
                    return message
            finally:
                self._body_readers_waiting -= 1
        return {"type": "http.disconnect"}

    async def stream(self) -> AsyncGenerator[bytes, None]:
        if hasattr(self, "_body"):
            yield self._body
            yield b""
            return
        if self._stream_consumed:
            raise RuntimeError("Stream consumed")
        while not self._stream_consumed:
            message = await self._next_message()
            if message["type"] == "http.request":
                body = message.get("body", b"")
                if not message.get("more_body", False):
                    self._stream_consumed = True
                if body:
                    yield body
            elif message["type"] == "http.disconnect":  # pragma: no branch
                raise ClientDisconnect()
        yield b""

    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            chunks: list[bytes] = []
            async for chunk in self.stream():
                chunks.append(chunk)
            self._body = b"".join(chunks)
        return self._body

    async def json(self) -> Any:
        if not hasattr(self, "_json"):  # pragma: no branch
            body = await self.body()
            self._json = json.loads(body)
        return self._json

    async def _get_form(
        self,
        *,
        max_files: int | float = 1000,
        max_fields: int | float = 1000,
        max_part_size: int = 1024 * 1024,
    ) -> FormData:
        if self._form is None:  # pragma: no branch
            assert parse_options_header is not None, (
                "The `python-multipart` library must be installed to use form parsing."
            )
            content_type_header = self.headers.get("Content-Type")
            content_type: bytes
            content_type, _ = parse_options_header(content_type_header)
            if content_type == b"multipart/form-data":
                try:
                    multipart_parser = MultiPartParser(
                        self.headers,
                        self.stream(),
                        max_files=max_files,
                        max_fields=max_fields,
                        max_part_size=max_part_size,
                    )
                    self._form = await multipart_parser.parse()
                except MultiPartException as exc:
                    if "app" in self.scope:
                        raise HTTPException(status_code=400, detail=exc.message)
                    raise exc
            elif content_type == b"application/x-www-form-urlencoded":
                try:
                    form_parser = FormParser(
                        self.headers,
                        self.stream(),
                        max_fields=max_fields,
                        max_part_size=max_part_size,
                    )
                    self._form = await form_parser.parse()
                except MultiPartException as exc:
                    if "app" in self.scope:
                        raise HTTPException(status_code=400, detail=exc.message)
                    raise exc
            else:
                self._form = FormData()
        return self._form

    def form(
        self,
        *,
        max_files: int | float = 1000,
        max_fields: int | float = 1000,
        max_part_size: int = 1024 * 1024,
    ) -> AwaitableOrContextManager[FormData]:
        return AwaitableOrContextManagerWrapper(
            self._get_form(max_files=max_files, max_fields=max_fields, max_part_size=max_part_size)
        )

    async def close(self) -> None:
        if self._form is not None:  # pragma: no branch
            await self._form.close()

    async def is_disconnected(self) -> bool:
        if not self._is_disconnected:
            if self._stashed_message is not None:
                # Polling `receive()` here could consume more of the request
                # body than the single stashed message we may hold on to, so
                # detection is left to the body-reading path for now.
                return self._is_disconnected
            try:
                # Don't wait for observers blocked in `receive()`: if another
                # task is receiving right now, it will observe a disconnect
                # that is already on its way and update the shared state.
                self._receive_lock.acquire_nowait()
            except anyio.WouldBlock:
                return self._is_disconnected
            try:
                if self._stashed_message is not None:
                    return self._is_disconnected
                message: Message = {}

                # If message isn't immediately available, move on
                with anyio.CancelScope() as cs:
                    cs.cancel()
                    message = await self._receive()

                if message.get("type") == "http.disconnect":
                    self._set_disconnected()
                elif message.get("type") == "http.request":
                    # Don't swallow body data: stash it so the body-reading
                    # path still receives it through `_next_message()`.
                    self._stashed_message = message
                    self._stash_empty = anyio.Event()
            finally:
                self._receive_lock.release()
        return self._is_disconnected

    async def wait_disconnected(self) -> None:
        """
        Wait until the client connection is known to be disconnected.

        Any number of callers may await this concurrently with each other and
        with the body-reading path: calls to `receive()` are serialized so
        observers never compete for messages, and every caller is notified
        through a shared event. Each caller can be cancelled independently
        without affecting the others, and once the disconnect state has been
        determined, callers that join later return immediately.

        Observers never swallow `http.request` messages. If an observer peeks
        a body message, it is stashed for the body-reading path and the
        observer waits for it to be collected before polling again, so at
        most one body message is ever buffered. While the request body has
        not been fully delivered, disconnect detection is therefore driven
        cooperatively with the body-reading path. Once the body is complete
        only `http.disconnect` can follow, so observers poll `receive()`
        directly — unless a body reader is still waiting to collect the
        stashed final message, in which case they wait for the handoff first.
        """
        while not self._is_disconnected:
            async with self._receive_lock:
                if self._is_disconnected:
                    return
                stashed = self._stashed_message
                if stashed is None or self._stream_consumed or (
                    not stashed.get("more_body", False) and not self._body_readers_waiting
                ):
                    message = await self._receive()
                    if message["type"] == "http.disconnect":
                        self._set_disconnected()
                        return
                    # A body message arrived while observing. Stash it for
                    # the body-reading path instead of swallowing it.
                    self._stashed_message = message
                    self._stash_empty = anyio.Event()
                    continue
            # A peeked body message is still waiting to be collected by the
            # body-reading path. Polling again could buffer a second body
            # message, so wait for the handoff instead. The body reader sets
            # `_stash_empty` when collecting the stash, which necessarily
            # happens before it can observe a disconnect itself, so no
            # disconnect notification can be missed while waiting here.
            await self._stash_empty.wait()

    async def send_push_promise(self, path: str) -> None:
        if "http.response.push" in self.scope.get("extensions", {}):
            raw_headers: list[tuple[bytes, bytes]] = []
            for name in SERVER_PUSH_HEADERS_TO_COPY:
                for value in self.headers.getlist(name):
                    raw_headers.append((name.encode("latin-1"), value.encode("latin-1")))
            await self._send({"type": "http.response.push", "path": path, "headers": raw_headers})
