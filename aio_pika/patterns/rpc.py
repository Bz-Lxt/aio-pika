import asyncio
import inspect
import json
import logging
import time
import uuid
from contextlib import suppress
from enum import Enum
from functools import partial
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Optional,
    Tuple,
)

from aiormq.abc import ExceptionType

from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    ConsumerTag,
    DeliveryMode,
)
from aio_pika.exceptions import MessageProcessError
from aio_pika.exchange import ExchangeType
from aio_pika.message import IncomingMessage, Message

from ..tools import ensure_awaitable
from .base import Base, CallbackType, Proxy, T


log = logging.getLogger(__name__)


class RPCException(RuntimeError):
    pass


class RPCMessageType(str, Enum):
    ERROR = "error"
    RESULT = "result"
    CALL = "call"
    # A streaming RPC request, client -> server
    STREAM = "stream"
    # Terminal message of a successfully finished stream, server -> client
    STREAM_END = "stream-end"
    # Cancellation of a streaming call, client -> cancel exchange
    CANCEL = "cancel"


# This needed only for migration from 6.x to 7.x
# TODO: Remove this in 8.x release
RPCMessageTypes = RPCMessageType  # noqa


class _StreamEnd:
    """Sentinel pushed into a stream queue on STREAM_END."""

    __slots__ = ()


_STREAM_END_SENTINEL = _StreamEnd()


class _StreamState:
    """Per-call state for an outstanding streaming RPC call on the client."""

    __slots__ = ("queue", "finished")

    def __init__(self) -> None:
        self.queue: "asyncio.Queue[Any]" = asyncio.Queue()
        self.finished = False


class RPC(Base):
    __slots__ = (
        "channel",
        "loop",
        "proxy",
        "futures",
        "result_queue",
        "result_consumer_tag",
        "routes",
        "queues",
        "consumer_tags",
        "dlx_exchange",
        "rpc_exchange",
        "host_exceptions",
        "stream_queues",
        "stream_tasks",
        "cancel_exchange",
        "cancel_queue",
        "cancel_consumer_tag",
        "cancelled_cids",
    )

    DLX_NAME = "rpc.dlx"
    CANCEL_EXCHANGE_NAME = "rpc.cancel"
    # How long the server remembers a cancel message which arrived before
    # the corresponding streaming call message was processed.
    CANCEL_TTL = 60.0
    DELIVERY_MODE = DeliveryMode.NOT_PERSISTENT

    __doc__ = """
    Remote Procedure Call helper.

    Create an instance ::

        rpc = await RPC.create(channel, host_exceptions=False)

    Registering python function ::

        # RPC instance passes only keyword arguments
        def multiply(*, x, y):
            return x * y

        await rpc.register("multiply", multiply)

    Call function through proxy ::

        assert await rpc.proxy.multiply(x=2, y=3) == 6

    Call function explicit ::

        assert await rpc.call('multiply', dict(x=2, y=3)) == 6

    Registering streaming python function ::

        # The function must be an async generator (or return an
        # async iterable / a plain value, which is emitted as a single item)
        async def range(*, to):
            for value in range(to):
                yield value

        await rpc.register("range", range)

    Iterate results as they arrive ::

        async for value in rpc.call_stream('range', dict(to=3)):
            ...  # 0, 1, 2

    Breaking the iteration cancels the remote stream, remote exceptions
    are raised by the iterator.

    Show exceptions on remote side ::

        rpc = await RPC.create(channel, host_exceptions=True)
    """

    result_queue: AbstractQueue
    result_consumer_tag: ConsumerTag
    cancel_queue: AbstractQueue
    cancel_consumer_tag: ConsumerTag
    dlx_exchange: AbstractExchange
    cancel_exchange: AbstractExchange
    rpc_exchange: Optional[AbstractExchange]

    def __init__(
        self,
        channel: AbstractChannel,
        host_exceptions: bool = False,
    ) -> None:
        self.channel = channel
        self.loop = asyncio.get_event_loop()
        self.proxy = Proxy(self.call)
        self.futures: Dict[str, asyncio.Future] = {}
        self.routes: Dict[str, Callable[..., Any]] = {}
        self.queues: Dict[Callable[..., Any], AbstractQueue] = {}
        self.consumer_tags: Dict[Callable[..., Any], ConsumerTag] = {}
        self.host_exceptions = host_exceptions
        # client side: correlation_id -> state of an outstanding stream call
        self.stream_queues: Dict[str, _StreamState] = {}
        # server side: correlation_id -> task producing a stream
        self.stream_tasks: Dict[str, "asyncio.Task[None]"] = {}
        # server side: cancels which arrived before the call message
        self.cancelled_cids: Dict[str, float] = {}

    def __remove_future(
        self,
        correlation_id: str,
    ) -> Callable[[asyncio.Future], None]:
        def do_remove(future: asyncio.Future) -> None:
            log.debug("Remove done future %r", future)
            self.futures.pop(correlation_id, None)

        return do_remove

    def create_future(self) -> Tuple[asyncio.Future, str]:
        future = self.loop.create_future()
        log.debug("Create future for RPC call")
        correlation_id = str(uuid.uuid4())
        self.futures[correlation_id] = future
        future.add_done_callback(self.__remove_future(correlation_id))
        return future, correlation_id

    def _format_routing_key(self, method_name: str) -> str:
        return (
            f"{self.rpc_exchange.name}::{method_name}"
            if self.rpc_exchange
            else method_name
        )

    async def close(self) -> None:
        if not hasattr(self, "result_queue"):
            log.warning("RPC already closed")
            return

        log.debug("Cancelling listening %r", self.result_queue)
        await self.result_queue.cancel(self.result_consumer_tag)
        del self.result_consumer_tag

        log.debug("Unbinding %r", self.result_queue)
        await self.result_queue.unbind(
            self.dlx_exchange,
            "",
            arguments={"From": self.result_queue.name, "x-match": "any"},
        )

        log.debug("Cancelling undone futures %r", self.futures)
        for future in self.futures.values():
            if future.done():
                continue

            future.set_exception(asyncio.CancelledError)

        # Unblock every stream iterated on the client side.
        self._finish_streams(asyncio.CancelledError())

        # Stop producing streams served by this instance.
        stream_tasks = [
            task
            for task in self.stream_tasks.values()
            if not task.done()
        ]
        for task in stream_tasks:
            task.cancel()
        if stream_tasks:
            await asyncio.gather(*stream_tasks, return_exceptions=True)

        log.debug("Cancelling stream-cancel listening %r", self.cancel_queue)
        await self.cancel_queue.cancel(self.cancel_consumer_tag)
        del self.cancel_consumer_tag

        log.debug("Unbinding %r", self.cancel_queue)
        await self.cancel_queue.unbind(self.cancel_exchange, "")

        log.debug("Deleting %r", self.cancel_queue)
        await self.cancel_queue.delete()
        del self.cancel_queue
        del self.cancel_exchange

        log.debug("Deleting %r", self.result_queue)
        await self.result_queue.delete()
        del self.result_queue
        del self.dlx_exchange

        if self.rpc_exchange:
            del self.rpc_exchange

    async def initialize(
        self,
        auto_delete: bool = True,
        durable: bool = False,
        exchange: str = "",
        **kwargs: Any,
    ) -> None:
        if hasattr(self, "result_queue"):
            return

        self.rpc_exchange = (
            await self.channel.declare_exchange(
                exchange,
                type=ExchangeType.DIRECT,
                auto_delete=True,
                durable=durable,
            )
            if exchange
            else None
        )

        self.result_queue = await self.channel.declare_queue(
            None,
            auto_delete=auto_delete,
            durable=durable,
            **kwargs,
        )

        self.dlx_exchange = await self.channel.declare_exchange(
            self.DLX_NAME,
            type=ExchangeType.HEADERS,
            auto_delete=True,
        )

        await self.result_queue.bind(
            self.dlx_exchange,
            "",
            arguments={"From": self.result_queue.name, "x-match": "any"},
        )

        self.result_consumer_tag = await self.result_queue.consume(
            self.on_result_message,
            exclusive=True,
            no_ack=True,
        )

        # Every RPC instance joins the fanout exchange used for cancelling
        # streaming calls, so a cancel reaches the instance serving the
        # call whichever one it is. Declaring the exchange on both sides
        # also makes a cancel publish safe when no server is up yet.
        self.cancel_exchange = await self.channel.declare_exchange(
            self.CANCEL_EXCHANGE_NAME,
            type=ExchangeType.FANOUT,
            auto_delete=True,
        )

        self.cancel_queue = await self.channel.declare_queue(
            None,
            auto_delete=auto_delete,
            durable=durable,
        )
        await self.cancel_queue.bind(self.cancel_exchange, "")

        self.cancel_consumer_tag = await self.cancel_queue.consume(
            self.on_cancel_message,
            exclusive=True,
            no_ack=True,
        )

        self.channel.close_callbacks.add(self.on_close)
        self.channel.return_callbacks.add(self.on_message_returned)

    def on_close(
        self,
        channel: Optional[AbstractChannel],
        exc: Optional[ExceptionType] = None,
    ) -> None:
        log.debug("Closing RPC futures because %r", exc)
        for future in self.futures.values():
            if future.done():
                continue

            future.set_exception(exc or Exception)

        close_exc: BaseException
        if isinstance(exc, BaseException):
            close_exc = exc
        elif isinstance(exc, type) and issubclass(exc, BaseException):
            close_exc = exc()
        else:
            close_exc = Exception()

        # Unblock streaming callers so the iterator does not hang forever.
        self._finish_streams(close_exc)

        # Stop server-side producers, even if they are sleeping in user code
        # rather than waiting for a broken channel write.
        for task in self.stream_tasks.values():
            if not task.done():
                task.cancel()

    def _finish_streams(self, exc: BaseException) -> None:
        for state in self.stream_queues.values():
            if state.finished:
                continue

            state.finished = True
            state.queue.put_nowait(exc)

    @classmethod
    async def create(cls, channel: AbstractChannel, **kwargs: Any) -> "RPC":
        """Creates a new instance of :class:`aio_pika.patterns.RPC`.
        You should use this method instead of :func:`__init__`,
        because :func:`create` returns coroutine and makes async initialize

        :param channel: initialized instance of :class:`aio_pika.Channel`
        :returns: :class:`RPC`

        """
        rpc = cls(channel)
        await rpc.initialize(**kwargs)
        return rpc

    def on_message_returned(
        self,
        channel: Optional[AbstractChannel],
        message: AbstractIncomingMessage,
    ) -> None:
        if message.correlation_id is None:
            log.warning(
                "Message without correlation_id was returned: %r",
                message,
            )
            return

        # Server side: a stream reply is unroutable, meaning the caller's
        # result queue is gone. Cancel the producer so it stops streaming.
        if message.type in (
            RPCMessageType.RESULT.value,
            RPCMessageType.ERROR.value,
            RPCMessageType.STREAM_END.value,
        ):
            task = self.stream_tasks.pop(message.correlation_id, None)
            if task is not None:
                if not task.done():
                    log.debug(
                        "Stream %r reply was returned, cancelling producer",
                        message.correlation_id,
                    )
                    task.cancel()
                return

            log.debug(
                "Returned stream reply for an unknown stream: %r",
                message,
            )
            return

        stream = self.stream_queues.pop(message.correlation_id, None)
        if stream is not None:
            if not stream.finished:
                stream.finished = True
                stream.queue.put_nowait(
                    MessageProcessError("Message has been returned", message),
                )
            return

        future = self.futures.pop(message.correlation_id, None)

        if not future or future.done():
            log.warning("Unknown message was returned: %r", message)
            return

        future.set_exception(
            MessageProcessError("Message has been returned", message),
        )

    async def on_result_message(self, message: AbstractIncomingMessage) -> None:
        if message.correlation_id is None:
            log.warning(
                "Message without correlation_id was received: %r",
                message,
            )
            return

        stream = self.stream_queues.get(message.correlation_id)
        if stream is not None:
            await self.on_stream_result_message(stream, message)
            return

        future = self.futures.pop(message.correlation_id, None)

        if future is None:
            if message.type in (
                RPCMessageType.RESULT.value,
                RPCMessageType.ERROR.value,
                RPCMessageType.STREAM_END.value,
            ):
                # A late reply arriving after the caller stopped iterating.
                log.debug("Late stream reply was ignored: %r", message)
            else:
                log.warning("Unknown message: %r", message)
            return

        try:
            payload = await self.deserialize_message(message)
        except Exception as e:
            log.error("Failed to deserialize response on message: %r", message)
            future.set_exception(e)
            return

        if message.type == RPCMessageType.RESULT.value:
            future.set_result(payload)
        elif message.type == RPCMessageType.ERROR.value:
            if not isinstance(payload, Exception):
                payload = RPCException("Wrapped non-exception object", payload)
            future.set_exception(payload)
        elif message.type == RPCMessageType.CALL.value:
            future.set_exception(
                asyncio.TimeoutError("Message timed-out", message),
            )
        else:
            future.set_exception(
                RuntimeError("Unknown message type %r" % message.type),
            )

    async def on_stream_result_message(
        self,
        stream: _StreamState,
        message: AbstractIncomingMessage,
    ) -> None:
        try:
            payload = await self.deserialize_message(message)
        except Exception as e:
            log.error("Failed to deserialize response on message: %r", message)
            stream.finished = True
            stream.queue.put_nowait(e)
            return

        message_type = message.type

        if message_type == RPCMessageType.RESULT.value:
            stream.queue.put_nowait(payload)
            return

        stream.finished = True

        if message_type == RPCMessageType.STREAM_END.value:
            stream.queue.put_nowait(_STREAM_END_SENTINEL)
        elif message_type == RPCMessageType.ERROR.value:
            if not isinstance(payload, Exception):
                payload = RPCException("Wrapped non-exception object", payload)
            stream.queue.put_nowait(payload)
        elif message_type in (
            RPCMessageType.CALL.value,
            RPCMessageType.STREAM.value,
        ):
            # An expired call message is dead-lettered back to the caller.
            stream.queue.put_nowait(
                asyncio.TimeoutError("Message timed-out", message),
            )
        else:
            stream.queue.put_nowait(
                RuntimeError("Unknown message type %r" % message_type),
            )

    async def on_call_message(
        self,
        method_name: str,
        message: IncomingMessage,
    ) -> None:

        routing_key = self._format_routing_key(method_name)

        if routing_key not in self.routes:
            log.warning("Method %r not registered in %r", method_name, self)
            return

        if message.type == RPCMessageType.STREAM.value:
            await self.on_stream_call_message(routing_key, message)
            return

        try:
            payload = await self.deserialize_message(message)
            func = self.routes[routing_key]
            result: Any = await self.execute(func, payload)
            message_type = RPCMessageType.RESULT
        except Exception as e:
            result = self.serialize_exception(e)
            message_type = RPCMessageType.ERROR

            if self.host_exceptions is True:
                log.exception(e)

        if not message.reply_to:
            log.info(
                'RPC message without "reply_to" header %r call result '
                "will be lost",
                message,
            )
            await message.ack()
            return

        try:
            result_message = await self.serialize_message(
                payload=result,
                message_type=message_type,
                correlation_id=message.correlation_id,
                delivery_mode=message.delivery_mode,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            result_message = await self.serialize_message(
                payload=e,
                message_type=RPCMessageType.ERROR,
                correlation_id=message.correlation_id,
                delivery_mode=message.delivery_mode,
            )

        try:
            await self.channel.default_exchange.publish(
                result_message,
                message.reply_to,
                mandatory=False,
            )
        except Exception as exc:
            log.error(
                "Failed to send reply %r due to %s: %s",
                result_message,
                type(exc).__name__,
                exc,
            )
            log.debug(
                "Full traceback for failure to send reply %r",
                exc_info=True,
            )
            await message.reject(requeue=False)
            return

        if message_type == RPCMessageType.ERROR.value:
            await message.ack()
            return

        await message.ack()

    async def on_cancel_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        correlation_id = message.correlation_id
        if correlation_id is None:
            return

        task = self.stream_tasks.pop(correlation_id, None)
        if task is not None:
            if not task.done():
                log.debug(
                    "Cancelling stream %r by caller request",
                    correlation_id,
                )
                task.cancel()
            return

        # The cancel can win the race against the call delivery, remember
        # it so the stream is dropped as soon as the call arrives.
        self._prune_cancelled_cids()
        self.cancelled_cids[correlation_id] = time.monotonic()

    def _prune_cancelled_cids(self) -> None:
        now = time.monotonic()
        for correlation_id, ts in tuple(self.cancelled_cids.items()):
            if now - ts > self.CANCEL_TTL:
                self.cancelled_cids.pop(correlation_id, None)

    def _take_cancelled_cid(self, correlation_id: str) -> bool:
        self._prune_cancelled_cids()
        return self.cancelled_cids.pop(correlation_id, None) is not None

    async def on_stream_call_message(
        self,
        routing_key: str,
        message: IncomingMessage,
    ) -> None:
        correlation_id = message.correlation_id

        if (
            correlation_id is not None
            and self._take_cancelled_cid(correlation_id)
        ):
            log.debug(
                "Stream call %r was cancelled before processing",
                correlation_id,
            )
            await self._settle_message(message)
            return

        try:
            payload = await self.deserialize_message(message)
        except Exception as exc:
            try:
                if message.reply_to:
                    await self._stream_publish(
                        message,
                        self.serialize_exception(exc),
                        RPCMessageType.ERROR,
                    )
            except Exception:
                log.debug(
                    "Failed to send stream error reply",
                    exc_info=True,
                )
                await self._settle_message(message, reject=True)
                return
            await self._settle_message(message)
            return

        func = self.routes[routing_key]
        task: "asyncio.Task[None]" = self.loop.create_task(
            self._serve_stream_call(func, payload, message),
        )

        if correlation_id is not None:
            self.stream_tasks[correlation_id] = task

        try:
            await task
        finally:
            if correlation_id is not None:
                self.stream_tasks.pop(correlation_id, None)

    async def _serve_stream_call(
        self,
        func: CallbackType,
        payload: Dict[str, Any],
        message: IncomingMessage,
    ) -> None:
        correlation_id = message.correlation_id

        if not message.reply_to:
            log.info(
                'RPC message without "reply_to" header %r stream result '
                "will be lost",
                message,
            )
            await self._settle_message(message)
            return

        generator: Optional[AsyncGenerator[Any, None]] = None

        try:
            result = func(**payload)
            if inspect.isawaitable(result) and not inspect.isasyncgen(result):
                result = await result

            if hasattr(result, "__aiter__"):
                generator = result
                async for item in generator:
                    await self._stream_publish(
                        message,
                        item,
                        RPCMessageType.RESULT,
                    )
            else:
                await self._stream_publish(
                    message,
                    result,
                    RPCMessageType.RESULT,
                )

            await self._stream_publish(
                message,
                None,
                RPCMessageType.STREAM_END,
            )
        except asyncio.CancelledError:
            # The caller stopped iterating (or this instance is closing):
            # stop producing and acknowledge the call message.
            log.debug("Stream %r cancelled by the caller", correlation_id)
            if generator is not None:
                with suppress(Exception, asyncio.CancelledError):
                    await generator.aclose()
            await self._settle_message(message)
            return
        except Exception as exc:
            if self.host_exceptions is True:
                log.error(
                    "Stream %r raised %s: %s",
                    correlation_id,
                    type(exc).__name__,
                    exc,
                )
                log.debug("Full traceback for stream %r", exc_info=True)
            try:
                await self._stream_publish(
                    message,
                    self.serialize_exception(exc),
                    RPCMessageType.ERROR,
                )
            except Exception:
                log.debug(
                    "Failed to send stream error reply",
                    exc_info=True,
                )
                await self._settle_message(message, reject=True)
                return

        await self._settle_message(message)

    async def _stream_publish(
        self,
        message: AbstractIncomingMessage,
        payload: Any,
        message_type: RPCMessageType,
    ) -> None:
        try:
            reply = await self.serialize_message(
                payload=payload,
                message_type=message_type,
                correlation_id=message.correlation_id,
                delivery_mode=message.delivery_mode,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reply = await self.serialize_message(
                payload=self.serialize_exception(exc),
                message_type=RPCMessageType.ERROR,
                correlation_id=message.correlation_id,
                delivery_mode=message.delivery_mode,
            )

        reply_to = message.reply_to
        if not reply_to:
            return

        # Shield the channel RPC: cancelling the producer task must not
        # cancel an in-flight basic.publish, otherwise the channel-wide
        # RPC lock gets stuck and the channel becomes unusable.
        await asyncio.shield(
            self.channel.default_exchange.publish(
                reply,
                reply_to,
                # Mandatory lets the broker return a reply when the
                # caller's result queue is gone, so the server can stop
                # producing instead of streaming into the void.
                mandatory=True,
            ),
        )

    @staticmethod
    async def _settle_message(
        message: IncomingMessage,
        *,
        reject: bool = False,
    ) -> None:
        try:
            coro = (
                message.reject(requeue=False)
                if reject
                else message.ack()
            )
            # Shield as well: the producer may be settling while the
            # serving task is being cancelled.
            await asyncio.shield(coro)
        except (Exception, asyncio.CancelledError):
            log.debug(
                "Failed to settle stream call message",
                exc_info=True,
            )

    def serialize_exception(self, exception: Exception) -> Any:
        """Make python exception serializable"""
        return exception

    async def execute(self, func: CallbackType, payload: Dict[str, Any]) -> T:
        """Executes rpc call. Might be overlapped."""
        return await func(**payload)

    async def deserialize_message(
        self,
        message: AbstractIncomingMessage,
    ) -> Any:
        return self.deserialize(message.body)

    async def serialize_message(
        self,
        payload: Any,
        message_type: RPCMessageType,
        correlation_id: Optional[str],
        delivery_mode: DeliveryMode,
        **kwargs: Any,
    ) -> Message:
        return Message(
            self.serialize(payload),
            content_type=self.CONTENT_TYPE,
            correlation_id=correlation_id,
            delivery_mode=delivery_mode,
            timestamp=time.time(),
            type=message_type.value,
            **kwargs,
        )

    async def call(
        self,
        method_name: str,
        kwargs: Optional[Dict[str, Any]] = None,
        *,
        expiration: Optional[int] = None,
        priority: int = 5,
        delivery_mode: DeliveryMode = DELIVERY_MODE,
    ) -> Any:
        """Call remote method and awaiting result.

        :param method_name: Name of method
        :param kwargs: Methos kwargs
        :param expiration:
            If not `None` messages which staying in queue longer
            will be returned and :class:`asyncio.TimeoutError` will be raised.
        :param priority: Message priority
        :param delivery_mode: Call message delivery mode
        :raises asyncio.TimeoutError: when message expired
        :raises CancelledError: when called :func:`RPC.cancel`
        :raises RuntimeError: internal error
        """

        future, correlation_id = self.create_future()

        message = await self.serialize_message(
            payload=kwargs or {},
            message_type=RPCMessageType.CALL,
            correlation_id=correlation_id,
            delivery_mode=delivery_mode,
            reply_to=self.result_queue.name,
            headers={"From": self.result_queue.name},
            priority=priority,
        )

        if expiration is not None:
            message.expiration = expiration

        routing_key = self._format_routing_key(method_name)

        log.debug("Publishing calls for %s(%r)", routing_key, kwargs)
        exchange = self.rpc_exchange or self.channel.default_exchange
        await exchange.publish(
            message,
            routing_key=routing_key,
            mandatory=True,
        )

        log.debug("Waiting RPC result for %s(%r)", routing_key, kwargs)
        return await future

    async def call_stream(
        self,
        method_name: str,
        kwargs: Optional[Dict[str, Any]] = None,
        *,
        expiration: Optional[int] = None,
        priority: int = 5,
        delivery_mode: DeliveryMode = DELIVERY_MODE,
    ) -> AsyncGenerator[Any, None]:
        """Call a remote method and iterate over zero or more results,
        which are yielded as they arrive.

        Registered async generators (or any async iterable) are streamed
        item by item; plain return values are delivered as a single item.
        Concurrent calls never mix, because every result carries its own
        correlation id.

        Breaking or closing the iterator sends a cancel message which
        stops the remote producer. Remote exceptions are raised here,
        closing the channel/connection raises as well instead of hanging.

        :param method_name: Name of method
        :param kwargs: Method kwargs
        :param expiration:
            If not `None` messages which staying in queue longer
            will be returned and :class:`asyncio.TimeoutError` will be raised.
        :param priority: Message priority
        :param delivery_mode: Call message delivery mode
        :raises asyncio.TimeoutError: when message expired
        :raises asyncio.CancelledError: when called :func:`RPC.close`
        :raises MessageProcessError: when the call message was returned
        :raises Exception: exception raised on the remote side
        """

        correlation_id = str(uuid.uuid4())
        stream = _StreamState()
        self.stream_queues[correlation_id] = stream

        message = await self.serialize_message(
            payload=kwargs or {},
            message_type=RPCMessageType.STREAM,
            correlation_id=correlation_id,
            delivery_mode=delivery_mode,
            reply_to=self.result_queue.name,
            headers={"From": self.result_queue.name},
            priority=priority,
        )

        if expiration is not None:
            message.expiration = expiration

        routing_key = self._format_routing_key(method_name)

        log.debug("Publishing stream call for %s(%r)", routing_key, kwargs)
        exchange = self.rpc_exchange or self.channel.default_exchange
        try:
            await exchange.publish(
                message,
                routing_key=routing_key,
                mandatory=True,
            )
        except Exception:
            self.stream_queues.pop(correlation_id, None)
            raise

        try:
            while True:
                item = await stream.queue.get()

                if item is _STREAM_END_SENTINEL:
                    return

                if isinstance(item, BaseException):
                    raise item

                yield item
        finally:
            self.stream_queues.pop(correlation_id, None)

            if not stream.finished:
                # The caller stopped iterating before the stream ended:
                # ask the server to stop producing.
                await self._send_stream_cancel(correlation_id)

    async def _send_stream_cancel(self, correlation_id: str) -> None:
        try:
            message = await self.serialize_message(
                payload=None,
                message_type=RPCMessageType.CANCEL,
                correlation_id=correlation_id,
                delivery_mode=self.DELIVERY_MODE,
            )
            await self.cancel_exchange.publish(
                message,
                routing_key="",
                mandatory=False,
            )
        except Exception:
            log.debug(
                "Failed to send stream cancel %r",
                correlation_id,
                exc_info=True,
            )

    async def register(
        self,
        method_name: str,
        func: CallbackType,
        **kwargs: Any,
    ) -> Any:
        """Method creates a queue with name which equal of
        `method_name` argument. Then subscribes this queue.

        :param method_name: Method name
        :param func:
            target function. Function **MUST** accept only keyword arguments.
        :param kwargs: arguments which will be passed to `queue_declare`
        :raises RuntimeError:
            Function already registered in this :class:`RPC` instance
            or method_name already used.
        """
        arguments = kwargs.pop("arguments", {})
        arguments.update({"x-dead-letter-exchange": self.DLX_NAME})

        # Async generator functions are streamed by :meth:`call_stream`
        # and must stay callable as-is; ``ensure_awaitable`` would wrap them
        # into a coroutine wrapper which is meaningless for them.
        if not inspect.isasyncgenfunction(func):
            func = ensure_awaitable(func)

        kwargs["arguments"] = arguments

        routing_key = self._format_routing_key(method_name)

        queue = await self.channel.declare_queue(routing_key, **kwargs)

        if self.rpc_exchange:
            await queue.bind(
                self.rpc_exchange,
                routing_key,
            )

        if func in self.consumer_tags:
            raise RuntimeError("Function already registered")

        if routing_key in self.routes:
            raise RuntimeError(
                "Method name already used for %r" % self.routes[routing_key],
            )

        self.consumer_tags[func] = await queue.consume(
            partial(self.on_call_message, method_name),
        )

        self.routes[routing_key] = func
        self.queues[func] = queue

    async def unregister(self, func: CallbackType) -> None:
        """Cancels subscription to the method-queue.

        :param func: Function
        """
        if func not in self.consumer_tags:
            return

        consumer_tag = self.consumer_tags.pop(func)
        queue = self.queues.pop(func)

        await queue.cancel(consumer_tag)

        self.routes.pop(queue.name)


class JsonRPCError(RuntimeError):
    pass


class JsonRPC(RPC):
    SERIALIZER = json
    CONTENT_TYPE = "application/json"

    def serialize(self, data: Any) -> bytes:
        return self.SERIALIZER.dumps(
            data,
            ensure_ascii=False,
            default=repr,
        ).encode()

    def serialize_exception(self, exception: Exception) -> Any:
        return {
            "error": {
                "type": exception.__class__.__name__,
                "message": repr(exception),
                "args": exception.args,
            },
        }

    async def deserialize_message(
        self,
        message: AbstractIncomingMessage,
    ) -> Any:
        payload = await super().deserialize_message(message)
        if message.type == RPCMessageType.ERROR:
            payload = JsonRPCError("RPC exception", payload)
        return payload


__all__ = (
    "JsonRPC",
    "RPC",
    "RPCException",
    "RPCMessageType",
)
