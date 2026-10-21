import asyncio
import logging
import warnings
from functools import partial

import pytest

import aio_pika
from aio_pika import Message
from aio_pika.exceptions import MessageProcessError
from aio_pika.message import IncomingMessage
from aio_pika.patterns.rpc import JsonRPC, JsonRPCError, RPC
from aio_pika.patterns.rpc import log as rpc_logger
from tests import get_random_name


async def rpc_func(*, foo, bar):
    assert not foo
    assert not bar

    return {"foo": "bar"}


async def rpc_func2(*, foo, bar):
    assert not foo
    assert not bar

    return {"foo": "bar2"}


async def rpc_stream_range(*, count, delay=0):
    for i in range(count):
        yield i
        if delay:
            await asyncio.sleep(delay)


async def rpc_stream_empty():
    if False:  # pragma: no cover
        yield 0


async def rpc_stream_error(*, after):
    for i in range(after):
        yield i
    raise RuntimeError("stream boom")


class TestCase:
    async def test_simple(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        result = await rpc.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar"}

        await rpc.unregister(rpc_func)
        await rpc.close()

        # Close already closed
        await rpc.close()

    async def test_error(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        with pytest.raises(AssertionError):
            await rpc.proxy.test.rpc(foo=True, bar=None)

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_unroutable(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        with pytest.raises(MessageProcessError):
            await rpc.proxy.unroutable()

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_timed_out(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        await channel.declare_queue(
            "test.timed_out",
            auto_delete=True,
            arguments={"x-dead-letter-exchange": RPC.DLX_NAME},
        )

        with pytest.raises(asyncio.TimeoutError):
            await rpc.call("test.timed_out", expiration=1)

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_close_twice(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.close()
        await rpc.close()

    async def test_init_twice(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.initialize()

        await rpc.close()

    async def test_send_unknown_message(
        self,
        channel: aio_pika.Channel,
        caplog,
    ):
        rpc = await RPC.create(channel, auto_delete=True)

        body = b"test body"
        with caplog.at_level(logging.WARNING, logger=rpc_logger.name):
            await channel.default_exchange.publish(
                Message(body),
                routing_key=rpc.result_queue.name,
            )

            await asyncio.sleep(0.5)

            for log_record in caplog.records:
                if log_record.levelno == logging.WARNING:
                    break
            else:
                raise pytest.fail("Expected log message")

            incoming = log_record.args[0]

            assert isinstance(incoming, IncomingMessage)
            assert incoming.body == body
            assert (
                "Message without correlation_id was received:"
                in log_record.message
            )

        with caplog.at_level(logging.WARNING, logger=rpc_logger.name):
            await channel.default_exchange.publish(
                Message(body),
                routing_key="should-returned",
            )

            await asyncio.sleep(0.5)

            for log_record in caplog.records:
                if log_record.levelno == logging.WARNING:
                    break
            else:
                raise pytest.fail("Expected log message")

            incoming = log_record.args[0]
            assert isinstance(incoming, IncomingMessage)
            assert incoming.body == body
            assert (
                "Message without correlation_id was received:"
                in log_record.message
            )

        await rpc.close()

    async def test_close_cancelling(
        self,
        channel: aio_pika.Channel,
        event_loop,
    ):
        rpc = await RPC.create(channel, auto_delete=True)

        async def sleeper():
            await asyncio.sleep(60)

        method_name = get_random_name("test", "sleeper")

        await rpc.register(method_name, sleeper, auto_delete=True)

        tasks = set()

        for _ in range(10):
            tasks.add(event_loop.create_task(rpc.call(method_name)))

        await rpc.close()

        logging.info("Waiting for results")
        for task in tasks:
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_register_twice(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        async def bypass(_: aio_pika.abc.AbstractIncomingMessage):
            return

        await rpc.register("test.sleeper", bypass, auto_delete=True)

        with pytest.raises(RuntimeError):
            await rpc.register(
                "test.sleeper",
                bypass,
                auto_delete=True,
            )

        await rpc.register("test.one", rpc_func, auto_delete=True)

        with pytest.raises(RuntimeError):
            await rpc.register("test.two", rpc_func, auto_delete=True)

        await rpc.unregister(rpc_func)
        await rpc.unregister(rpc_func)

        await rpc.close()

    async def test_register_non_coroutine(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        def bypass(_):
            return

        with pytest.deprecated_call():
            await rpc.register(
                "test.non-coroutine",
                bypass,  # type: ignore
                auto_delete=True,
            )

        async def coro(_):
            return

        with pytest.warns(UserWarning) as record:
            warnings.warn("Test", UserWarning)
            await rpc.register(
                "test.coroutine",
                coro,  # type: ignore
                auto_delete=True,
            )

        assert len([w for w in record if w.category is UserWarning]) == 1

        with pytest.warns() as record:
            warnings.warn("Test", UserWarning)
            await rpc.register(
                "test.coroutine_partial",
                partial(partial(coro)),  # type: ignore
                auto_delete=True,
            )

        assert len([w for w in record if w.category is UserWarning]) == 1

    async def test_non_serializable_result(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        async def bad_func():
            async def inner():
                await asyncio.sleep(0)

            return inner()

        await rpc.register(
            "test.not-serializable",
            bad_func,
            auto_delete=True,
        )

        with pytest.raises(TypeError):
            await rpc.call("test.not-serializable")

    async def test_custom_exchange(self, channel: aio_pika.Channel):
        rpc_ex1 = await RPC.create(channel, auto_delete=True, exchange="ex1")
        rpc_ex2 = await RPC.create(channel, auto_delete=True, exchange="ex2")
        rpc_default = await RPC.create(channel, auto_delete=True)

        await rpc_ex1.register("test.rpc", rpc_func, auto_delete=True)
        result = await rpc_ex1.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar"}

        with pytest.raises(MessageProcessError):
            await rpc_ex2.proxy.test.rpc(foo=None, bar=None)

        await rpc_ex2.register("test.rpc", rpc_func2, auto_delete=True)
        result = await rpc_ex2.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar2"}

        with pytest.raises(MessageProcessError):
            await rpc_default.proxy.test.rpc(foo=None, bar=None)

        await rpc_default.register("test.rpc", rpc_func, auto_delete=True)
        result = await rpc_default.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar"}

        await rpc_ex1.unregister(rpc_func)
        await rpc_ex1.close()

        await rpc_ex2.unregister(rpc_func2)
        await rpc_ex2.close()

        await rpc_default.unregister(rpc_func)
        await rpc_default.close()

    async def test_stream_simple(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        method_name = get_random_name("test", "stream")
        await rpc.register(
            method_name, rpc_stream_range, auto_delete=True,
        )

        result = [
            item
            async for item in rpc.call_stream(method_name, {"count": 5})
        ]
        assert result == [0, 1, 2, 3, 4]

        await rpc.unregister(rpc_stream_range)
        await rpc.close()

    async def test_stream_empty(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        method_name = get_random_name("test", "stream-empty")
        await rpc.register(
            method_name, rpc_stream_empty, auto_delete=True,
        )

        result = [item async for item in rpc.call_stream(method_name)]
        assert result == []

        await rpc.unregister(rpc_stream_empty)
        await rpc.close()

    async def test_stream_plain_value(self, channel: aio_pika.Channel):
        # A regular coroutine function called via call_stream must deliver
        # exactly one item and then end.
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        result = [
            item
            async for item in rpc.call_stream(
                "test.rpc", {"foo": None, "bar": None},
            )
        ]
        assert result == [{"foo": "bar"}]

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_stream_error(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        method_name = get_random_name("test", "stream-error")
        await rpc.register(
            method_name, rpc_stream_error, auto_delete=True,
        )

        stream = rpc.call_stream(method_name, {"after": 2})

        assert await stream.__anext__() == 0
        assert await stream.__anext__() == 1

        with pytest.raises(RuntimeError, match="stream boom"):
            await stream.__anext__()

        await stream.aclose()

        await rpc.unregister(rpc_stream_error)
        await rpc.close()

    async def test_stream_concurrent(self, channel: aio_pika.Channel):
        # Concurrent streams of the same method must not mix.
        rpc = await RPC.create(channel, auto_delete=True)

        method_name = get_random_name("test", "stream-concurrent")
        await rpc.register(
            method_name, rpc_stream_range, auto_delete=True,
        )

        async def collect():
            return [
                item
                async for item in rpc.call_stream(
                    method_name, {"count": 4, "delay": 0.02},
                )
            ]

        results = await asyncio.gather(*(collect() for _ in range(6)))
        assert results == [[0, 1, 2, 3] for _ in range(6)]

        await rpc.unregister(rpc_stream_range)
        await rpc.close()

    async def test_stream_client_cancel(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        produced = []
        stopped = asyncio.Event()

        async def rpc_stream_long(*, delay):
            try:
                for i in range(1000):
                    produced.append(i)
                    yield i
                    await asyncio.sleep(delay)
            finally:
                stopped.set()

        method_name = get_random_name("test", "stream-cancel")
        await rpc.register(method_name, rpc_stream_long, auto_delete=True)

        received = []
        async for item in rpc.call_stream(method_name, {"delay": 0.05}):
            received.append(item)
            if len(received) == 2:
                break

        # The cancel message must reach the server and stop the producer.
        await asyncio.wait_for(stopped.wait(), timeout=5)

        # The producer must not run to the end.
        assert len(produced) < 1000

        await rpc.unregister(rpc_stream_long)
        await rpc.close()

    async def test_stream_cancel_before_first_item(
        self,
        channel: aio_pika.Channel,
        event_loop,
    ):
        rpc = await RPC.create(channel, auto_delete=True)

        method_name = get_random_name("test", "stream-cancel-early")

        async def rpc_stream_late(*, delay):
            await asyncio.sleep(delay)
            yield 1

        await rpc.register(method_name, rpc_stream_late, auto_delete=True)

        stream = rpc.call_stream(method_name, {"delay": 30})
        # Cancel before any item could be produced or even delivered.
        await stream.aclose()

        # The serving task must stop (or never start) instead of hanging.
        deadline = event_loop.time() + 5
        while rpc.stream_tasks and event_loop.time() < deadline:
            await asyncio.sleep(0.05)

        assert not rpc.stream_tasks

        await rpc.unregister(rpc_stream_late)
        await rpc.close()

    async def test_stream_unroutable(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        with pytest.raises(MessageProcessError):
            async for _ in rpc.call_stream(
                get_random_name("test", "stream-unroutable"),
            ):
                pass

        await rpc.close()

    async def test_stream_timed_out(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        method_name = get_random_name("test", "stream-timed-out")

        await channel.declare_queue(
            method_name,
            auto_delete=True,
            arguments={"x-dead-letter-exchange": RPC.DLX_NAME},
        )

        with pytest.raises(asyncio.TimeoutError):
            async for _ in rpc.call_stream(method_name, expiration=1):
                pass

        await rpc.close()

    async def test_stream_close_during_iteration(
        self,
        channel: aio_pika.Channel,
    ):
        rpc = await RPC.create(channel, auto_delete=True)

        async def rpc_stream_forever():
            while True:
                yield 1
                await asyncio.sleep(0.02)

        method_name = get_random_name("test", "stream-close")
        await rpc.register(
            method_name, rpc_stream_forever, auto_delete=True,
        )

        stream = rpc.call_stream(method_name)

        assert await stream.__anext__() == 1

        close_task = asyncio.ensure_future(rpc.close())

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stream.__anext__(), timeout=5)

        await stream.aclose()
        await close_task

    async def test_json_stream_error(self, channel: aio_pika.Channel):
        rpc = await JsonRPC.create(channel, auto_delete=True)

        method_name = get_random_name("test", "json-stream-error")
        await rpc.register(
            method_name, rpc_stream_error, auto_delete=True,
        )

        stream = rpc.call_stream(method_name, {"after": 1})

        assert await stream.__anext__() == 0

        with pytest.raises(JsonRPCError):
            await stream.__anext__()

        await stream.aclose()

        await rpc.unregister(rpc_stream_error)
        await rpc.close()

    async def test_stream_separate_instances_cancel(
        self,
        create_connection,
        create_channel,
    ):
        # Caller and callee are distinct RPC instances on distinct
        # connections, so the cancel has to travel through the shared
        # fanout exchange rather than reaching in-process state.
        server_connection = await create_connection()
        try:
            server_channel = await create_channel(
                connection=server_connection, cleanup=False,
            )
            server = await RPC.create(server_channel, auto_delete=True)

            stopped = asyncio.Event()
            produced = []

            async def stream_long(*, delay):
                try:
                    for i in range(1000):
                        produced.append(i)
                        yield i
                        await asyncio.sleep(delay)
                finally:
                    stopped.set()

            method_name = get_random_name("test", "stream-separate")
            await server.register(method_name, stream_long, auto_delete=True)

            client_connection = await create_connection()
            try:
                client_channel = await create_channel(
                    connection=client_connection, cleanup=False,
                )
                client = await RPC.create(client_channel, auto_delete=True)

                received = []
                async for item in client.call_stream(
                    method_name, {"delay": 0.05},
                ):
                    received.append(item)
                    if len(received) == 2:
                        break

                await asyncio.wait_for(stopped.wait(), timeout=5)
                assert len(produced) < 1000

                await client.close()
            finally:
                await client_connection.close()

            await server.unregister(stream_long)
            await server.close()
        finally:
            await server_connection.close()

    async def test_stream_server_stops_when_client_gone(
        self,
        create_connection,
        create_channel,
    ):
        # When the client connection drops without a graceful cancel,
        # the mandatory stream replies are returned and the server must
        # stop producing instead of streaming forever.
        server_connection = await create_connection()
        try:
            server_channel = await create_channel(
                connection=server_connection, cleanup=False,
            )
            server = await RPC.create(server_channel, auto_delete=True)

            stopped = asyncio.Event()

            async def stream_forever():
                try:
                    while True:
                        yield 0
                        await asyncio.sleep(0.05)
                finally:
                    stopped.set()

            method_name = get_random_name("test", "stream-client-gone")
            await server.register(
                method_name, stream_forever, auto_delete=True,
            )

            client_connection = await create_connection()
            try:
                client_channel = await create_channel(
                    connection=client_connection, cleanup=False,
                )
                client = await RPC.create(client_channel, auto_delete=True)

                stream = client.call_stream(method_name)
                assert await stream.__anext__() == 0

                # Drop the client's channel without a graceful cancel:
                # its consumers go away and the result queue is deleted.
                await client_channel.close()
                await stream.aclose()
            finally:
                await client_connection.close()

            await asyncio.wait_for(stopped.wait(), timeout=10)
            assert not server.stream_tasks

            await server.unregister(stream_forever)
            await server.close()
        finally:
            await server_connection.close()
