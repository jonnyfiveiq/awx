"""SQL Server notification broker for dispatcherd.

Replaces PostgreSQL pg_notify with a hybrid approach:
- Polling table (awx_notify_messages) for reliable multi-consumer message delivery
- Service Broker WAITFOR RECEIVE as a zero-latency wake-up signal

This module implements the Broker Protocol from dispatcherd.protocols so it
can be used as a drop-in replacement for dispatcherd.brokers.pg_notify.
"""

import asyncio
import json
import logging
import threading
import time
import uuid
from typing import Any, AsyncGenerator, Callable, Coroutine, Iterator, Optional

import pyodbc

from dispatcherd.chunking import split_message
from dispatcherd.protocols import BrokerSelfCheckStatus

logger = logging.getLogger('awx.main.dispatch.service_broker')

CLEANUP_INTERVAL_SECONDS = 300
MESSAGE_RETENTION_SECONDS = 300


def _build_connection_string(config: dict) -> str:
    server = config.get('server', 'localhost,1433')
    database = config.get('database', 'awx')
    user = config.get('user', 'SA')
    password = config.get('password', '')
    driver = config.get('driver', 'ODBC Driver 18 for SQL Server')
    trust_cert = config.get('TrustServerCertificate', 'yes')
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate={trust_cert};"
    )


def _create_connection(conn_str: str) -> pyodbc.Connection:
    conn = pyodbc.connect(conn_str, autocommit=True)
    return conn


class Broker:
    def __init__(
        self,
        config: dict | None = None,
        channels: tuple | list = (),
        default_publish_channel: str | None = None,
        max_connection_idle_seconds: int | None = 30,
        max_self_check_message_age_seconds: int | None = 2,
        poll_interval_seconds: float = 0.25,
        **kwargs,
    ) -> None:
        if not config:
            raise RuntimeError('Must specify config with SQL Server connection parameters')

        self._conn_str = _build_connection_string(config)
        self._config = config.copy()

        self.broker_id = f"broker_{str(uuid.uuid4()).replace('-', '_')}"

        self.max_connection_idle_seconds = max_connection_idle_seconds
        self.max_self_check_message_age_seconds = max_self_check_message_age_seconds
        self.poll_interval_seconds = poll_interval_seconds

        self._sync_connection: pyodbc.Connection | None = None
        self._sync_lock = threading.Lock()
        self._async_connection: pyodbc.Connection | None = None

        self.user_channels = list(channels)
        server_channels = list(channels)
        self.self_check_channel: str | None
        if self.max_connection_idle_seconds is not None:
            self.self_check_channel = f"self_check_{str(uuid.uuid4()).replace('-', '_')}"
            if self.self_check_channel not in server_channels:
                server_channels.append(self.self_check_channel)
        else:
            self.self_check_channel = None
        self.channels = server_channels

        self.default_publish_channel = default_publish_channel
        self.self_check_status = BrokerSelfCheckStatus.IDLE
        self.last_self_check_message_time = time.monotonic() if self.max_connection_idle_seconds is not None else None

        self.notify_loop_active: bool = False
        self.notify_queue: list = []
        self.max_message_bytes: int | None = None

        self.self_check_success_count = 0
        self.self_check_success_total_duration = 0.0
        self.self_check_success_max_duration = 0.0

        self._last_seen_id: int = 0
        self._last_cleanup = time.monotonic()
        self._initialized = False

    def _initialize_last_seen_id(self, conn: pyodbc.Connection) -> None:
        if self._initialized:
            return
        cursor = conn.cursor()
        cursor.execute("SELECT ISNULL(MAX(id), 0) FROM awx_notify_messages")
        row = cursor.fetchone()
        self._last_seen_id = row[0] if row else 0
        cursor.close()
        self._initialized = True
        logger.info('Service broker initialized, last_seen_id=%d', self._last_seen_id)

    def get_publish_channel(self, channel: str | None = None) -> str:
        if channel is not None:
            return channel
        if self.default_publish_channel is not None:
            return self.default_publish_channel
        if len(self.user_channels) == 1:
            return self.user_channels[0]
        raise ValueError('Could not determine a channel to publish to')

    def __str__(self) -> str:
        return 'service_broker'

    # --- sync connection ---

    def get_connection(self) -> pyodbc.Connection:
        with self._sync_lock:
            if self._sync_connection is None:
                start = time.perf_counter()
                self._sync_connection = _create_connection(self._conn_str)
                logger.info('Service broker sync connection established in %.3f seconds', time.perf_counter() - start)
            return self._sync_connection

    def _get_async_connection(self) -> pyodbc.Connection:
        if self._async_connection is None:
            start = time.perf_counter()
            self._async_connection = _create_connection(self._conn_str)
            logger.info('Service broker async connection established in %.3f seconds', time.perf_counter() - start)
        return self._async_connection

    # --- publish ---

    def _do_publish(self, conn: pyodbc.Connection, channel: str, message: str) -> None:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO awx_notify_messages (channel, payload) VALUES (?, ?)",
                channel, message
            )
            try:
                cursor.execute("""
                    DECLARE @dialog UNIQUEIDENTIFIER;
                    BEGIN DIALOG CONVERSATION @dialog
                        FROM SERVICE [AwxSignalService]
                        TO SERVICE 'AwxSignalService'
                        ON CONTRACT [AwxSignalContract]
                        WITH ENCRYPTION = OFF;
                    SEND ON CONVERSATION @dialog
                        MESSAGE TYPE [AwxSignalMessage] (N'wake');
                    END CONVERSATION @dialog;
                """)
            except Exception as e:
                logger.debug('Service Broker signal send failed (non-fatal): %s', e)
        finally:
            cursor.close()

    def publish_message(self, channel: str | None = None, message: str = '') -> str:
        connection = self.get_connection()
        channel = self.get_publish_channel(channel)

        chunks = split_message(message, max_bytes=self.max_message_bytes)
        for chunk in chunks:
            self._do_publish(connection, channel, chunk)

        logger.debug('Sent message of %d chars as %d chunk(s) to %s', len(message), len(chunks), channel)
        return channel

    async def apublish_message(self, channel: str | None = None, origin: str | int | None = '', message: str = '') -> None:
        channel = self.get_publish_channel(channel)
        chunks = split_message(message, max_bytes=self.max_message_bytes)

        if self.notify_loop_active:
            for chunk in chunks:
                self.notify_queue.append((channel, chunk))
            return

        loop = asyncio.get_event_loop()
        conn = self._get_async_connection()
        for chunk in chunks:
            await loop.run_in_executor(None, self._do_publish, conn, channel, chunk)

        logger.debug('Sent async message of %d chars as %d chunk(s) to %s', len(message), len(chunks), channel)

    # --- receive ---

    def _poll_messages(self, conn: pyodbc.Connection) -> list[tuple[str, str]]:
        self._initialize_last_seen_id(conn)
        cursor = conn.cursor()
        try:
            placeholders = ','.join('?' for _ in self.channels)
            cursor.execute(
                f"SELECT id, channel, payload FROM awx_notify_messages "
                f"WHERE id > ? AND channel IN ({placeholders}) ORDER BY id",
                self._last_seen_id, *self.channels
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()

        messages = []
        for row in rows:
            msg_id, channel, payload = row
            self._last_seen_id = msg_id
            messages.append((channel, payload))
        return messages

    def _wait_for_signal(self, conn: pyodbc.Connection, timeout_ms: int) -> bool:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "WAITFOR (RECEIVE TOP(1) conversation_handle, message_type_name "
                "FROM [AwxSignalQueue]), TIMEOUT ?",
                timeout_ms
            )
            row = cursor.fetchone()
            if row:
                conv_handle = row[0]
                try:
                    cursor.execute("END CONVERSATION ?", conv_handle)
                except Exception:
                    pass
                return True
            return False
        except Exception as e:
            logger.debug('WAITFOR RECEIVE failed: %s', e)
            return False
        finally:
            cursor.close()

    def _maybe_cleanup(self, conn: pyodbc.Connection) -> None:
        now = time.monotonic()
        if now - self._last_cleanup < CLEANUP_INTERVAL_SECONDS:
            return
        self._last_cleanup = now
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM awx_notify_messages WHERE created_at < DATEADD(SECOND, ?, SYSUTCDATETIME())",
                -MESSAGE_RETENTION_SECONDS
            )
            deleted = cursor.rowcount
            if deleted > 0:
                logger.debug('Cleaned up %d old notification messages', deleted)
        except Exception as e:
            logger.debug('Message cleanup failed (non-fatal): %s', e)
        finally:
            cursor.close()

    def process_notify(
        self, connected_callback: Callable | None = None, timeout: float = 5.0, max_messages: int | None = 1
    ) -> Iterator[tuple[str, str]]:
        connection = self.get_connection()
        self._initialize_last_seen_id(connection)

        if connected_callback:
            connected_callback()

        logger.info('Service broker sync listener started on channels: %s', self.channels)

        start_time = time.monotonic()
        message_count = 0

        while True:
            elapsed = time.monotonic() - start_time
            if elapsed >= timeout:
                break

            remaining_ms = int((timeout - elapsed) * 1000)
            if remaining_ms <= 0:
                break

            wait_ms = min(remaining_ms, 5000)
            self._wait_for_signal(connection, wait_ms)

            messages = self._poll_messages(connection)
            for channel, payload in messages:
                yield (channel, payload)
                message_count += 1
                if max_messages is not None and message_count >= max_messages:
                    return

            self._maybe_cleanup(connection)

    async def aprocess_notify(
        self, connected_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None
    ) -> AsyncGenerator[tuple[str, str], None]:
        loop = asyncio.get_event_loop()
        conn = self._get_async_connection()
        self._initialize_last_seen_id(conn)

        if connected_callback:
            await connected_callback()

        logger.info('Service broker async listener started on channels: %s', self.channels)

        while True:
            self.notify_loop_active = True
            wait_timeout = self.max_connection_idle_seconds or 30
            wait_ms = wait_timeout * 1000

            got_signal = await loop.run_in_executor(None, self._wait_for_signal, conn, wait_ms)

            messages = await loop.run_in_executor(None, self._poll_messages, conn)

            for channel, payload in messages:
                yield (channel, payload)

            if not messages and not got_signal:
                if self.max_connection_idle_seconds is not None:
                    logger.debug(
                        'No message received for %d seconds, starting self check',
                        self.max_connection_idle_seconds
                    )
                    await self.initiate_self_check()

            self.notify_loop_active = False
            for reply_channel, reply_message in self.notify_queue:
                await loop.run_in_executor(None, self._do_publish, conn, reply_channel, reply_message)
            self.notify_queue = []

            await loop.run_in_executor(None, self._maybe_cleanup, conn)

    # --- self check ---

    async def initiate_self_check(self) -> None:
        if self.max_connection_idle_seconds is None:
            return
        if self.self_check_status == BrokerSelfCheckStatus.IN_PROGRESS:
            assert self.last_self_check_message_time is not None
            delta = time.monotonic() - self.last_self_check_message_time
            raise RuntimeError(f'self check message for broker {self.broker_id} did not arrive in {delta} seconds')

        assert self.self_check_channel is not None
        await self.apublish_message(
            channel=self.self_check_channel,
            message=json.dumps({'self_check': True, 'task': f'lambda: "{self.broker_id}"'})
        )
        self.self_check_status = BrokerSelfCheckStatus.IN_PROGRESS
        self.last_self_check_message_time = time.monotonic()

    def verify_self_check(self, message: dict[str, Any]) -> None:
        if self.max_connection_idle_seconds is None:
            return
        if self.broker_id not in message.get('task', ''):
            logger.debug('Ignoring self-check from different broker: %s', message.get('task'))
            return

        now = time.monotonic()
        assert self.last_self_check_message_time is not None
        delta_seconds = now - self.last_self_check_message_time
        self.self_check_status = BrokerSelfCheckStatus.IDLE

        assert self.max_self_check_message_age_seconds is not None
        if delta_seconds < self.max_self_check_message_age_seconds:
            self._record_self_check_success(delta_seconds)
            logger.debug('Self check succeeded, %.2fs, broker-id %s', delta_seconds, self.broker_id)
        else:
            raise RuntimeError(f'Self check failed, received after {delta_seconds:.2f}s, broker-id {self.broker_id}')

    def _record_self_check_success(self, delta_seconds: float) -> None:
        self.self_check_success_count += 1
        self.self_check_success_total_duration += delta_seconds
        if delta_seconds > self.self_check_success_max_duration:
            self.self_check_success_max_duration = delta_seconds

    # --- lifecycle ---

    def close(self) -> None:
        if self._sync_connection:
            logger.info('Closing service broker sync connection')
            try:
                self._sync_connection.close()
            except Exception:
                pass
            self._sync_connection = None

    async def aclose(self) -> None:
        if self._async_connection:
            logger.info('Closing service broker async connection')
            try:
                self._async_connection.close()
            except Exception:
                pass
            self._async_connection = None

        self.self_check_status = BrokerSelfCheckStatus.IDLE
        self.last_self_check_message_time = time.monotonic() if self.max_connection_idle_seconds is not None else None
        self.notify_loop_active = False
        self.notify_queue = []
