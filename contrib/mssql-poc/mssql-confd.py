# MS SQL Server dual-database configuration for AAP Controller
# Loaded via /etc/tower/conf.d/mssql.py (AWX settings glob)
# ANSTRAT-1887 Phase 3: Route ALL ORM traffic to SQL Server
import os

# --- SQL Server database connection ---
DATABASES['mssql'] = {
    'ENGINE': 'mssql',
    'NAME': 'awx',
    'USER': 'SA',
    'PASSWORD': os.environ.get('MSSQL_SA_PASSWORD', 'AAP_P0C_Password_2026!'),
    'HOST': 'host.docker.internal',
    'PORT': '1433',
    'OPTIONS': {
        'driver': 'ODBC Driver 18 for SQL Server',
        'extra_params': 'TrustServerCertificate=yes',
    },
    'ATOMIC_REQUESTS': True,
}

from django.db.backends.signals import connection_created

def _mssql_connection_init(sender, connection, **kwargs):
    if connection.alias == 'mssql':
        with connection.cursor() as cursor:
            cursor.execute("SET XACT_ABORT OFF")

connection_created.connect(_mssql_connection_init, weak=False)

# --- Database Router: ALL ORM → mssql, PG stays for pg_notify only ---
class MSSQLRouter:
    def db_for_read(self, model, **hints):
        return 'mssql'

    def db_for_write(self, model, **hints):
        return 'mssql'

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == 'mssql':
            return False
        return None


DATABASE_ROUTERS = [MSSQLRouter()]

# --- Monkey-patch: advisory_lock no-op for SQL Server ---
from contextlib import contextmanager


@contextmanager
def _mssql_advisory_lock_noop(name, wait=False, lock_session_timeout_milliseconds=None):
    yield True


import ansible_base.lib.utils.db as _db_utils
_db_utils.advisory_lock = _mssql_advisory_lock_noop

# --- Monkey-patch: transaction.atomic() defaults to 'mssql' instead of 'default' ---
import django.db.transaction as _tx
_orig_atomic = _tx.atomic

class _MSSQLAtomicWrapper:
    def __new__(cls, using=None, *args, **kwargs):
        if using is None:
            using = 'mssql'
        return _orig_atomic(using, *args, **kwargs)

_tx.atomic = _MSSQLAtomicWrapper

_orig_on_commit = _tx.on_commit

def _mssql_on_commit(func, using=None, robust=False):
    if using is None:
        using = 'mssql'
    return _orig_on_commit(func, using=using, robust=robust)

_tx.on_commit = _mssql_on_commit

# --- Monkey-patch: bulk_create(ignore_conflicts=True) for SQL Server ---
from django.db.models.query import QuerySet as _QS
_orig_bulk_create = _QS.bulk_create


def _mssql_bulk_create(self, objs, *args, ignore_conflicts=False, **kwargs):
    if ignore_conflicts and self.db == 'mssql':
        created = []
        for obj in objs:
            try:
                created.extend(_orig_bulk_create(self, [obj], *args, **kwargs))
            except Exception:
                pass
        return created
    return _orig_bulk_create(self, objs, *args, ignore_conflicts=ignore_conflicts, **kwargs)

_QS.bulk_create = _mssql_bulk_create

import logging as _logging
_mssql_logger = _logging.getLogger('awx.main.mssql')

def _mssql_patched_update_host_metrics(updated_hosts_list):
    from awx.main.models import HostMetric
    from django.utils.timezone import now
    from django.db import connections
    import itertools

    current_time = now()
    args = [iter(updated_hosts_list)] * 500
    for hosts in itertools.zip_longest(*args):
        hosts = [h for h in hosts if h is not None]
        if not hosts:
            continue
        try:
            conn = connections['mssql']
            conn.needs_rollback = False
            with conn.cursor() as cursor:
                for hostname in hosts:
                    cursor.execute(
                        "IF NOT EXISTS (SELECT 1 FROM main_hostmetric WHERE hostname = %s) "
                        "INSERT INTO main_hostmetric (hostname, first_automation, last_automation, automated_counter, deleted_counter, deleted, used_in_inventories) "
                        "VALUES (%s, %s, %s, 1, 0, 0, 0)",
                        [hostname, hostname, current_time, current_time]
                    )
                    cursor.execute(
                        "UPDATE main_hostmetric SET last_automation = %s, automated_counter = automated_counter + 1, deleted = 0 WHERE hostname = %s",
                        [current_time, hostname]
                    )
        except Exception as e:
            _mssql_logger.warning(f'MSSQL host metrics update skipped: {e}')

_hm_patch_state = {'applied': False, 'fn': _mssql_patched_update_host_metrics}

def _apply_host_metrics_patch(sender, connection, **kwargs):
    if _hm_patch_state['applied']:
        return
    if connection.alias == 'mssql':
        try:
            from awx.main.models.events import JobEvent
            JobEvent._update_host_metrics = staticmethod(_hm_patch_state['fn'])
            _hm_patch_state['applied'] = True
        except Exception:
            pass

connection_created.connect(_apply_host_metrics_patch, weak=False)


# =============================================================================
# Phase 4: Replace pg_notify with SQL Server notification bus
# Eliminates the last PostgreSQL dependency (dispatcherd, PubSub, wsrelay)
# =============================================================================

import json as _json
import asyncio as _asyncio
from collections import namedtuple as _namedtuple

_SBNotification = _namedtuple('SBNotification', ['channel', 'payload'])

_MSSQL_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=host.docker.internal,1433;"
    f"DATABASE=awx;"
    f"UID=SA;"
    f"PWD={os.environ.get('MSSQL_SA_PASSWORD', 'AAP_P0C_Password_2026!')};"
    "TrustServerCertificate=yes;"
)

_SB_BROKER_CONFIG = {
    'server': 'host.docker.internal,1433',
    'database': 'awx',
    'user': 'SA',
    'password': os.environ.get('MSSQL_SA_PASSWORD', 'AAP_P0C_Password_2026!'),
    'driver': 'ODBC Driver 18 for SQL Server',
    'TrustServerCertificate': 'yes',
}


def _sb_create_connection():
    import pyodbc
    return pyodbc.connect(_MSSQL_CONN_STR, autocommit=True)


# --- A. Monkey-patch get_dispatcherd_config: swap pg_notify → service_broker ---

import awx.main.dispatch.config as _dispatch_config
_orig_get_dispatcherd_config = _dispatch_config.get_dispatcherd_config


def _mssql_get_dispatcherd_config(for_service=False, mock_publish=False):
    config = _orig_get_dispatcherd_config(for_service=for_service, mock_publish=mock_publish)
    if not mock_publish:
        config['brokers'].pop('pg_notify', None)
        config['brokers']['awx.main.dispatch.brokers.service_broker'] = {
            'config': _SB_BROKER_CONFIG.copy(),
            'default_publish_channel': config.get('brokers', {}).get('pg_notify', {}).get(
                'default_publish_channel', CLUSTER_HOST_ID
            ) if 'pg_notify' in config.get('brokers', {}) else CLUSTER_HOST_ID,
        }
        config['publish']['default_broker'] = 'awx.main.dispatch.brokers.service_broker'
        if for_service:
            from awx.main.dispatch import get_task_queuename
            config['brokers']['awx.main.dispatch.brokers.service_broker']['channels'] = [
                'tower_broadcast_all', 'tower_settings_change', get_task_queuename()
            ]
    return config


_dispatch_config.get_dispatcherd_config = _mssql_get_dispatcherd_config


# --- B. Monkey-patch configure_dispatcherd: don't check connection.vendor ---

import awx.main.apps as _apps_mod

def _mssql_configure_dispatcherd(self):
    from awx.main.dispatch.config import get_dispatcherd_config
    from dispatcherd.config import setup as dispatcher_setup
    config_dict = get_dispatcherd_config()
    dispatcher_setup(config_dict)

_apps_mod.MainConfig.configure_dispatcherd = _mssql_configure_dispatcherd


# --- C. Monkey-patch prefork.py: stub pg_notify import ---

import sys as _sys
import types as _types

if 'dispatcherd.brokers.pg_notify' not in _sys.modules:
    _stub = _types.ModuleType('dispatcherd.brokers.pg_notify')
    _stub.__file__ = '<stub for mssql poc>'
    _sys.modules['dispatcherd.brokers.pg_notify'] = _stub


# --- D. Replace PubSub class and pg_bus_conn ---

class _ServiceBrokerPubSub:
    def __init__(self, conn, select_timeout=None):
        self.conn = conn
        self.select_timeout = select_timeout or 5
        self._listening_channels = set()

    def listen(self, channel):
        self._listening_channels.add(channel)

    def unlisten(self, channel):
        self._listening_channels.discard(channel)

    def notify(self, channel, payload):
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO awx_notify_messages (channel, payload) VALUES (?, ?)",
                channel, payload
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
            except Exception:
                pass
        finally:
            cursor.close()

    def events(self):
        import pyodbc
        cursor = self.conn.cursor()
        cursor.execute("SELECT ISNULL(MAX(id), 0) FROM awx_notify_messages")
        last_seen = cursor.fetchone()[0]
        cursor.close()

        while True:
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    "WAITFOR (RECEIVE TOP(1) conversation_handle, message_type_name "
                    "FROM [AwxSignalQueue]), TIMEOUT ?",
                    self.select_timeout * 1000
                )
                row = cursor.fetchone()
                if row:
                    try:
                        cursor.execute("END CONVERSATION ?", row[0])
                    except Exception:
                        pass
                cursor.close()
            except Exception:
                import time
                time.sleep(self.select_timeout)

            cursor = self.conn.cursor()
            placeholders = ','.join('?' for _ in self._listening_channels)
            if not self._listening_channels:
                cursor.close()
                yield None
                continue

            cursor.execute(
                f"SELECT id, channel, payload FROM awx_notify_messages "
                f"WHERE id > ? AND channel IN ({placeholders}) ORDER BY id",
                last_seen, *self._listening_channels
            )
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                yield None
            else:
                for row in rows:
                    last_seen = row[0]
                    yield _SBNotification(row[1], row[2])

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


@contextmanager
def _sb_pg_bus_conn(new_connection=False, select_timeout=None):
    conn = _sb_create_connection()
    pubsub = _ServiceBrokerPubSub(conn, select_timeout=select_timeout)
    yield pubsub
    try:
        conn.close()
    except Exception:
        pass


import awx.main.dispatch as _dispatch_mod
_dispatch_mod.PubSub = _ServiceBrokerPubSub
_dispatch_mod.pg_bus_conn = _sb_pg_bus_conn
_dispatch_mod.create_listener_connection = _sb_create_connection


# --- E. Patch WebSocketRelayManager to use SQL Server instead of psycopg ---

import awx.main.wsrelay as _wsrelay_mod


async def _sb_on_ws_heartbeat_loop(mgr):
    """Service Broker replacement for WebSocketRelayManager.on_ws_heartbeat"""
    import pyodbc

    conn = _sb_create_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ISNULL(MAX(id), 0) FROM awx_notify_messages")
    last_seen = cursor.fetchone()[0]
    cursor.close()

    loop = _asyncio.get_event_loop()

    def _wait_and_poll():
        nonlocal last_seen
        try:
            c = conn.cursor()
            c.execute(
                "WAITFOR (RECEIVE TOP(1) conversation_handle, message_type_name "
                "FROM [AwxSignalQueue]), TIMEOUT 30000"
            )
            row = c.fetchone()
            if row:
                try:
                    c.execute("END CONVERSATION ?", row[0])
                except Exception:
                    pass
            c.close()
        except Exception:
            import time
            time.sleep(5)

        c = conn.cursor()
        c.execute(
            "SELECT id, channel, payload FROM awx_notify_messages "
            "WHERE id > ? AND channel = ? ORDER BY id",
            last_seen, 'web_ws_heartbeat'
        )
        rows = c.fetchall()
        c.close()
        results = []
        for r in rows:
            last_seen = r[0]
            results.append(r[2])
        return results

    while True:
        payloads = await loop.run_in_executor(None, _wait_and_poll)
        for payload_str in payloads:
            try:
                payload = _json.loads(payload_str)
            except _json.JSONDecodeError:
                continue

            if payload.get('hostname') == mgr.local_hostname:
                continue

            action = payload.get('action')
            hostname = payload.get('hostname')
            ip = payload.get('ip') or hostname

            if action in ('online', 'offline'):
                if ip is None:
                    continue

            if action == 'online':
                mgr.known_hosts[hostname] = ip
            elif action == 'offline':
                await mgr.cleanup_offline_host(hostname)


_orig_wsrelay_run = _wsrelay_mod.WebSocketRelayManager.run


async def _sb_wsrelay_run(self):
    """Replacement run() that uses SQL Server instead of psycopg"""
    from awx.main.wsrelay import RelayWebsocketStatsManager, WebsocketRelayConnection

    self.stats_mgr = RelayWebsocketStatsManager(self.local_hostname)
    self.stats_mgr.start()

    on_ws_heartbeat_task = _asyncio.get_running_loop().create_task(
        _sb_on_ws_heartbeat_loop(self),
        name="WebSocketRelayManager.on_ws_heartbeat_sb",
    )

    while True:
        if on_ws_heartbeat_task.done():
            exc = on_ws_heartbeat_task.exception()
            raise Exception(f"on_ws_heartbeat_task has exited: {exc}")

        future_remote_hosts = self.known_hosts.keys()
        current_remote_hosts = self.relay_connections.keys()
        deleted_remote_hosts = set(current_remote_hosts) - set(future_remote_hosts)
        new_remote_hosts = set(future_remote_hosts) - set(current_remote_hosts)

        for hostname, address in self.known_hosts.items():
            if hostname not in self.relay_connections:
                continue
            if address != self.relay_connections[hostname].remote_host:
                deleted_remote_hosts.add(hostname)
                new_remote_hosts.add(hostname)

        for hostname, relay_conn in self.relay_connections.items():
            if not relay_conn.connected:
                deleted_remote_hosts.add(hostname)

        if deleted_remote_hosts:
            await _asyncio.gather(*[self.cleanup_offline_host(h) for h in deleted_remote_hosts])

        for h in new_remote_hosts:
            stats = self.stats_mgr.new_remote_host_stats(h)
            relay_connection = WebsocketRelayConnection(
                name=self.local_hostname, stats=stats, remote_host=self.known_hosts[h]
            )
            relay_connection.start()
            self.relay_connections[h] = relay_connection

        from django.conf import settings as _settings
        await _asyncio.sleep(_settings.BROADCAST_WEBSOCKET_NEW_INSTANCE_POLL_RATE_SECONDS)


_wsrelay_mod.WebSocketRelayManager.run = _sb_wsrelay_run
