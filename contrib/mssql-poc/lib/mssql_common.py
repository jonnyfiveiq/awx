"""
Shared MSSQL migration patches for AAP components.
ANSTRAT-1887: Platform lifecycle as code — SQL Server POC

Provides reusable ORM routing, monkey-patches, and notification bus
configuration used by both Controller and Gateway (and future components).

Usage from a Django settings file (exec'd context):
    import sys
    sys.path.insert(0, '/opt/mssql-poc')
    from mssql_common import apply_orm_patches, get_sb_broker_config, stub_pg_notify
    apply_orm_patches(DATABASES, db_name='awx')
"""
import os
import sys
import types
import logging
from contextlib import contextmanager

logger = logging.getLogger('aap.mssql_common')

# ─── Default connection parameters ───────────────────────────────────────────

DEFAULT_MSSQL_HOST = 'host.docker.internal'
DEFAULT_MSSQL_PORT = '1433'
DEFAULT_MSSQL_USER = 'SA'
DEFAULT_MSSQL_PASSWORD = os.environ.get('MSSQL_SA_PASSWORD', 'AAP_P0C_Password_2026!')
DEFAULT_MSSQL_DRIVER = 'ODBC Driver 18 for SQL Server'


def _mssql_db_entry(db_name, host=None, port=None, user=None, password=None, **extra):
    """Build a DATABASES entry dict for an MSSQL alias."""
    return {
        'ENGINE': 'mssql',
        'NAME': db_name,
        'USER': user or DEFAULT_MSSQL_USER,
        'PASSWORD': password or DEFAULT_MSSQL_PASSWORD,
        'HOST': host or DEFAULT_MSSQL_HOST,
        'PORT': port or DEFAULT_MSSQL_PORT,
        'OPTIONS': {
            'driver': DEFAULT_MSSQL_DRIVER,
            'extra_params': 'TrustServerCertificate=yes',
        },
        'ATOMIC_REQUESTS': True,
        'AUTOCOMMIT': True,
        'CONN_MAX_AGE': 0,
        'CONN_HEALTH_CHECKS': False,
        'TIME_ZONE': None,
        'TEST': {},
        **extra,
    }


# ─── ORM patches ─────────────────────────────────────────────────────────────

def add_mssql_database(databases, db_name, host=None, port=None, user=None, password=None):
    """Add 'mssql' alias to the DATABASES dict."""
    databases['mssql'] = _mssql_db_entry(db_name, host, port, user, password)


def add_mssql_healthcheck(databases, db_name, host=None, port=None, user=None, password=None):
    """Add or override 'healthcheck' alias pointing at MSSQL."""
    databases['healthcheck'] = _mssql_db_entry(
        db_name, host, port, user, password,
        CONN_MAX_AGE=0,
        CONN_HEALTH_CHECKS=True,
        TEST={'MIRROR': 'default'},
    )
    databases['healthcheck']['ATOMIC_REQUESTS'] = False
    databases['healthcheck'].setdefault('AUTOCOMMIT', True)
    databases['healthcheck'].setdefault('CONN_MAX_AGE', 0)
    databases['healthcheck'].setdefault('TIME_ZONE', None)


def install_xact_abort_handler():
    """SET XACT_ABORT OFF on every new mssql/healthcheck connection.

    SQL Server defaults XACT_ABORT to ON inside explicit transactions,
    which causes the entire transaction to abort on any error. Django
    expects per-statement error handling, so we turn it off.
    """
    from django.db.backends.signals import connection_created

    def _on_connection_created(sender, connection, **kwargs):
        if getattr(connection, 'vendor', '') == 'microsoft' or connection.alias in ('mssql',):
            with connection.cursor() as cursor:
                cursor.execute("SET XACT_ABORT OFF")

    connection_created.connect(_on_connection_created, weak=False)


def install_mssql_router():
    """Install a database router that sends ALL ORM traffic to 'mssql'.

    Returns the DATABASE_ROUTERS list to assign in settings.
    """
    class MSSQLRouter:
        def db_for_read(self, model, **hints):
            return 'mssql'

        def db_for_write(self, model, **hints):
            return 'mssql'

        def allow_relation(self, obj1, obj2, **hints):
            return True

        def allow_migrate(self, db, app_label, model_name=None, **hints):
            if db == 'mssql':
                return True
            return None

    return [MSSQLRouter()]


def install_advisory_lock_noop():
    """Replace DAB's advisory_lock with a no-op context manager.

    The real implementation uses pg_advisory_lock() which is PG-only.
    For the POC, concurrent lock contention is not a concern.
    """
    @contextmanager
    def _noop(name, wait=False, lock_session_timeout_milliseconds=None):
        yield True

    import ansible_base.lib.utils.db as _db_utils
    _db_utils.advisory_lock = _noop


def install_transaction_patches():
    """Patch transaction.atomic() and on_commit() to default to 'mssql'.

    Without this, any code calling atomic() or on_commit() without
    specifying `using=` would hit the 'default' (PG) database.
    """
    import django.db.transaction as _tx
    _orig_atomic = _tx.atomic
    _orig_on_commit = _tx.on_commit

    class _MSSQLAtomicWrapper:
        def __new__(cls, using=None, *args, **kwargs):
            if using is None:
                using = 'mssql'
            return _orig_atomic(using, *args, **kwargs)

    def _mssql_on_commit(func, using=None, robust=False):
        if using is None:
            using = 'mssql'
        return _orig_on_commit(func, using=using, robust=robust)

    _tx.atomic = _MSSQLAtomicWrapper
    _tx.on_commit = _mssql_on_commit


def install_uuid_format_patch():
    """Use native uniqueidentifier type for UUIDField on SQL Server.

    mssql-django defaults to char(32) for UUIDField, but this causes
    "Insufficient result space to convert uniqueidentifier value to char"
    in INSERT...OUTPUT INSERTED statements because the OUTPUT clause
    tries to fit a 36-char uniqueidentifier into a 32-char column.

    Fix: override UUIDField.db_type() to return 'uniqueidentifier' and
    set has_native_uuid_field=True so Django passes UUID objects directly.
    We cannot import mssql.base.DatabaseWrapper here (circular import
    during settings loading), so we patch the field method instead.
    """
    from mssql.features import DatabaseFeatures
    DatabaseFeatures.has_native_uuid_field = True

    from django.db import models
    _orig_uuid_db_type = models.UUIDField.db_type

    def _mssql_uuid_db_type(self, connection):
        if connection.vendor == 'microsoft':
            return 'uniqueidentifier'
        return _orig_uuid_db_type(self, connection)

    models.UUIDField.db_type = _mssql_uuid_db_type


def install_bulk_create_patch():
    """Patch bulk_create for SQL Server.

    Two issues on MSSQL:
    1. ignore_conflicts maps to ON CONFLICT DO NOTHING (PG-only)
    2. OUTPUT INSERTED.* with uniqueidentifier columns causes
       "Insufficient result space to convert uniqueidentifier to char"

    Fallback: insert rows one-by-one via save() on MSSQL.
    """
    from django.db.models.query import QuerySet as _QS
    _orig_bulk_create = _QS.bulk_create

    def _mssql_bulk_create(self, objs, *args, ignore_conflicts=False, **kwargs):
        if self.db == 'mssql':
            created = []
            for obj in objs:
                try:
                    obj.save(using='mssql')
                    created.append(obj)
                except Exception:
                    if not ignore_conflicts:
                        raise
            return created
        return _orig_bulk_create(self, objs, *args, ignore_conflicts=ignore_conflicts, **kwargs)

    _QS.bulk_create = _mssql_bulk_create


def install_textfield_index_patch():
    """Cap TextField to nvarchar(450) on MSSQL.

    SQL Server cannot index nvarchar(max) columns. EDA uses
    TextField(unique=True) and explicit Index() on TextField columns
    extensively. For the POC, all TextFields use nvarchar(450) on MSSQL
    to avoid index creation failures. 450 nvarchar chars = 900 bytes,
    the SQL Server index key limit.
    """
    from django.db import models
    _orig_db_type = models.TextField.db_type

    def _mssql_text_db_type(self, connection):
        if connection.vendor == 'microsoft':
            return 'nvarchar(450)'
        return _orig_db_type(self, connection)

    models.TextField.db_type = _mssql_text_db_type


def apply_orm_patches(databases, db_name, host=None, port=None, user=None, password=None,
                      include_healthcheck=False):
    """Apply all ORM patches in one call.

    Args:
        databases: The Django DATABASES dict to modify.
        db_name: SQL Server database name (e.g. 'awx', 'aap_gateway').
        host/port/user/password: Connection parameters (defaults apply).
        include_healthcheck: If True, also override the 'healthcheck' alias.

    Returns:
        DATABASE_ROUTERS list to assign in settings.
    """
    add_mssql_database(databases, db_name, host, port, user, password)
    if include_healthcheck:
        add_mssql_healthcheck(databases, db_name, host, port, user, password)
    install_xact_abort_handler()
    install_advisory_lock_noop()
    install_transaction_patches()
    install_uuid_format_patch()
    install_bulk_create_patch()
    install_textfield_index_patch()
    return install_mssql_router()


# ─── Notification bus ─────────────────────────────────────────────────────────

def get_sb_broker_config(db_name, host=None, port=None, user=None, password=None):
    """Build a Service Broker config dict for dispatcherd."""
    _host = host or DEFAULT_MSSQL_HOST
    _port = port or DEFAULT_MSSQL_PORT
    return {
        'server': f'{_host},{_port}',
        'database': db_name,
        'user': user or DEFAULT_MSSQL_USER,
        'password': password or DEFAULT_MSSQL_PASSWORD,
        'driver': DEFAULT_MSSQL_DRIVER,
        'TrustServerCertificate': 'yes',
    }


def stub_pg_notify():
    """Stub dispatcherd.brokers.pg_notify in sys.modules.

    The dispatcherd ForkServerManager pre-fork module imports pg_notify.
    Stubbing prevents psycopg from being required at import time.
    """
    if 'dispatcherd.brokers.pg_notify' not in sys.modules:
        stub = types.ModuleType('dispatcherd.brokers.pg_notify')
        stub.__file__ = '<stub for mssql poc>'
        sys.modules['dispatcherd.brokers.pg_notify'] = stub


def get_odbc_connection_string(db_name, host=None, port=None, user=None, password=None):
    """Build a pyodbc connection string for direct ODBC use."""
    _host = host or DEFAULT_MSSQL_HOST
    _port = port or DEFAULT_MSSQL_PORT
    return (
        f"DRIVER={{{DEFAULT_MSSQL_DRIVER}}};"
        f"SERVER={_host},{_port};"
        f"DATABASE={db_name};"
        f"UID={user or DEFAULT_MSSQL_USER};"
        f"PWD={password or DEFAULT_MSSQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
