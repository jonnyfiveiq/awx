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
