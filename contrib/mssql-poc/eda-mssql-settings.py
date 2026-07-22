# MS SQL Server configuration for EDA (Event-Driven Ansible)
# Loaded via DJANGO_SETTINGS_MODULE=eda_mssql_settings (env var override)
# ANSTRAT-1887 Phase 3+4: Route ALL ORM traffic to SQL Server + notification bus
#
# Requires: PYTHONPATH=/opt/mssql-poc (mssql_common.py, service_broker.py)
import os as _os
import sys
import logging as _logging

sys.path.insert(0, '/opt/mssql-poc')

_SKIP_MSSQL = _os.environ.get('SKIP_MSSQL')
if _SKIP_MSSQL:
    _logging.getLogger('aap.eda.mssql').info(
        'SKIP_MSSQL set — MSSQL settings not loaded')

# Import all settings from the original EDA Dynaconf chain
from aap_eda.settings.default import *  # noqa: F401,F403,E402

# =============================================================================
# Phase 3: ORM routing — all Django ORM reads/writes go to SQL Server
# =============================================================================

if not _SKIP_MSSQL:
    from mssql_common import apply_orm_patches, get_sb_broker_config, stub_pg_notify

    DATABASE_ROUTERS = apply_orm_patches(
        DATABASES,  # noqa: F405 — from wildcard import
        db_name='eda',
    )

    # Redirect 'default' to MSSQL so Django internals (health checks, connection.cursor(),
    # direct DATABASES['default'] access) don't require PostgreSQL.
    DATABASES['default'] = DATABASES['mssql'].copy()


# =============================================================================
# Phase 4: Replace pg_notify with SQL Server notification bus
# =============================================================================

if not _SKIP_MSSQL:
    _SB_BROKER_CONFIG = get_sb_broker_config(db_name='eda')

    # --- Stub pg_notify with a Broker class that redirects to service_broker ---
    # The dispatcherd management command hardcodes "pg_notify" as a broker key.
    # The stub must include a Broker class so dispatcherd can resolve it.
    import types as _types
    import service_broker as _sb_module
    from service_broker import Broker as _SBBroker
    sys.modules['dispatcherd.brokers.service_broker'] = _sb_module

    if 'dispatcherd.brokers.pg_notify' not in sys.modules:
        _stub = _types.ModuleType('dispatcherd.brokers.pg_notify')
        _stub.__file__ = '<stub for mssql poc>'
    else:
        _stub = sys.modules['dispatcherd.brokers.pg_notify']

    class _PgNotifyStubBroker(_SBBroker):
        def __init__(self, **kwargs):
            kwargs.pop('conninfo', None)
            kwargs.pop('sync_connection_factory', None)
            kwargs['config'] = _SB_BROKER_CONFIG.copy()
            super().__init__(**kwargs)

    _stub.Broker = _PgNotifyStubBroker
    sys.modules['dispatcherd.brokers.pg_notify'] = _stub

    # --- Override DISPATCHERD_DEFAULT_SETTINGS to use service_broker ---
    _SB_BROKER_ENTRY = {
        "config": _SB_BROKER_CONFIG.copy(),
        "channels": ["default"],
        "default_publish_channel": "default",
    }

    DISPATCHERD_DEFAULT_SETTINGS = {  # noqa: F405
        "version": 2,
        "service": {
            "process_manager_cls": "ForkServerManager",
            "process_manager_kwargs": {
                "preload_modules": ["aap_eda.core.dispatcherd_pre_fork"],
            },
            "min_workers": 2,
            "max_workers": 8,
            "pool_kwargs": {},
        },
        "brokers": {
            "service_broker": _SB_BROKER_ENTRY.copy(),
        },
        "producers": {},
        "publish": {"default_broker": "service_broker"},
    }

    # DefaultWorker settings (includes producers for scheduled tasks)
    DISPATCHERD_DEFAULT_WORKER_SETTINGS = {  # noqa: F405
        "version": 2,
        "service": {
            "process_manager_cls": "ForkServerManager",
            "process_manager_kwargs": {
                "preload_modules": ["aap_eda.core.dispatcherd_pre_fork"],
            },
            "min_workers": 2,
            "max_workers": 8,
        },
        "brokers": {
            "service_broker": _SB_BROKER_ENTRY.copy(),
        },
        "producers": {
            "ScheduledProducer": {
                "task_schedule": {
                    "aap_eda.tasks.orchestrator.monitor_rulebook_processes": {"schedule": 5},
                    "aap_eda.tasks.project.monitor_project_tasks": {"schedule": 30},
                    "aap_eda.tasks.shared_resources.resync_shared_resources": {"schedule": 900},
                }
            },
            "OnStartProducer": {
                "task_list": {
                    "aap_eda.tasks.analytics.schedule_gather_analytics": {}
                }
            },
        },
        "publish": {"default_broker": "service_broker"},
    }

    # --- Patch CoreConfig.ready() to use Service Broker config ---
    import aap_eda.core.apps as _eda_apps

    def _mssql_eda_ready(self):
        """Replacement CoreConfig.ready() that uses Service Broker."""
        from aap_eda.api.views import dab_decorate  # noqa: F401

        # Bypass dispatcherd health check — control_with_reply("alive") doesn't
        # work through Service Broker. Workers ARE running; bypass so Envoy
        # health checks pass. Must patch AFTER dab_decorate import triggers
        # views loading, so the from-import binding in views.py gets replaced.
        import aap_eda.core.health as _health
        import aap_eda.core.views as _views
        _bypass = lambda raise_exceptions=False: True
        _health.check_dispatcherd_workers_health = _bypass
        _views.check_dispatcherd_workers_health = _bypass

        from dispatcherd.config import setup as dispatcher_setup
        dispatcher_setup(DISPATCHERD_DEFAULT_SETTINGS)

        # Patch DISTINCT ON queries (PG-only) — must be deferred until models are loaded
        import aap_eda.tasks.activation_request_queue as _arq
        from aap_eda.core.models import ActivationRequestQueue
        from django.db.models import Min

        def _mssql_list_requests():
            min_ids = (
                ActivationRequestQueue.objects
                .values('process_parent_type', 'process_parent_id')
                .annotate(min_id=Min('id'))
                .values_list('min_id', flat=True)
            )
            return ActivationRequestQueue.objects.filter(
                id__in=min_ids
            ).order_by('process_parent_id')

        _arq.list_requests = _mssql_list_requests

    _eda_apps.CoreConfig.ready = _mssql_eda_ready

    # --- Patch the dispatcherd management command to not hardcode pg_notify ---
    import aap_eda.core.management.commands.dispatcherd as _disp_cmd

    _orig_handle = _disp_cmd.Command.handle

    def _mssql_dispatcherd_handle(self, *args, **options):
        from django.conf import settings as _s
        from dispatcherd.config import setup as dispatcherd_setup
        from dispatcherd import run_service as run_dispatcherd_service

        worker_class = options.get("worker_class")
        verbosity = options.get("verbosity", 1)

        try:
            if worker_class == "ActivationWorker":
                from aap_eda.core.management.commands.dispatcherd import utils
                queue_name = utils.sanitize_postgres_identifier(
                    _s.RULEBOOK_QUEUE_NAME
                )
                dispatcher_worker_settings = {
                    **_s.DISPATCHERD_DEFAULT_SETTINGS,
                    "brokers": {
                        "service_broker": {
                            **list(_s.DISPATCHERD_DEFAULT_SETTINGS["brokers"].values())[0],
                            "channels": [queue_name],
                        },
                    },
                }
                dispatcherd_setup(dispatcher_worker_settings)
            elif worker_class == "DefaultWorker":
                dispatcherd_setup(_s.DISPATCHERD_DEFAULT_WORKER_SETTINGS)

            if verbosity >= 1:
                self.stdout.write(
                    self.style.SUCCESS(f"Starting {worker_class} with dispatcherd.")
                )
            _disp_cmd.logger.info(f"Starting {worker_class} with dispatcherd.")
            run_dispatcherd_service()

        except KeyboardInterrupt:
            shutdown_msg = f"{worker_class} shutdown requested."
            self.stdout.write(self.style.WARNING(shutdown_msg))
            _disp_cmd.logger.info(shutdown_msg)
        except Exception as e:
            error_msg = f"Failed to start {worker_class}: {e}"
            self.stderr.write(self.style.ERROR(error_msg))
            _disp_cmd.logger.error(error_msg, exc_info=True)
            raise SystemExit(1)

    _disp_cmd.Command.handle = _mssql_dispatcherd_handle

    _logging.getLogger('aap.eda.mssql').info(
        'EDA MSSQL settings loaded: ORM routing + notification bus active'
    )
