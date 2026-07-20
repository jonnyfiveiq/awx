# MS SQL Server configuration for AAP Gateway
# Appended to /etc/ansible-automation-platform/gateway/settings.py via Secret patch
# ANSTRAT-1887 Phase 3+4: Route ALL ORM traffic to SQL Server + notification bus
#
# Requires: mssql_common.py installed at /opt/mssql-poc/mssql_common.py
import os as _os
import sys
sys.path.insert(0, '/opt/mssql-poc')

import logging as _logging

_SKIP_MSSQL = _os.environ.get('SKIP_MSSQL')
if _SKIP_MSSQL:
    _logging.getLogger('aap.gateway.mssql').info(
        'SKIP_MSSQL set — MSSQL settings not loaded (init container mode)')

# =============================================================================
# Phase 3: ORM routing — all Django ORM reads/writes go to SQL Server
# =============================================================================

if not _SKIP_MSSQL:
    from mssql_common import apply_orm_patches, get_sb_broker_config, stub_pg_notify, add_mssql_healthcheck

    DATABASE_ROUTERS = apply_orm_patches(
        DATABASES,
        db_name='aap_gateway',
        include_healthcheck=True,
    )


# =============================================================================
# Phase 4: Replace pg_notify with SQL Server notification bus
# =============================================================================

if not _SKIP_MSSQL:
    stub_pg_notify()

    import socket as _socket

    _SB_BROKER_CONFIG = get_sb_broker_config(db_name='aap_gateway')

    import aap_gateway_api.apps as _gw_apps_mod

    _dispatch_patches_state = {'applied': False}

    def _mssql_gw_ready(self):
        """Replacement ready() that patches dispatcherd config for Service Broker."""
        from django.db.models import signals
        from dynamic_preferences.signals import preference_updated

        from aap_gateway_api.signals.preloaded_data import create_preload_data
        from aap_gateway_api.utils.preferences import initialize_preferences

        signals.post_migrate.connect(
            lambda sender, **kwargs: initialize_preferences(),
            sender=self, weak=False
        )
        signals.post_migrate.connect(
            lambda sender, **kwargs: create_preload_data(**kwargs),
            sender=self, weak=False
        )

        from aap_gateway_api.preferences import gateway_preference_registry

        def _notify_on_preference_update(sender, section, name, old_value, new_value, **kwargs):
            preference = gateway_preference_registry.get(name, section)
            if preference.on_update:
                preference.on_update(old_value, new_value)

        preference_updated.connect(_notify_on_preference_update)

        if not _dispatch_patches_state['applied']:
            _dispatch_patches_state['applied'] = True

            import aap_gateway_api.dispatch.config as _dispatch_config

            def _mssql_get_dispatcherd_config():
                from django.conf import settings as _settings
                _cluster_host_id = getattr(_settings, 'CLUSTER_HOST_ID', _socket.gethostname())

                return {
                    "version": 2,
                    "service": {
                        "process_manager_cls": "ForkServerManager",
                        "process_manager_kwargs": {
                            "preload_modules": ["aap_gateway_api.dispatch.pre_fork"],
                        },
                        "min_workers": getattr(_settings, "DISPATCHERD_MIN_WORKERS", 2),
                        "max_workers": getattr(_settings, "DISPATCHERD_MAX_WORKERS", 4),
                    },
                    "brokers": {
                        "aap_gateway_api.dispatch.brokers.service_broker": {
                            "config": _SB_BROKER_CONFIG.copy(),
                            "channels": [
                                _cluster_host_id,
                                "gateway_broadcast",
                            ],
                            "default_publish_channel": "gateway_broadcast",
                        }
                    },
                    "producers": {},
                    "publish": {"default_broker": "aap_gateway_api.dispatch.brokers.service_broker"},
                }

            _dispatch_config.get_dispatcherd_config = _mssql_get_dispatcherd_config

        from dispatcherd.config import setup as dispatcherd_setup
        from aap_gateway_api.dispatch.config import get_dispatcherd_config
        dispatcherd_setup(get_dispatcherd_config())

        import aap_gateway_api.signals  # noqa: F401

        from aap_gateway_api.views.api.v1.ping import PingView
        def _mssql_check_db(self):
            from django.db import connections
            with connections['mssql'].cursor() as cursor:
                cursor.execute('SELECT 1')
        PingView._check_db = _mssql_check_db

        # Patch Envoy xDS: replace DISTINCT ON with MSSQL-compatible query
        from aap_gateway_api.views.api.envoy.rest_control_plane import XDSView

        def _mssql_get_qs(self, request, ModelClass, name_field):
            from django.db.models import Min
            ids = (ModelClass.objects
                   .values(name_field)
                   .annotate(_first_id=Min('id'))
                   .values_list('_first_id', flat=True))
            qs = ModelClass.objects.filter(id__in=ids)
            if names := request.POST.get("resource_names"):
                if len(names) == 1 and names[0] == "*":
                    return qs
                qs = qs.filter(**{f"{name_field}__in": names})
            return qs

        XDSView.get_qs = _mssql_get_qs

    _gw_apps_mod.MyAppConfig.ready = _mssql_gw_ready

    _logging.getLogger('aap.gateway.mssql').info(
        'Gateway MSSQL settings loaded: ORM routing + notification bus active'
    )
