# AAP on MS SQL Server - POC Guide

**ANSTRAT-1887: Bring Your Own Database (BYOD) - MS SQL Server**

This guide covers running the full AAP platform (Controller + Gateway + EDA) with ALL ORM traffic on Microsoft SQL Server, with PostgreSQL completely eliminated. It documents a working deployment proven on a Kind cluster with AAP 2.7.

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Environment Reference](#environment-reference)
- [Part 1: SQL Server Setup](#part-1-sql-server-setup)
- [Part 2: Controller on MSSQL](#part-2-controller-on-mssql)
- [Part 3: Gateway on MSSQL](#part-3-gateway-on-mssql)
- [Part 4: EDA on MSSQL](#part-4-eda-on-mssql)
- [Part 5: Service Cluster Data Sync](#part-5-service-cluster-data-sync)
- [Part 6: End-to-End Verification](#part-6-end-to-end-verification)
- [Part 7: PostgreSQL-Free Operation](#part-7-postgresql-free-operation)
- [Shared Library: mssql_common.py](#shared-library-mssql_commonpy)
- [Notification Bus Architecture](#notification-bus-architecture)
- [Monkey-Patch Rationale](#monkey-patch-rationale)
- [Gotchas and Lessons Learned](#gotchas-and-lessons-learned)
- [File Reference](#file-reference)
- [Known Limitations](#known-limitations)

## Architecture

All three AAP components — Controller, Gateway, and EDA — run entirely on SQL Server. PostgreSQL is eliminated.

```
                         ┌──────────────────────────────┐
                         │     AAP Gateway Pod          │
                         │   (nginx + uwsgi + gRPC)     │
                         ├──────────────────────────────┤
                         │  Django ORM    dispatcherd    │
                         │  (all models)  (service_broker)│
                         │       │              │       │
                         │  MSSQLRouter         │       │
                         │       ▼              ▼       │
                         │     mssql ◄──────────┘       │
                         └────────┬─────────────────────┘
                                  │
                         ┌────────▼─────────────────────┐
                         │   SQL Server (host.docker.    │
                         │   internal:1433)              │
                         │  ┌──────────┐ ┌──────────┐   │
                         │  │   awx    │ │aap_gateway│   │
                         │  │(controller)│(gateway)  │   │
                         │  └──────────┘ └──────────┘   │
                         │  ┌──────────┐                │
                         │  │   eda    │                │
                         │  │ (eda)    │                │
                         │  └──────────┘                │
                         └───────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────────────┐
                    │                                   │
           ┌────────┴─────────────────┐  ┌──────────────┴──────────┐
           │  AAP Controller Pods     │  │    EDA Pods              │
           │  (task + web)            │  │  (api + workers +        │
           ├──────────────────────────┤  │   event-stream)          │
           │  Django ORM  dispatcherd │  ├──────────────────────────┤
           │  (all models) + PubSub   │  │  Django ORM  dispatcherd │
           │       │       + wsrelay  │  │  (all models) (svc_broker)│
           │  MSSQLRouter (svc_broker)│  │       │           │     │
           │       ▼          │      │  │  MSSQLRouter       │     │
           │     mssql ◄──────┘      │  │       ▼            ▼     │
           └──────────────────────────┘  │     mssql ◄───────┘     │
                                         └──────────────────────────┘
```

### Notification Bus (replaces pg_notify)

Both components use a **hybrid polling table + Service Broker** signal that replaces PostgreSQL's `LISTEN`/`NOTIFY`:

```
Publisher                          SQL Server
   │                          ┌─────────────────────┐
   ├──INSERT INTO──────────>  │ awx_notify_messages  │  (durable, multi-reader)
   │                          │  id, channel, payload│
   └──SEND ON CONVERSATION──> │ AwxSignalQueue       │  (wake-up signal only)
                              └──────────┬──────────┘
                                         │
                    ┌────────────────────┬┴──────────────┐
                    │                    │               │
              dispatcherd          cache_clear       wsrelay
              (last_seen=N)       (last_seen=M)    (last_seen=P)
              WAITFOR RECEIVE     WAITFOR RECEIVE   WAITFOR RECEIVE
              then poll table     then poll table    then poll table
```

## Prerequisites

- **Kind cluster** with AAP 2.7 deployed (controller + gateway pods running)
- **MS SQL Server** accessible from the cluster (Azure SQL Edge for ARM64/Apple Silicon, or SQL Server 2019+ for x86)
- **kubectl** configured for your cluster
- **Podman or Docker** for building container images
- **Local registry** at `localhost:5001` (Kind default)

## Environment Reference

These values were used in the proven deployment. Adjust as needed.

| Setting | Value |
|---------|-------|
| Kubernetes namespace | `aap27` |
| SQL Server host (from pod) | `host.docker.internal` |
| SQL Server port | `1433` |
| SQL Server SA password | `AAP_P0C_Password_2026!` |
| Controller database | `awx` |
| Gateway database | `aap_gateway` |
| EDA database | `eda` |
| AAP admin password | `r9hwurZIPH1U9FlUWl2SuZMOUnGuBIY6` |
| Controller base image | `localhost:5001/aap27/controller-rhel9:2.7` |
| Gateway base image | `localhost:5001/aap27/gateway-rhel9:2.7` |
| EDA base image | `localhost:5001/aap27/eda-controller-rhel9:2.7` |
| MSSQL image tag | `2.7-mssql` (controller/gateway/EDA) |
| Kind cluster runtime | Podman |
| External gateway port | `44927` |
| Gateway session cookie | `gateway_sessionid44927` |
| Controller Secret | `myaap-controller-app-credentials` |
| Gateway Secret | `myaap-gateway-settings` |

---

## Part 1: SQL Server Setup

### 1.1 Start SQL Server (Azure SQL Edge for Apple Silicon)

```bash
podman run -d --name mssql \
  -e ACCEPT_EULA=Y \
  -e MSSQL_SA_PASSWORD='AAP_P0C_Password_2026!' \
  -p 1433:1433 \
  mcr.microsoft.com/azure-sql-edge:latest
```

### 1.2 Create Databases

```bash
# Controller database
podman exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U SA -P 'AAP_P0C_Password_2026!' \
  -C -Q "CREATE DATABASE awx"

# Gateway database
podman exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U SA -P 'AAP_P0C_Password_2026!' \
  -C -Q "CREATE DATABASE aap_gateway"

# EDA database (or use scripts/setup_eda_db.sql which creates both DB and Service Broker)
podman exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U SA -P 'AAP_P0C_Password_2026!' \
  -C -Q "CREATE DATABASE eda"
```

### 1.3 Set Up Notification Bus

Run the notification bus setup SQL for **all three** databases. The scripts create the polling table (`awx_notify_messages`) and Service Broker objects:

```bash
# Controller (awx database) — scripts/setup_notification_bus.sql
# Gateway (aap_gateway database) — scripts/setup_gateway_db.sql
# EDA (eda database) — scripts/setup_eda_db.sql
```

Each script creates:
- `awx_notify_messages` table: `id BIGINT IDENTITY`, `channel NVARCHAR(200)`, `payload NVARCHAR(MAX)`, `created_at DATETIME2`
- Index `IX_notify_id_channel` on `(id, channel)`
- Service Broker: `AwxSignalMessage` type, `AwxSignalContract`, `AwxSignalQueue`, `AwxSignalService`
- Enables `ENABLE_BROKER` and `TRUSTWORTHY ON` on the database

### Connecting DBeaver

| Target | Host | Port | Database | User | Password |
|--------|------|------|----------|------|----------|
| SQL Server | `localhost` | `1433` | `awx`, `aap_gateway`, or `eda` | `SA` | `AAP_P0C_Password_2026!` |
| PostgreSQL | `localhost` | `5433` (via port-forward) | from Secret | from Secret | from Secret |

```bash
# Port-forward PostgreSQL for comparison queries
kubectl -n aap27 port-forward svc/<postgres-svc> 5433:5432
```

---

## Part 2: Controller on MSSQL

### 2.1 Build the Controller MSSQL Image

```bash
cd contrib/mssql-poc
podman build -f Dockerfile.controller -t localhost:5001/aap27/controller-rhel9:2.7-mssql .
podman push localhost:5001/aap27/controller-rhel9:2.7-mssql
```

`Dockerfile.controller` layers on top of the base controller image:
1. Installs ODBC Driver 18 for SQL Server via Microsoft's RHEL 9 repo
2. Installs `mssql-django==1.7.3` and `pyodbc==5.3.0` into the AWX virtualenv
3. Copies `lib/mssql_common.py` to `/opt/mssql-poc/`
4. Creates the `awx.main.dispatch.brokers` package and copies `service_broker.py` into it

### 2.2 Schema Migration

Run Django migrations against the `mssql` database. Some migrations fail on SQL Server and must be faked:

```bash
TASK_POD=$(kubectl -n aap27 get pods -l app.kubernetes.io/component=automationcontroller \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
  source /var/lib/awx/venv/awx/bin/activate
  awx-manage migrate --database=mssql
"

# Fake migrations that fail on SQL Server syntax
for m in 0069 0144 0185 0187 0189; do
  kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
    source /var/lib/awx/venv/awx/bin/activate
    awx-manage migrate main $m --database=mssql --fake
  "
done

# Re-run until all pass
kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
  source /var/lib/awx/venv/awx/bin/activate
  awx-manage migrate --database=mssql
"
```

### 2.3 Fix Schema Gaps

Faked migrations leave missing columns and tables. Run `scripts/fix_schema_gaps.sql` against the `awx` database. This adds:
- Missing columns: `managed` on `main_instance`, `listener_port` nullable, `version` nullable, `job_created` on event tables
- Missing tables: `main_receptoraddress`, `main_activitystream_receptor_address`
- Unpartitioned views: `_unpartitioned_main_*event` (AWX uses these for event queries)
- Drops `isjson` CHECK constraints auto-generated by mssql-django

### 2.4 Migrate Data from PostgreSQL

```bash
kubectl cp scripts/migrate_pg_to_mssql.py aap27/$TASK_POD:/tmp/ -c controller-task

kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
  source /var/lib/awx/venv/awx/bin/activate
  python /tmp/migrate_pg_to_mssql.py
"
```

### 2.5 Deploy the MSSQL Configuration

The controller configuration is deployed as `mssql.py` in the `myaap-controller-app-credentials` Secret. This file is loaded by AWX's conf.d mechanism (`/etc/tower/conf.d/mssql.py`).

The configuration file (`mssql-confd.py`) contains **both Phase 3 (ORM) and Phase 4 (notification bus)** patches:

**Phase 3 patches (eager — run at settings load):**

| Patch | Purpose |
|-------|---------|
| `DATABASES['mssql']` | SQL Server connection using `mssql-django` backend |
| `MSSQLRouter` + `DATABASE_ROUTERS` | Routes ALL ORM reads/writes to `mssql`; blocks migrations |
| `SET XACT_ABORT OFF` | Allows Django savepoints to work correctly |
| `_MSSQLAtomicWrapper` | `transaction.atomic()` defaults to `mssql` |
| `_mssql_on_commit` | `on_commit` callbacks default to `mssql` |
| `_mssql_advisory_lock_noop` | No-op for PostgreSQL advisory locks |
| `_mssql_bulk_create` | Row-by-row fallback for `bulk_create(ignore_conflicts=True)` |
| `_mssql_patched_update_host_metrics` | Raw SQL upsert for HostMetric (deferred via `connection_created`) |

**Phase 4 patches (notification bus):**

| Patch | When | Purpose |
|-------|------|---------|
| **pg_notify stub with Broker class** | Eager (settings load) | Stubs `dispatcherd.brokers.pg_notify` in `sys.modules` with a `Broker` class that delegates to `service_broker.Broker` |
| **get_dispatcherd_config** | Deferred (`connection_created`) | Swaps `pg_notify` broker for `service_broker` in dispatcherd config |
| **configure_dispatcherd** | Deferred (`connection_created`) | Bypasses `connection.vendor != 'postgresql'` check |
| **PubSub/pg_bus_conn** | Deferred (`connection_created`) | Replaces AWX's `PubSub`, `pg_bus_conn`, `create_listener_connection` |
| **wsrelay** | Deferred (`connection_created`) | Patches `WebSocketRelayManager.run()` to use SQL Server |

#### Critical: pg_notify Stub Must Include a Broker Class

The stub for `dispatcherd.brokers.pg_notify` must include a `Broker` class, not just be an empty module. This is because dispatcherd resolves the broker class from config **before** the deferred `connection_created` patch fires. The stub's Broker class extends `service_broker.Broker` and overrides `__init__` to inject the MSSQL config:

```python
from awx.main.dispatch.brokers.service_broker import Broker as _SBBroker

class _PgNotifyStubBroker(_SBBroker):
    def __init__(self, **kwargs):
        kwargs['config'] = _SB_BROKER_CONFIG
        super().__init__(**kwargs)

_stub.Broker = _PgNotifyStubBroker
```

The `kwargs['config'] = ...` pattern is required because `service_broker.Broker.__init__` expects a `config` dict parameter, not individual connection params. Passing `super().__init__(**_SB_BROKER_CONFIG)` would fail with `RuntimeError: Must specify config with SQL Server connection parameters`.

#### Deploy the Secret

```bash
# Encode and patch the secret
MSSQL_B64=$(base64 < mssql-confd.py)
kubectl patch secret myaap-controller-app-credentials -n aap27 \
  --type merge -p "{\"data\":{\"mssql.py\":\"${MSSQL_B64}\"}}"
```

### 2.6 Deploy the MSSQL Controller Image

```bash
# Update both controller deployments to use the MSSQL image
kubectl set image deployment/myaap-controller-task \
  controller-task=localhost:5001/aap27/controller-rhel9:2.7-mssql \
  -n aap27

kubectl set image deployment/myaap-controller-web \
  controller-web=localhost:5001/aap27/controller-rhel9:2.7-mssql \
  -n aap27
```

Wait for rollout:
```bash
kubectl rollout status deployment/myaap-controller-task -n aap27 --timeout=120s
kubectl rollout status deployment/myaap-controller-web -n aap27 --timeout=120s
```

### 2.7 Register Execution Environments

```bash
kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
  source /var/lib/awx/venv/awx/bin/activate
  awx-manage register_default_execution_environments
"
```

### 2.8 Verify Controller

```bash
# Check ORM routing
kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
  source /var/lib/awx/venv/awx/bin/activate
  awx-manage shell -c \"
from django.conf import settings
print('Routers:', settings.DATABASE_ROUTERS)
from awx.main.models import Organization
print('Org count (from MSSQL):', Organization.objects.count())
\"
"

# Check notification bus
kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
  source /var/lib/awx/venv/awx/bin/activate
  awx-manage shell -c \"
from awx.main.dispatch.config import get_dispatcherd_config
config = get_dispatcherd_config()
print('Brokers:', list(config.get('brokers', {}).keys()))
print('Default broker:', config.get('publish', {}).get('default_broker'))
\"
"

# Run a job
kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
  source /var/lib/awx/venv/awx/bin/activate
  awx-manage shell -c \"
from awx.main.models import JobTemplate
jt = JobTemplate.objects.get(name='Demo Job Template')
job = jt.create_unified_job()
job.signal_start()
print(f'Job {job.id} launched')
\"
"

# After ~30 seconds, verify
kubectl exec -n aap27 $TASK_POD -c controller-task -- bash -c "
  source /var/lib/awx/venv/awx/bin/activate
  awx-manage shell -c \"
from awx.main.models import Job, JobEvent, HostMetric
job = Job.objects.latest('id')
print(f'Job {job.id}: status={job.status} failed={job.failed}')
print(f'Events: {JobEvent.objects.filter(job_id=job.id).count()}')
\"
"
```

Expected output:
```
Job <id>: status=successful failed=False
Events: 9
```

---

## Part 3: Gateway on MSSQL

### 3.1 Build the Gateway MSSQL Image

```bash
cd contrib/mssql-poc
podman build -f Dockerfile.gateway -t localhost:5001/aap27/gateway-rhel9:2.7-mssql .
podman push localhost:5001/aap27/gateway-rhel9:2.7-mssql
```

`Dockerfile.gateway` is structurally identical to the controller Dockerfile but targets the gateway's paths:
- Pip install into `/opt/aap-gateway/venv/bin/pip`
- Service broker module at `/app/gateway/aap_gateway_api/dispatch/brokers/service_broker.py`

### 3.2 Schema Migration

```bash
GATEWAY_POD=$(kubectl -n aap27 get pods -l app.kubernetes.io/name=gateway \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n aap27 $GATEWAY_POD -c api -- \
  aap-gateway-manage migrate --database mssql --no-input
```

### 3.3 Fix IDENTITY Columns

**Critical step.** Django's `mssql-django` backend does not create IDENTITY properties on `id` columns during `migrate`. This means Django's `model.save()` fails when it tries to insert a row without specifying an ID (SQL Server doesn't auto-generate one).

Run `scripts/fix_gateway_identity_columns.py` inside the gateway pod:

```bash
kubectl cp scripts/fix_gateway_identity_columns.py \
  aap27/$GATEWAY_POD:/tmp/ -c api

kubectl exec -n aap27 $GATEWAY_POD -c api -- \
  python /tmp/fix_gateway_identity_columns.py
```

This script:
1. Finds all tables where `id` column lacks the IDENTITY property
2. For each table: drops FKs, creates a new table with IDENTITY, copies data with `IDENTITY_INSERT ON`, drops the old table, renames, and recreates all constraints/indexes
3. Reseeds IDENTITY counters

### 3.4 Fix Schema Gaps

Run `scripts/fix_gateway_schema_gaps.sql` against the `aap_gateway` database. This:
- Drops `isjson` CHECK constraints (auto-generated by mssql-django on JSONField columns, they block valid JSON operations)
- Reseeds IDENTITY counters on all tables after data migration

### 3.5 Migrate Data from PostgreSQL

```bash
kubectl cp scripts/migrate_gateway_data.py \
  aap27/$GATEWAY_POD:/tmp/ -c api

kubectl exec -n aap27 $GATEWAY_POD -c api -- \
  python /tmp/migrate_gateway_data.py
```

The script migrates all gateway tables in dependency order:
- Django framework tables (content types, permissions, sessions)
- Gateway users, orgs, teams
- Service configuration (service types, clusters, nodes, keys)
- Routing (routes, API routes, additional routes)
- DAB tables (authentication, RBAC, resource registry, OAuth2, feature flags)

### 3.6 Deploy the Gateway MSSQL Settings

The gateway settings file is injected by appending to the existing `settings.py` in the `myaap-gateway-settings` Secret. The file `gateway-mssql-settings.py` contains both Phase 3 and Phase 4 patches.

**Phase 3 patches** use the shared library (`mssql_common.py`):
```python
from mssql_common import apply_orm_patches, get_sb_broker_config, stub_pg_notify
DATABASE_ROUTERS = apply_orm_patches(DATABASES, db_name='aap_gateway', include_healthcheck=True)
```

**Phase 4 patches** are gateway-specific:

| Patch | Purpose |
|-------|---------|
| `stub_pg_notify()` | Stubs `dispatcherd.brokers.pg_notify` in `sys.modules` |
| `_mssql_gw_ready()` | Replacement `AppConfig.ready()` that configures dispatcherd for Service Broker |
| `get_dispatcherd_config` | Returns config pointing at `aap_gateway_api.dispatch.brokers.service_broker` |
| `PingView._check_db` | Patches healthcheck to query `mssql` alias instead of `default` |
| `XDSView.get_qs` | Patches Envoy xDS endpoint to replace `DISTINCT ON` with MSSQL-compatible query |

#### Gateway-Specific: xDS DISTINCT ON Patch

The Envoy REST xDS control plane endpoints (`/v3/discovery:clusters`, `/v3/discovery:listeners`) use `DISTINCT ON` which is PostgreSQL-only. The patch replaces it with `Min('id')` + `GROUP BY`:

```python
def _mssql_get_qs(self, request, ModelClass, name_field):
    from django.db.models import Min
    ids = (ModelClass.objects
           .values(name_field)
           .annotate(_first_id=Min('id'))
           .values_list('_first_id', flat=True))
    qs = ModelClass.objects.filter(id__in=ids)
    # ... name filtering ...
    return qs
```

#### Gateway-Specific: SKIP_MSSQL Environment Variable

The gateway settings include a `SKIP_MSSQL` check so that init containers (which run the same image but don't need MSSQL) can skip the MSSQL configuration:

```python
_SKIP_MSSQL = os.environ.get('SKIP_MSSQL')
if not _SKIP_MSSQL:
    # ... all MSSQL patches ...
```

#### Deploy the Secret

```bash
# Get current settings
kubectl get secret myaap-gateway-settings -n aap27 \
  -o jsonpath='{.data.settings\.py}' | base64 -d > /tmp/gw-settings.py

# Append MSSQL settings
cat gateway-mssql-settings.py >> /tmp/gw-settings.py

# Update secret
GW_B64=$(base64 < /tmp/gw-settings.py)
kubectl patch secret myaap-gateway-settings -n aap27 \
  --type merge -p "{\"data\":{\"settings.py\":\"${GW_B64}\"}}"
```

### 3.7 Deploy the Gateway MSSQL Image

```bash
kubectl set image deployment/myaap-gateway \
  api=localhost:5001/aap27/gateway-rhel9:2.7-mssql \
  -n aap27

kubectl rollout status deployment/myaap-gateway -n aap27 --timeout=120s
```

### 3.8 Verify Gateway

```bash
# Ping endpoint
curl -s http://localhost:44927/api/gateway/v1/ping/ | python3 -m json.tool

# Login
# Step 1: Get CSRF token
CSRF=$(curl -s -c - http://localhost:44927/api/gateway/v1/login/ | grep csrftoken | awk '{print $7}')

# Step 2: Login (form-encoded, NOT JSON)
curl -s -X POST http://localhost:44927/api/gateway/v1/login/ \
  -H "X-CSRFToken: $CSRF" \
  -H "Cookie: csrftoken=$CSRF" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=r9hwurZIPH1U9FlUWl2SuZMOUnGuBIY6"
```

---

## Part 4: EDA on MSSQL

### 4.1 Build the EDA MSSQL Image

```bash
cd contrib/mssql-poc
podman build -f Dockerfile.eda -t localhost:5001/aap27/eda-controller-rhel9:2.7-mssql .
podman push localhost:5001/aap27/eda-controller-rhel9:2.7-mssql
```

`Dockerfile.eda` is structurally identical to the controller/gateway Dockerfiles but targets EDA's paths:
1. Installs ODBC Driver 18 via Microsoft's RHEL 9 repo
2. Installs `mssql-django==1.7.3`, `pyodbc==5.3.0`, and `pytz` into system Python (`/usr/bin/pip3`)
3. Copies `lib/mssql_common.py`, `service_broker.py`, and `eda-mssql-settings.py` to `/opt/mssql-poc/`

Key difference from controller/gateway: EDA uses system Python 3.12 (no virtualenv), and all MSSQL files go to `/opt/mssql-poc/` on `PYTHONPATH` rather than into a component-specific dispatch/brokers directory.

### 4.2 Settings Injection (Dynaconf)

EDA uses **Dynaconf** for settings, not Django's conf.d mechanism (controller) or appended settings.py (gateway). The injection approach:

1. `eda-mssql-settings.py` is a wrapper settings module that:
   - Does `from aap_eda.settings.default import *` (triggers full Dynaconf chain)
   - Overrides DATABASES via `mssql_common.apply_orm_patches()`
   - Stubs `dispatcherd.brokers.pg_notify` and registers the Service Broker
   - Patches `CoreConfig.ready()` to configure dispatcherd with Service Broker
   - Patches the `dispatcherd` management command to use Service Broker config
   - Patches `list_requests()` to replace `DISTINCT ON` with `Min()` + `GROUP BY`

2. Deploy via environment variables on all 4 EDA deployments:
   - `DJANGO_SETTINGS_MODULE=eda_mssql_settings`
   - `PYTHONPATH=/opt/mssql-poc`

This modifies **zero** EDA source files.

### 4.3 EDA-Specific Patches

| Patch | Purpose |
|-------|---------|
| `apply_orm_patches(DATABASES, db_name='eda')` | ORM routing, UUID fix, advisory lock noop, transaction patches (shared library) |
| `_PgNotifyStubBroker` | pg_notify stub with Broker class that delegates to Service Broker |
| `DISPATCHERD_DEFAULT_SETTINGS` | Override dispatcherd config to use `service_broker` instead of `pg_notify` |
| `DISPATCHERD_DEFAULT_WORKER_SETTINGS` | DefaultWorker-specific config with scheduled producers |
| `_mssql_eda_ready()` | Replacement `CoreConfig.ready()` that configures dispatcherd, bypasses health check, and patches DISTINCT ON |
| `check_dispatcherd_workers_health` bypass | Stubs health check to always return `True` — `control_with_reply("alive")` is incompatible with Service Broker. Patches both `aap_eda.core.health` and `aap_eda.core.views` modules (from-import binding) |
| `_mssql_dispatcherd_handle()` | Replacement dispatcherd management command handler for ActivationWorker/DefaultWorker |
| `_mssql_list_requests()` | Replaces `DISTINCT ON` (PG-only) in `activation_request_queue.list_requests()` |
| `install_textfield_index_patch()` | Caps TextField to `nvarchar(450)` for MSSQL index compatibility |

#### DISTINCT ON Patch (EDA-Specific)

EDA's `activation_request_queue.list_requests()` uses `.distinct("process_parent_type", "process_parent_id")` which is PostgreSQL-only. The patch replaces it with `Min('id')` + `GROUP BY`:

```python
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
```

This patch is deferred into `CoreConfig.ready()` because importing `aap_eda.tasks.activation_request_queue` at settings-load time triggers an import cascade (`tasks.__init__` → `project.py` → `core.models` → `ansible_base.rbac` → settings access) that fails with `AttributeError: 'Settings' object has no attribute 'ANSIBLE_BASE_ORGANIZATION_MODEL'`.

### 4.4 Schema Migration

Run Django migrations from a pod or a helper script with `post_migrate` signals disabled (EDA's `post_migrate` handlers try to query tables before they exist):

```python
# run_eda_migrations.py — run inside the pod or locally with PYTHONPATH set
import os, sys
sys.path.insert(0, '/opt/mssql-poc')
os.environ['DJANGO_SETTINGS_MODULE'] = 'eda_mssql_settings'

import django
django.setup()

from django.db.models.signals import post_migrate
receivers_backup = list(post_migrate.receivers)
post_migrate.receivers = []

from django.core.management import call_command
call_command('migrate', database='mssql', verbosity=2)

post_migrate.receivers = receivers_backup
```

Run inside the EDA pod:
```bash
EDA_POD=$(kubectl -n aap27 get pods -l app.kubernetes.io/name=eda-api \
  -o jsonpath='{.items[0].metadata.name}')

kubectl cp run_eda_migrations.py aap27/$EDA_POD:/tmp/ -c eda-api

kubectl exec -n aap27 $EDA_POD -c eda-api -- bash -c "
  PYTHONPATH=/opt/mssql-poc DJANGO_SETTINGS_MODULE=eda_mssql_settings \
  python3 /tmp/run_eda_migrations.py
"
```

### 4.5 Post-Migration Fixes

#### Drop isjson CHECK Constraints

```sql
-- Connect to 'eda' database and drop all isjson constraints
DECLARE @sql NVARCHAR(MAX) = '';
SELECT @sql += 'ALTER TABLE [' + OBJECT_NAME(parent_object_id)
    + '] DROP CONSTRAINT [' + name + '];' + CHAR(13)
FROM sys.check_constraints
WHERE definition LIKE '%isjson%';
EXEC sp_executesql @sql;
```

#### Fix IDENTITY Columns

Run `scripts/fix_eda_identity_columns.py` against the EDA database. This follows the same pattern as the gateway fix — rebuilds tables to add IDENTITY on `id` columns.

**Important:** Tables with UUID primary keys (`core_audit_action`, `core_audit_event`) use `uniqueidentifier` and cannot have IDENTITY. The script correctly skips these tables.

```bash
python scripts/fix_eda_identity_columns.py
```

### 4.6 Migrate Data from PostgreSQL

```bash
kubectl cp scripts/migrate_eda_data.py aap27/$EDA_POD:/tmp/ -c eda-api

kubectl exec -n aap27 $EDA_POD -c eda-api -- bash -c "
  PYTHONPATH=/opt/mssql-poc DJANGO_SETTINGS_MODULE=eda_mssql_settings \
  python3 /tmp/migrate_eda_data.py
"
```

The script migrates all EDA tables in dependency order:
- Django framework tables (content types, permissions, sessions)
- EDA users, organizations, teams
- Credentials and decision environments
- Projects and rulebooks
- Activations and event streams
- Jobs and instances
- Rulebook processes
- Audit tables (UUID primary keys — no IDENTITY_INSERT needed)
- Settings and feature flags
- DAB tables (RBAC, resource registry)

### 4.7 Deploy the EDA MSSQL Image

**IMPORTANT:** Only modify EDA deployments. Do NOT touch controller or gateway.

```bash
# Set image on all 4 EDA deployments
for dep in myaap-eda-api myaap-eda-activation-worker myaap-eda-default-worker myaap-eda-event-stream; do
  kubectl set image deployment/$dep -n aap27 --all \
    localhost:5001/aap27/eda-controller-rhel9:2.7-mssql
done

# Set env vars on all EDA deployments
for dep in myaap-eda-api myaap-eda-activation-worker myaap-eda-default-worker myaap-eda-event-stream; do
  kubectl set env deployment/$dep -n aap27 --containers='*' \
    DJANGO_SETTINGS_MODULE=eda_mssql_settings \
    PYTHONPATH=/opt/mssql-poc
done
```

Wait for rollout:
```bash
for dep in myaap-eda-api myaap-eda-activation-worker myaap-eda-default-worker myaap-eda-event-stream; do
  kubectl rollout status deployment/$dep -n aap27 --timeout=120s
done
```

#### Image Cache Busting

Kubernetes nodes cache images by tag. If you rebuild with the same tag, pods may use the stale cached image. Use incrementing tags (e.g. `2.7-mssql-v2`, `2.7-mssql-v3`, ...) to force pulls:

```bash
podman build -f Dockerfile.eda -t localhost:5001/aap27/eda-controller-rhel9:2.7-mssql-v2 .
podman push localhost:5001/aap27/eda-controller-rhel9:2.7-mssql-v2
```

#### Fix Nginx Container Environment

EDA's `eda-api` and `eda-event-stream` deployments contain **nginx containers** whose entrypoints import Django settings (requiring `SECRET_KEY`). These containers need environment variables even though they run nginx:

```bash
# Add envFrom to nginx containers in eda-api deployment
kubectl patch deployment myaap-eda-api -n aap27 --type json -p '[
  {"op": "add", "path": "/spec/template/spec/containers/1/envFrom",
   "value": [{"configMapRef": {"name": "myaap-eda-eda-env-properties"}}]},
  {"op": "add", "path": "/spec/template/spec/containers/1/env/-",
   "value": {"name": "EDA_SECRET_KEY", "valueFrom":
     {"secretKeyRef": {"name": "myaap-eda-server", "key": "secret_key"}}}}
]'

# Same for eda-event-stream nginx container
kubectl patch deployment myaap-eda-event-stream -n aap27 --type json -p '[
  {"op": "add", "path": "/spec/template/spec/containers/1/envFrom",
   "value": [{"configMapRef": {"name": "myaap-eda-eda-env-properties"}}]},
  {"op": "add", "path": "/spec/template/spec/containers/1/env/-",
   "value": {"name": "EDA_SECRET_KEY", "valueFrom":
     {"secretKeyRef": {"name": "myaap-eda-server", "key": "secret_key"}}}}
]'
```

#### Fix init-container Crash

The `eda-initial-data` init container may fail with `Organization.DoesNotExist` (pre-existing PG issue). Make it tolerate failures:

```bash
kubectl patch deployment myaap-eda-api -n aap27 --type json -p '[
  {"op": "replace",
   "path": "/spec/template/spec/initContainers/0/args",
   "value": ["-c", "aap-eda-manage create_initial_data || echo WARN: create_initial_data failed, continuing"]}
]'
```

### 4.8 Fix EDA Health Check (Envoy Routing)

After deploying EDA on MSSQL, the gateway may return **503 Service Unavailable** for all EDA requests (e.g. clicking "Projects" in Automation Decisions). This is caused by Envoy health checks failing on the EDA status endpoint.

**Root Cause Chain:**

1. Gateway's Envoy proxy health-checks EDA at `GET /api/eda/v1/status/` on port 8000
2. EDA's `StatusView.get()` calls `check_dispatcherd_workers_health()`
3. This function uses `control_with_reply("alive")` — a bidirectional control message through the broker
4. Service Broker doesn't support `control_with_reply` — it returns an empty list
5. EDA returns `{"status":"failed","message":"Dispatcherd workers unavailable"}` (HTTP 500)
6. Envoy marks EDA as unhealthy → **all** EDA requests get 503

**Fix (two parts):**

**Part A — Bypass in `_mssql_eda_ready()`** (already in `eda-mssql-settings.py`):

The `_mssql_eda_ready()` function patches `check_dispatcherd_workers_health` to always return `True`. The patch must happen **after** `dab_decorate` is imported (which triggers views loading), and must patch **both** modules due to Python's `from module import name` binding semantics:

```python
import aap_eda.core.health as _health
import aap_eda.core.views as _views
_bypass = lambda raise_exceptions=False: True
_health.check_dispatcherd_workers_health = _bypass
_views.check_dispatcherd_workers_health = _bypass
```

**Part B — Update nginx containers to use MSSQL image:**

EDA's `eda-api` and `eda-event-stream` deployments each have a container named "nginx" that actually runs **gunicorn** on port 8000 (not actual nginx). This is the container Envoy health-checks. It needs the MSSQL image + environment variables:

```bash
# Update nginx container image in eda-api and eda-event-stream
kubectl set image deployment/myaap-eda-api -n aap27 \
  nginx=localhost:5001/aap27/eda-controller-rhel9:2.7-mssql

kubectl set image deployment/myaap-eda-event-stream -n aap27 \
  nginx=localhost:5001/aap27/eda-controller-rhel9:2.7-mssql

# Set MSSQL env vars on the nginx containers
for dep in myaap-eda-api myaap-eda-event-stream; do
  kubectl set env deployment/$dep -n aap27 -c nginx \
    DJANGO_SETTINGS_MODULE=eda_mssql_settings \
    PYTHONPATH=/opt/mssql-poc
done
```

**Verify:** Both ports should now return healthy status:
```bash
# Port 8000 (nginx/gunicorn — what Envoy checks)
kubectl exec -n aap27 $EDA_POD -c nginx -- curl -s http://localhost:8000/api/eda/v1/status/
# → {"status":"good"}

# Port 8002 (eda-api gunicorn)
kubectl exec -n aap27 $EDA_POD -c eda-api -- curl -s http://localhost:8002/api/eda/v1/status/
# → {"status":"good"}
```

### 4.9 Post-Migration Data Integrity Fixes

After data migration, several RBAC/resource registry tables may have missing or mismatched rows. These issues stem from two root causes:

1. **Disabled `post_migrate` signals** — we disabled these during schema migration (Step 4.4) to prevent premature table access. Some DAB RBAC data is populated by `post_migrate` handlers, not by the migration files themselves.
2. **Content type ID mismatch** — `django_content_type` IDs are auto-generated and differ between PG and MSSQL. Tables that reference content types by ID (e.g. `dab_resource_registry_resource`) retain PG's IDs after data copy, which don't match MSSQL's IDs.

#### 4.9.1 Fix Missing DAB Content Types and Permissions

The `dab_rbac_dabcontenttype` table may be missing entries that were created by `post_migrate` signals. In practice, `core.auditrule` is the most common missing entry.

**Diagnose:**
```bash
kubectl exec -n aap27 $EDA_POD -c eda-api -- python3 -c "
import os, django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'eda_mssql_settings')
django.setup()
from ansible_base.rbac.models import DABContentType, DABPermission
pg_ct = set(DABContentType.objects.using('default').values_list('app_label','model',flat=False))
ms_ct = set(DABContentType.objects.using('mssql').values_list('app_label','model',flat=False))
missing = pg_ct - ms_ct
print(f'Missing DAB content types: {missing or \"none\"}')
print(f'DAB permissions: PG={DABPermission.objects.using(\"default\").count()}, MSSQL={DABPermission.objects.using(\"mssql\").count()}')
"
```

**Fix:** Copy missing rows from PG to MSSQL with `IDENTITY_INSERT ON`. Must include all non-nullable columns (`id`, `service`, `app_label`, `model`, `api_slug`, `pk_field_type` for content types; `id`, `name`, `codename`, `content_type_id`, `api_slug` for permissions). Also copy any missing rows in the `dab_rbac_roledefinition_permissions` join table that reference the missing permission.

#### 4.9.2 Fix Content Type ID Mismatch in Resource Registry

**This affects Gateway specifically.** The `dab_resource_registry_resource` table has a `content_type_id` foreign key. When data is copied from PG, these IDs reference PG's `django_content_type` rows. But MSSQL's `django_content_type` table has different auto-generated IDs for the same `(app_label, model)` pairs.

**Symptom:** `Resource.DoesNotExist` errors when creating users, assigning roles, or logging in. The error appears in Gateway logs as:
```
ansible_base.resource_registry.models.resource.Resource.DoesNotExist: Resource matching query does not exist.
```

**Diagnose:**
```bash
kubectl exec -n aap27 $GATEWAY_POD -c api -- python3 -c "
import os, sys, django
sys.path.insert(0, '/opt/mssql-poc')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gateway_mssql_settings')
django.setup()
from django.contrib.contenttypes.models import ContentType
from ansible_base.resource_registry.models import Resource
pg_map = {ct.id: (ct.app_label, ct.model) for ct in ContentType.objects.using('default').all()}
mssql_map = {(ct.app_label, ct.model): ct.id for ct in ContentType.objects.using('mssql').all()}
bad = 0
for r in Resource.objects.using('mssql').all():
    key = pg_map.get(r.content_type_id)
    if key:
        correct = mssql_map.get(key)
        if correct and correct != r.content_type_id:
            bad += 1
            print(f'Resource id={r.id}: ct_id {r.content_type_id} -> should be {correct} ({key[0]}.{key[1]})')
print(f'Total needing fix: {bad}')
"
```

**Fix:** Update each mismatched Resource row's `content_type_id` to the correct MSSQL value:
```python
r.content_type_id = correct_mssql_id
r.save(using='mssql', update_fields=['content_type_id'])
```

#### 4.9.3 Remove Duplicate Resource Entries

After fixing content type IDs, check for duplicate Resource entries. Duplicates occur when:
1. The original PG row was copied with the wrong `content_type_id` (so the system couldn't find it)
2. The application auto-created a new Resource row (with the correct `content_type_id`)
3. We then fixed the original row's `content_type_id` — now both rows have the correct ID

**Symptom:** `Resource.MultipleObjectsReturned: get() returned more than one Resource` on login or role assignment.

**Diagnose:**
```bash
kubectl exec -n aap27 $GATEWAY_POD -c api -- python3 -c "
import os, sys, django
sys.path.insert(0, '/opt/mssql-poc')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gateway_mssql_settings')
django.setup()
from ansible_base.resource_registry.models import Resource
from django.db.models import Count
dupes = (Resource.objects.using('mssql')
    .values('content_type_id', 'object_id')
    .annotate(cnt=Count('id')).filter(cnt__gt=1))
for d in dupes:
    print(f'Duplicate: ct_id={d[\"content_type_id\"]}, object_id={d[\"object_id\"]}, count={d[\"cnt\"]}')
    for r in Resource.objects.using('mssql').filter(
        content_type_id=d['content_type_id'], object_id=d['object_id']).order_by('id'):
        print(f'  id={r.id}, ansible_id={r.ansible_id}')
"
```

**Fix:** Keep the row whose `ansible_id` matches PG (preserves cross-service identity), delete the auto-created duplicate:
```python
# Compare with PG to find the canonical ansible_id
pg_resource = Resource.objects.using('default').get(content_type=..., object_id=...)
# Delete the MSSQL row that doesn't match
Resource.objects.using('mssql').filter(...).exclude(ansible_id=pg_resource.ansible_id).delete()
```

### 4.10 Verify EDA

```bash
# Check all 4 EDA deployments are running
kubectl get pods -n aap27 | grep eda

# Verify MSSQL routing
kubectl exec -n aap27 $EDA_POD -c eda-api -- bash -c "
  PYTHONPATH=/opt/mssql-poc DJANGO_SETTINGS_MODULE=eda_mssql_settings \
  python3 -c \"
import django; import os
os.environ['DJANGO_SETTINGS_MODULE']='eda_mssql_settings'
django.setup()
from django.conf import settings
print('MSSQL engine:', settings.DATABASES.get('mssql', {}).get('ENGINE'))
print('Router:', settings.DATABASE_ROUTERS)
from aap_eda.core.models import Organization
print('Org count (from MSSQL):', Organization.objects.count())
\"
"

# Verify controller and gateway images are UNCHANGED
kubectl get deploy myaap-controller-task myaap-controller-web myaap-gateway -n aap27 \
  -o jsonpath='{range .items[*]}{.metadata.name}: {.spec.template.spec.containers[0].image}{"\n"}{end}'
```

Expected: EDA pods all Running; controller/gateway images remain at their `2.7-mssql` tags (not changed to EDA image).

---

## Part 5: Service Cluster Data Sync (Restoring Envoy Routing)

### Why This Step Exists — The Failure Sequence

When controller is deployed on MSSQL first (Part 2), everything works: controller serves its own API, and gateway routes traffic to controller through Envoy using service cluster data in PostgreSQL.

When you then move **gateway** to MSSQL (Part 3), the `MSSQLRouter` redirects **all** gateway ORM queries — including the Envoy xDS control plane — to SQL Server. The xDS endpoints (`/v3/discovery:clusters`, `/v3/discovery:listeners`) now query the `aap_gateway` MSSQL database for service cluster data. But that data was only ever in PostgreSQL, so the MSSQL tables are empty. This causes:

1. **`RelatedObjectDoesNotExist`** errors in xDS views — the xDS queries hit empty MSSQL tables and fail on FK lookups
2. **Envoy returns empty cluster/listener config** — no upstream clusters defined, so Envoy can't route to controller, hub, or EDA
3. **Controller disappears from the gateway UI** — the platform UI shows no services because Envoy has no routes
4. **`/api/controller/` returns 503** — Envoy has no cluster definition for the controller upstream

The fix is to ensure the service cluster data exists in the MSSQL `aap_gateway` database before (or immediately after) switching gateway to MSSQL.

### Compounding Issue: Controller Image Revert

During gateway deployment, if controller pods bounce (e.g. from a rollout restart or resource pressure), the controller deployment may revert to the **base image** (`controller-rhel9:2.7`) instead of the MSSQL image (`controller-rhel9:2.7-mssql`). This happens if the deployment spec wasn't updated with `kubectl set image` (only the running pod was using the MSSQL image).

Symptoms when controller runs the wrong image:
- `ModuleNotFoundError: No module named 'awx.main.dispatch.brokers'` — broker module isn't in the base image
- `AttributeError: module 'dispatcherd.brokers.pg_notify' has no attribute 'Broker'` — pg_notify stub is empty because `service_broker.Broker` can't be imported
- Dispatcher crash-loops, jobs fail to launch

**Always verify** both deployment image specs match the MSSQL tag:
```bash
# Check controller deployments
kubectl get deploy myaap-controller-task myaap-controller-web -n aap27 \
  -o jsonpath='{range .items[*]}{.metadata.name}: {.spec.template.spec.containers[*].image}{"\n"}{end}'

# Should show 2.7-mssql, not 2.7
# Fix if wrong:
kubectl set image deployment/myaap-controller-task \
  controller-task=localhost:5001/aap27/controller-rhel9:2.7-mssql -n aap27
kubectl set image deployment/myaap-controller-web \
  controller-web=localhost:5001/aap27/controller-rhel9:2.7-mssql -n aap27
```

### Service Cluster Data

After gateway is on MSSQL, the Envoy xDS endpoints must return valid cluster and listener config. This requires the service cluster data (which services exist and how to route to them) to be present in MSSQL.

### 5.1 Understanding Service Clusters

The gateway maintains 4 service clusters that define how Envoy routes traffic:

| ID | Name | Type | service_type_id |
|----|------|------|-----------------|
| 1 | Gateway | 1 (gateway) | 1 |
| 2 | Controller | 2 (controller) | 2 |
| 3 | Hub | 3 (hub) | 3 |
| 4 | EDA | 4 (eda) | 4 |

Related tables (in FK dependency order):
- `aap_gateway_api_servicetype` — types (gateway, controller, hub, eda)
- `aap_gateway_api_servicecluster` — cluster definitions
- `aap_gateway_api_httpport` — port bindings
- `aap_gateway_api_servicenode` — node addresses
- `aap_gateway_api_servicekey` — service authentication keys
- `aap_gateway_api_route` — URL routing rules
- `aap_gateway_api_serviceapiroute` — API route definitions
- `aap_gateway_api_additionalroute` — additional routes

### 5.2 Delete Order (FK constraints)

If you need to re-sync data, delete in this order to avoid FK constraint violations:

```sql
DELETE FROM aap_gateway_api_serviceapiroute;
DELETE FROM aap_gateway_api_additionalroute;
DELETE FROM aap_gateway_api_route;
DELETE FROM aap_gateway_api_servicekey;
DELETE FROM aap_gateway_api_servicenode;
DELETE FROM aap_gateway_api_httpport;
DELETE FROM aap_gateway_api_servicecluster;
```

### 5.3 UNIQUE Constraint on service_type Column

**Gotcha:** MSSQL may have an extra `service_type` column (nvarchar) on `aap_gateway_api_servicecluster` that doesn't exist in PostgreSQL, alongside the correct `service_type_id` (bigint FK). This column has a UNIQUE constraint that only allows one NULL value — blocking insertion of multiple clusters.

Check and fix:

```sql
-- Check if the extra column exists
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'aap_gateway_api_servicecluster'
ORDER BY ORDINAL_POSITION;

-- Drop the UNIQUE constraint if it exists
-- Find the constraint name first:
SELECT name FROM sys.key_constraints
WHERE parent_object_id = OBJECT_ID('aap_gateway_api_servicecluster')
  AND type = 'UQ';

-- Then drop it:
ALTER TABLE aap_gateway_api_servicecluster
  DROP CONSTRAINT UQ__aap_gate__<suffix>;
```

### 5.4 IDENTITY_INSERT for Service Cluster Data

When inserting rows with explicit ID values (to match PostgreSQL IDs), you must enable `IDENTITY_INSERT`:

```sql
SET IDENTITY_INSERT aap_gateway_api_servicecluster ON;
INSERT INTO aap_gateway_api_servicecluster (id, name, ..., service_type_id)
VALUES (1, 'Gateway', ..., 1);
-- ... more inserts ...
SET IDENTITY_INSERT aap_gateway_api_servicecluster OFF;
```

### 5.5 Verify xDS Endpoints

After data sync, verify Envoy can get valid config:

```bash
# From inside the gateway pod:
kubectl exec -n aap27 $GATEWAY_POD -c api -- \
  curl -s -X POST http://localhost:8080/v3/discovery:clusters \
    -H 'Content-Type: application/json' \
    -d '{"resource_names":["*"]}' | python3 -m json.tool

kubectl exec -n aap27 $GATEWAY_POD -c api -- \
  curl -s -X POST http://localhost:8080/v3/discovery:listeners \
    -H 'Content-Type: application/json' \
    -d '{"resource_names":["*"]}' | python3 -m json.tool
```

Expected: JSON response with `resources` array containing cluster/listener definitions for all 4 services.

### 5.6 Verify Controller Routing Through Gateway

```bash
# Controller API through Envoy
curl -s http://localhost:44927/api/controller/v2/ping/ | python3 -m json.tool
```

---

## Part 6: End-to-End Verification

### 6.1 Full Stack Health Check

```bash
# Gateway ping
curl -s http://localhost:44927/api/gateway/v1/ping/ | python3 -m json.tool

# Controller ping (through gateway)
curl -s http://localhost:44927/api/controller/v2/ping/ | python3 -m json.tool

# Login and run a job
# (see Part 2.8 for job execution commands)
```

### 6.2 Expected Results

- Gateway ping: `db_connected: true`
- Controller ping: `200 OK`
- EDA pods: all 4 deployments Running (api 3/3, activation-worker 1/1, default-worker 1/1, event-stream 2/2)
- Job execution: `status=successful`, `failed=False`, 9 events
- UI accessible at `http://localhost:44927`
- Controller visible in gateway UI under Resources
- Automation Decisions visible in gateway UI
- MSSQL databases: `awx` (158 tables), `aap_gateway` (58 tables), `eda` (52 tables)

### 6.3 Check Logs

```bash
# Controller logs
kubectl logs -n aap27 -l app.kubernetes.io/component=automationcontroller \
  -c controller-task --since=5m | grep -iE 'error|exception' | \
  grep -v periodic_resource_sync

# Gateway logs
kubectl logs -n aap27 -l app.kubernetes.io/name=gateway \
  -c api --since=5m | grep -iE 'error|exception'

# EDA logs
kubectl logs -n aap27 -l app.kubernetes.io/name=eda-api \
  -c eda-api --since=5m | grep -iE 'error|exception'

kubectl logs -n aap27 -l app.kubernetes.io/name=eda-default-worker \
  --since=5m | grep -iE 'error|exception'
```

---

## Part 7: PostgreSQL-Free Operation

This section covers the final POC milestone: proving that Controller, Gateway, and EDA function **without any PostgreSQL instance running**.

### 7.1 The `DATABASES['default']` Problem

Django and many internal code paths (health checks, `connection.cursor()`, middleware) access `DATABASES['default']` directly, bypassing the `MSSQLRouter`. When PostgreSQL is shut down, these code paths fail with `OperationalError: Connection refused`.

The fix: redirect `DATABASES['default']` to the MSSQL alias in each component's settings.

### 7.2 Controller Settings Update

In `mssql-confd.py`, add after the `DATABASE_ROUTERS = apply_orm_patches(...)` line:

```python
DATABASES['default'] = DATABASES['mssql'].copy()
```

Also update the `myaap-controller-app-credentials` Secret with the new file and update the rsyslog container image to the MSSQL image (it runs `wait-for-migrations` at startup which connects to `default`):

```bash
# Encode and patch the Secret
MSSQL_B64=$(base64 < contrib/mssql-poc/mssql-confd.py)
kubectl patch secret myaap-controller-app-credentials -n aap27 \
  --type merge -p "{\"data\":{\"mssql.py\":\"${MSSQL_B64}\"}}"

# Update rsyslog container image on both controller deployments
kubectl set image deployment/myaap-controller-web -n aap27 \
  myaap-controller-rsyslog=localhost:5001/aap27/controller-rhel9:2.7-mssql
kubectl set image deployment/myaap-controller-task -n aap27 \
  myaap-controller-rsyslog=localhost:5001/aap27/controller-rhel9:2.7-mssql

# Update init-database container image on controller-task
kubectl set image deployment/myaap-controller-task -n aap27 \
  init-database=localhost:5001/aap27/controller-rhel9:2.7-mssql

# CRITICAL: Add mssql.py volume mount to rsyslog containers
# The rsyslog sidecar runs wait-for-migrations which uses DATABASES['default'].
# Without the mssql.py conf.d mount, it can't load the DATABASES['default'] redirect
# and will crash-loop trying to connect to PG — making the pod NotReady and
# causing Envoy to mark the controller upstream as "no healthy upstream".
kubectl patch deployment myaap-controller-web -n aap27 --type='json' -p='[
  {"op":"add","path":"/spec/template/spec/containers/2/volumeMounts/-","value":{
    "mountPath":"/etc/tower/conf.d/mssql.py",
    "name":"myaap-controller-application-credentials",
    "readOnly":true,"subPath":"mssql.py"}}
]'
kubectl patch deployment myaap-controller-task -n aap27 --type='json' -p='[
  {"op":"add","path":"/spec/template/spec/containers/3/volumeMounts/-","value":{
    "mountPath":"/etc/tower/conf.d/mssql.py",
    "name":"myaap-controller-application-credentials",
    "readOnly":true,"subPath":"mssql.py"}}
]'

kubectl rollout restart deployment/myaap-controller-web deployment/myaap-controller-task -n aap27
```

### 7.3 Gateway Settings Update

In `gateway-mssql-settings.py`, add after `DATABASE_ROUTERS = apply_orm_patches(...)`:

```python
DATABASES['default'] = DATABASES['mssql'].copy()
```

**Critical Gateway caveat**: The gateway settings module (`aap_gateway_api.settings`) uses `load_custom_envvars()` which maps `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_NAME`, `DATABASE_USER`, `DATABASE_PASSWORD` environment variables directly to `DATABASES['default']`. These env vars override the `DATABASES['default']` redirect from the settings file. You must also set these env vars to point at MSSQL:

```bash
# Override DATABASE_* env vars on the gateway deployment
kubectl set env deployment/myaap-gateway -n aap27 --containers=api \
  DATABASE_HOST=host.docker.internal \
  DATABASE_PORT=1433 \
  DATABASE_NAME=aap_gateway \
  DATABASE_USER=SA \
  DATABASE_PASSWORD='AAP_P0C_Password_2026!' \
  DATABASE_ENGINE=mssql

# Update the settings Secret
GW_SETTINGS=$(cat gateway-base-settings.py gateway-mssql-settings.py)
GW_B64=$(echo "$GW_SETTINGS" | base64)
kubectl patch secret myaap-gateway-settings -n aap27 \
  --type merge -p "{\"data\":{\"settings.py\":\"${GW_B64}\"}}"

# Skip migrations in init container (PG is down, migrations already applied)
kubectl patch deployment myaap-gateway -n aap27 --type='json' -p='[
  {"op":"replace","path":"/spec/template/spec/initContainers/0/command",
   "value":["echo","Migrations skipped - PG-free MSSQL mode"]}
]'
```

### 7.4 EDA Settings Update

In `eda-mssql-settings.py`, add after `DATABASE_ROUTERS = apply_orm_patches(...)`:

```python
DATABASES['default'] = DATABASES['mssql'].copy()
```

Then rebuild and push the EDA image (settings are baked in):

```bash
podman build -f Dockerfile.eda -t localhost:5001/aap27/eda-controller-rhel9:2.7-mssql-v17 .
podman push --tls-verify=false localhost:5001/aap27/eda-controller-rhel9:2.7-mssql-v17

# Update all EDA deployments and their init containers
IMG=localhost:5001/aap27/eda-controller-rhel9:2.7-mssql-v17
for dep in myaap-eda-api myaap-eda-activation-worker myaap-eda-default-worker myaap-eda-event-stream; do
  kubectl set image deployment/$dep -n aap27 --all=$IMG  # containers
done

# Remove SKIP_MSSQL from init containers and set DJANGO_SETTINGS_MODULE
for dep in myaap-eda-api myaap-eda-activation-worker myaap-eda-default-worker myaap-eda-event-stream; do
  kubectl patch deployment $dep -n aap27 --type='json' -p="[...]"  # see below
done
```

Init containers must have `SKIP_MSSQL` removed and `DJANGO_SETTINGS_MODULE=eda_mssql_settings` set, otherwise they fall back to PG and crash:

```bash
kubectl patch deployment <eda-deployment> -n aap27 --type='json' -p='[
  {"op":"replace","path":"/spec/template/spec/initContainers/0/env","value":[
    {"name":"DJANGO_SETTINGS_MODULE","value":"eda_mssql_settings"},
    {"name":"PYTHONPATH","value":"/opt/mssql-poc"},
    {"name":"EDA_SECRET_KEY","valueFrom":{"secretKeyRef":{
      "name":"myaap-eda-db-fields-encryption-secret","key":"secret_key"}}}
  ]}
]'
```

### 7.5 Shut Down PostgreSQL

```bash
kubectl scale statefulset myaap-postgres-15 -n aap27 --replicas=0
```

### 7.6 Verification

After all pods restart:

```bash
# Confirm no PG pods
kubectl get pods -n aap27 | grep postgres  # should return nothing

# Controller
kubectl exec -n aap27 deployment/myaap-controller-web -c myaap-controller-web \
  -- curl -sk http://localhost:8052/api/v2/ping/

# Gateway
curl -s http://localhost:44927/api/gateway/v1/ping/

# EDA
curl -s http://localhost:44927/api/eda/v1/status/

# Verify database engine
kubectl exec -n aap27 deployment/myaap-controller-web -c myaap-controller-web \
  -- bash -c "awx-manage shell -c \"
from django.db import connections
for alias in ['default','mssql']:
    c = connections[alias]; c.ensure_connection()
    print(f'{alias}: {c.vendor} @ {c.settings_dict[\\\"HOST\\\"]}')\""
```

Expected output for all three components: `ENGINE=microsoft`, `HOST=host.docker.internal`.

### 7.7 Known Issues in PG-Free Mode

| Issue | Impact | Workaround |
|-------|--------|------------|
| Controller rsyslog container crash-loops with non-MSSQL image | Pod shows NotReady | Use MSSQL controller image for rsyslog container |
| Gateway `load_custom_envvars()` overrides `DATABASES['default']` | Default alias points to dead PG | Set `DATABASE_*` env vars to MSSQL values |
| EDA init containers have `SKIP_MSSQL=1` | Init containers crash trying to connect to PG | Remove `SKIP_MSSQL`, set `DJANGO_SETTINGS_MODULE=eda_mssql_settings` |
| Gateway `dispatcherd_connected: false` | Non-critical — dispatcherd health check uses different mechanism | Cosmetic only, gateway fully functional |
| Automation Hub non-functional | Hub has not been migrated to MSSQL | Out of scope for this POC |
| Controller `ansible_id` column type mismatch | "Automation Executions" missing from UI, all authenticated API calls return 500 | Fix column from `char(32)` to `uniqueidentifier` (see 7.8) |

### 7.8 Fix Controller Resource Registry UUID Format

After data migration, the controller's `dab_resource_registry_resource.ansible_id` column may be `char(32)` with unhyphenated hex strings, while gateway and EDA use `uniqueidentifier`. This causes 500 errors on all authenticated controller API calls through the gateway because DAB JWT auth passes hyphenated UUIDs that can't match against the `char(32)` values.

**Symptom:** Controller API returns `Conversion failed when converting from a character string to uniqueidentifier (8169)` in `get_object_by_ansible_id`. The "Automation Executions" section is missing from the AAP UI.

**Fix (run from any pod with pyodbc access to the awx database):**

```python
import pyodbc
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=host.docker.internal,1433;DATABASE=awx;'
    'UID=SA;PWD=<password>;TrustServerCertificate=yes;')
conn.autocommit = True
c = conn.cursor()

# 1. Drop the unique constraint (name may vary — check with sys.indexes query)
c.execute("ALTER TABLE dab_resource_registry_resource DROP CONSTRAINT UQ__dab_reso__4985FC72617547C9")

# 2. Widen column to fit hyphenated UUIDs
c.execute("ALTER TABLE dab_resource_registry_resource ALTER COLUMN ansible_id varchar(36) NOT NULL")

# 3. Insert hyphens (8-4-4-4-12 format)
c.execute("""
    UPDATE dab_resource_registry_resource
    SET ansible_id = STUFF(STUFF(STUFF(STUFF(RTRIM(ansible_id), 9, 0, '-'), 14, 0, '-'), 19, 0, '-'), 24, 0, '-')
    WHERE LEN(RTRIM(ansible_id)) = 32
""")

# 4. Convert to uniqueidentifier (matches gateway/EDA schema)
c.execute("ALTER TABLE dab_resource_registry_resource ALTER COLUMN ansible_id uniqueidentifier NOT NULL")

# 5. Recreate unique constraint
c.execute("ALTER TABLE dab_resource_registry_resource ADD CONSTRAINT UQ_dab_rr_ansible_id UNIQUE (ansible_id)")
```

**Root cause:** The `mssql_common.install_uuid_format_patch()` override makes `UUIDField.db_type()` return `uniqueidentifier`, but this only affects Django's SQL generation going forward. If the initial schema migration ran before this patch was in place, `ansible_id` was created as `char(32)` — and the PG→MSSQL data migration copied the raw hex values without hyphens.

---

## Shared Library: mssql_common.py

The `lib/mssql_common.py` module centralises all reusable MSSQL patches so that controller, gateway, and EDA don't duplicate code. It provides:

| Function | Purpose |
|----------|---------|
| `apply_orm_patches(DATABASES, db_name, ...)` | One-call setup: adds mssql database, installs router, advisory lock noop, transaction patches, UUID format fix, bulk_create fix |
| `get_sb_broker_config(db_name)` | Build Service Broker config dict for dispatcherd |
| `stub_pg_notify()` | Stub `dispatcherd.brokers.pg_notify` in `sys.modules` |
| `install_uuid_format_patch()` | Override `UUIDField.db_type()` to return `'uniqueidentifier'` on MSSQL, and set `has_native_uuid_field = True` |
| `install_textfield_index_patch()` | Cap TextField to `nvarchar(450)` on MSSQL so SQL Server can index them (required by EDA's `TextField(unique=True)`) |
| `get_odbc_connection_string(db_name)` | Build pyodbc connection string for direct ODBC use |
| `add_mssql_healthcheck(databases, db_name)` | Override `healthcheck` alias to point at MSSQL |

### UUID Format Patch

Without `has_native_uuid_field = True`, Django's `UUIDField.get_db_prep_value()` converts UUIDs to 32-char hex strings (e.g. `f02b136c4db24255bd7c57dadf127ea7`) which SQL Server's `uniqueidentifier` type rejects. With the patch, Django passes UUID objects directly and pyodbc handles the conversion.

---

## Notification Bus Architecture

### Why Not Pure Service Broker?

`pg_notify` is pub/sub: all listeners see every message. Service Broker queues are single-consumer: `RECEIVE` removes messages. Since channels like `tower_settings_change` are consumed by both dispatcherd AND cache_clear, we use a hybrid approach:

- **Polling table** (`awx_notify_messages`): durable, append-only message log. Each consumer tracks its own `last_seen_id` — all consumers see all messages.
- **Service Broker signal**: after INSERT, publisher SENDs a lightweight signal on `AwxSignalQueue`. Consumers block on `WAITFOR RECEIVE` instead of busy-polling — gives near-instant wake-up.

### service_broker.py

The `service_broker.py` module implements the `dispatcherd` Broker Protocol. It's a standalone module with no AWX, gateway, or EDA imports (only `pyodbc`, `dispatcherd.chunking`, `dispatcherd.protocols`). This means the same file works for all three components.

Key features:
- Sync and async connection management
- Message chunking via `dispatcherd.chunking.split_message`
- Self-check health probe with configurable timeout (`max_self_check_message_age_seconds=60`)
- Automatic message cleanup (messages older than 5 minutes are deleted every 5 minutes)

### Three Codepaths Replaced (Controller)

| Codepath | Original | Replacement |
|----------|----------|-------------|
| Dispatcherd task dispatch | `dispatcherd.brokers.pg_notify` (psycopg `LISTEN`/`NOTIFY`) | `awx.main.dispatch.brokers.service_broker` |
| AWX PubSub (ws_heartbeat, cache_clear) | `awx.main.dispatch.PubSub` (raw psycopg) | `_ServiceBrokerPubSub` (monkey-patched) |
| WebSocket relay | `awx.main.wsrelay` (psycopg `AsyncConnection`) | Patched `run()` method |

### One Codepath Replaced (Gateway)

Gateway is simpler — it only uses dispatcherd for cache invalidation broadcasts:

| Codepath | Original | Replacement |
|----------|----------|-------------|
| Dispatcherd (cache invalidation) | `dispatcherd.brokers.pg_notify` | `aap_gateway_api.dispatch.brokers.service_broker` |

No PubSub, no wsrelay, no HostMetric — these are AWX-specific.

### Two Codepaths Replaced (EDA)

EDA uses dispatcherd for both task dispatch and activation worker coordination:

| Codepath | Original | Replacement |
|----------|----------|-------------|
| Dispatcherd (DefaultWorker) | `dispatcherd.brokers.pg_notify` | `service_broker` (via PYTHONPATH) |
| Dispatcherd (ActivationWorker) | `dispatcherd.brokers.pg_notify` | `service_broker` (dynamic channel per RULEBOOK_QUEUE_NAME) |

No PubSub, no wsrelay, no HostMetric — these are AWX-specific. The dispatcherd management command is monkey-patched to use `service_broker` config instead of hardcoded `pg_notify`.

---

## Monkey-Patch Rationale

AWX and Gateway assume PostgreSQL throughout. Rather than modifying source code, this POC uses runtime monkey-patches loaded via Django's configuration mechanisms. This approach:

1. **Zero source changes** — the fork is identical to upstream (broker module is the only new file)
2. **Fully reversible** — remove config to revert to PostgreSQL
3. **Isolates SQL Server concerns** — all MSSQL-specific logic lives in configuration files

### Load Order (Controller)

```
conf.d exec()  ──────>  pg_notify stub (with Broker class)
                         │
                         ├── import service_broker.Broker
                         ├── create _PgNotifyStubBroker subclass
                         ├── register connection_created handlers
                         │
First DB connection ──>  connection_created fires:
                         ├── SET XACT_ABORT OFF
                         ├── patch get_dispatcherd_config
                         ├── patch configure_dispatcherd
                         ├── replace PubSub/pg_bus_conn
                         ├── patch wsrelay
                         └── patch HostMetric upsert
```

### Load Order (Gateway)

```
settings.py append ───>  apply_orm_patches() (eager)
                         stub_pg_notify() (eager)
                         │
                         ├── patch MyAppConfig.ready()
                         │
AppConfig.ready() ────>  _mssql_gw_ready():
                         ├── patch get_dispatcherd_config
                         ├── setup dispatcherd
                         ├── patch PingView._check_db
                         └── patch XDSView.get_qs
```

### Load Order (EDA)

```
DJANGO_SETTINGS_MODULE ──>  eda_mssql_settings.py (settings wrapper)
                            │
                            ├── from aap_eda.settings.default import * (Dynaconf)
                            ├── apply_orm_patches() (eager)
                            ├── pg_notify stub with Broker class (eager)
                            ├── DISPATCHERD_DEFAULT_SETTINGS override (eager)
                            ├── patch CoreConfig.ready()
                            └── patch dispatcherd management command
                            │
AppConfig.ready() ────────> _mssql_eda_ready():
                            ├── dab_decorate import
                            ├── dispatcher_setup(DISPATCHERD_DEFAULT_SETTINGS)
                            └── patch list_requests() (DISTINCT ON → Min/GROUP BY)
```

### Why `weak=False` on Signal Handlers

All `connection_created.connect()` calls use `weak=False`. Conf.d files are loaded via `exec()` — without strong references, Python garbage-collects the handler functions when the exec scope exits, silently disconnecting the signal handlers.

### Why Deferred Patches

Controller patches that import `awx.main.dispatch.config` or `awx.main.dispatch` must be deferred. These modules trigger `awx.settings` loading, creating a circular import if executed during conf.d `exec()`. The deferred approach uses `connection_created` signal which fires after Django is fully initialized.

Gateway patches use `AppConfig.ready()` replacement instead of `connection_created` because the gateway's dispatch module doesn't have the same circular import risk.

---

## Gotchas and Lessons Learned

### SQL Server Specific

| Issue | Symptom | Fix |
|-------|---------|-----|
| **IDENTITY columns missing** | `Cannot insert explicit value for identity column` or auto-increment doesn't work | Run `fix_gateway_identity_columns.py` — mssql-django doesn't create IDENTITY on `id` columns during `migrate` |
| **DISTINCT ON (PostgreSQL-only)** | xDS endpoints return empty/error | Patch `XDSView.get_qs` with `Min('id')` + `GROUP BY` |
| **isjson CHECK constraints** | JSON field operations blocked | Drop all isjson constraints: `fix_schema_gaps.sql` |
| **UNIQUE constraint on NULL** | Can only insert one row with NULL in UNIQUE column | Drop the UNIQUE constraint (see Part 5.3) |
| **IDENTITY_INSERT** | Must be ON when inserting explicit IDs, OFF after | Wrap inserts: `SET IDENTITY_INSERT [table] ON; ... OFF;` |
| **pyodbc parameter passing** | `TypeError: takes from 2 to 3 positional arguments` | pyodbc 5.x: `cursor.execute(sql, [p1, p2])` not `cursor.execute(sql, p1, p2)` |
| **Table partitioning** | Migration 0144 fails | Fake it — SQL Server doesn't support PG-style partitioning |
| **UUID format** | `uniqueidentifier` rejects hex strings | `DatabaseFeatures.has_native_uuid_field = True` |
| **UUIDField → char(32)** | `Insufficient result space to convert uniqueidentifier value to char` in OUTPUT INSERTED | Override `UUIDField.db_type()` to return `'uniqueidentifier'` instead of patching `data_types` dict (avoids circular import) |
| **TextField(unique=True)** | `Cannot create index on nvarchar(max)` — EDA uses TextField with unique=True and explicit indexes | `install_textfield_index_patch()` caps TextField to `nvarchar(450)` on MSSQL |
| **UUID PK tables + IDENTITY** | `Identity column 'id' must be of data type int, bigint...` | Tables with `uniqueidentifier` PKs (`core_audit_action`, `core_audit_event`) don't need IDENTITY — skip them |

### Django / Python Specific

| Issue | Symptom | Fix |
|-------|---------|-----|
| **Circular import** | `ImportError` from `awx.main.dispatch` | Defer patches to `connection_created` or `AppConfig.ready()` |
| **Python import binding** | Patching `module.name` doesn't affect `from module import name` | Patch before the import, or patch the attribute on the target module |
| **pg_notify stub empty** | `AttributeError: module has no attribute 'Broker'` | Stub must include Broker class (dispatcherd resolves broker at config time) |
| **Broker init params** | `RuntimeError: Must specify config` | Broker expects `config=dict`, not `**dict` spread |
| **DATABASES['default']** | Django requires it | Keep pointing at PG or redirect to MSSQL too |
| **Dynaconf settings load** | `AttributeError: 'Settings' object has no attribute 'ANSIBLE_BASE_ORGANIZATION_MODEL'` | Cannot import `aap_eda.tasks.*` at settings-load time — triggers import cascade through models → rbac → settings. Defer to `CoreConfig.ready()` |
| **post_migrate signals** | Migration fails when post_migrate handlers query tables that don't exist yet | Disconnect `post_migrate.receivers` before `migrate`, restore after |

### Deployment Specific

| Issue | Symptom | Fix |
|-------|---------|-----|
| **Gateway MSSQL breaks controller routing** | Controller disappears from UI, `/api/controller/` returns 503, xDS returns `RelatedObjectDoesNotExist` | Service cluster data must be synced to MSSQL before/after gateway switchover — xDS queries now hit MSSQL not PG (see Part 5) |
| **Controller image reverts to base** | `ModuleNotFoundError: No module named 'awx.main.dispatch.brokers'`, dispatcher crash-loops | Deployment spec wasn't updated — use `kubectl set image` to persist the `2.7-mssql` tag in the deployment, not just the running pod. Verify with `kubectl get deploy -o jsonpath='{.spec.template.spec.containers[*].image}'` |
| **FK constraint ordering** | `DELETE conflicted with REFERENCE constraint` | Delete in order: serviceapiroute → additionalroute → route → servicekey → servicenode → httpport → servicecluster |
| **Gateway login is form-encoded** | 403 or redirect to login page | POST to `/api/gateway/v1/login/` with `Content-Type: application/x-www-form-urlencoded` and CSRF token, not JSON |
| **cache-clear psycopg error** | `psycopg.errors.UndefinedTable: relation "awx_notify_messages"` | Non-critical — `run_cache_clear` imports `pg_bus_conn` before deferred patch fires. Self-recovers on restart |
| **servicenode has no port column** | `Invalid column name 'port'` | MSSQL servicenode table doesn't have a `port` column (only: id, name, address, tags, etc.) |
| **dispatcherd_connected: false in gateway ping** | Gateway ping shows false | Non-critical for POC — dispatcherd status check may not fully initialize in all modes |
| **EDA nginx containers crash** | `ImproperlyConfigured: Either "SECRET_KEY" or "SECRET_KEY_FILE"` | Nginx containers in eda-api and eda-event-stream need `envFrom` (configMapRef) and `EDA_SECRET_KEY` (secretKeyRef) even though they run nginx — entrypoint imports Django |
| **EDA 503 through gateway** | Clicking anything in Automation Decisions returns "Service Unavailable" | Envoy health-checks `/api/eda/v1/status/` on port 8000. `check_dispatcherd_workers_health()` fails because `control_with_reply("alive")` doesn't work through Service Broker. Fix: (1) bypass health check in `_mssql_eda_ready()`, (2) update nginx containers to MSSQL image + env vars (see Part 4.8) |
| **EDA nginx is actually gunicorn** | Port 8000 still returns unhealthy after patching eda-api | The container named "nginx" in eda-api and eda-event-stream runs **gunicorn** on port 8000, not actual nginx. Must use MSSQL image + `DJANGO_SETTINGS_MODULE` env var on this container too |
| **Python from-import binding** | `_health.check_dispatcherd_workers_health = bypass` doesn't fix the view | `from aap_eda.core.health import check_dispatcherd_workers_health` in views.py creates a local binding. Must patch **both** `aap_eda.core.health` AND `aap_eda.core.views` modules |
| **eda-initial-data init container** | `Organization.DoesNotExist` | Pre-existing PG issue; make init container tolerate failures with `|| echo WARN` |
| **Image tag caching (K8s)** | Rebuilt image with same tag not picked up | Use incrementing tags (v2, v3, ...) to force image pulls. `imagePullPolicy: Always` also works but is slower |
| **Missing DAB content types** | `RuntimeError: Could not find content type for ('eda', 'core', 'auditrule')` on project create | `post_migrate` signals were disabled during migration — DAB RBAC content types and permissions created by those signals are missing. Copy from PG with `IDENTITY_INSERT ON` (see Part 4.9.1) |
| **Content type ID mismatch** | `Resource.DoesNotExist` on user create or role assignment in Gateway | `django_content_type` IDs differ between PG and MSSQL. `dab_resource_registry_resource.content_type_id` retains PG values after data copy. Remap to MSSQL IDs (see Part 4.9.2) |
| **Duplicate Resource entries** | `Resource.MultipleObjectsReturned: get() returned more than one Resource` on login | After fixing content type IDs, duplicate Resource rows exist (original + auto-created). Delete the auto-created duplicate, keep the one matching PG's `ansible_id` (see Part 4.9.3) |
| **Gateway `load_custom_envvars()` overrides `DATABASES['default']`** | Gateway `default` alias points to dead PG despite redirect in settings file | `aap_gateway_api.settings_utils._CUSTOM_ENVVAR_MAPPINGS` maps `DATABASE_HOST` → `DATABASES__default__HOST` etc. These env vars run AFTER the settings file. Must also set `DATABASE_HOST=host.docker.internal DATABASE_PORT=1433` etc. on the gateway deployment (see Part 7.3) |
| **EDA init containers have `SKIP_MSSQL=1`** | Init containers crash-loop when PG is down | Init containers were configured to skip MSSQL during initial migration. For PG-free operation, remove `SKIP_MSSQL` and set `DJANGO_SETTINGS_MODULE=eda_mssql_settings` (see Part 7.4) |
| **Controller rsyslog uses non-MSSQL image** | rsyslog container crash-loops on `wait-for-migrations` | rsyslog container built from base controller image, which connects to PG. Use the MSSQL controller image for this container (see Part 7.2) |
| **Controller rsyslog missing mssql.py mount** | rsyslog crash-loops even with MSSQL image; controller shows "no healthy upstream" in UI | rsyslog container doesn't have the `/etc/tower/conf.d/mssql.py` volume mount. Without it, `DATABASES['default']` redirect never loads. Pod shows NotReady, Envoy ejects the upstream. Add the volume mount via `kubectl patch` (see Part 7.2) |
| **Controller `ansible_id` char(32) vs uniqueidentifier** | Controller API returns 500 on all authenticated requests through gateway; "Automation Executions" missing from UI | `dab_resource_registry_resource.ansible_id` migrated as `char(32)` with unhyphenated hex strings (e.g. `d60e11536a304de88511352a81cd333e`) but gateway and EDA use `uniqueidentifier`. DAB JWT auth passes hyphenated UUIDs → MSSQL can't match. Fix: drop unique constraint, ALTER to `varchar(36)`, UPDATE with STUFF to insert hyphens, ALTER to `uniqueidentifier`, recreate constraint (see Part 7.8) |

---

## File Reference

### Container Build

| File | Description |
|------|-------------|
| `Dockerfile.controller` | Full Dockerfile for controller MSSQL image (ODBC + pyodbc + mssql-django + shared lib + broker) |
| `Dockerfile.gateway` | Full Dockerfile for gateway MSSQL image (same structure, gateway paths) |
| `Dockerfile.eda` | Full Dockerfile for EDA MSSQL image (system Python, all files to /opt/mssql-poc/) |
| `Dockerfile.fragment` | Container build fragment for controller (ODBC driver + Python packages only) |
| `Dockerfile.gateway.fragment` | Container build fragment for gateway |

### Configuration

| File | Description |
|------|-------------|
| `mssql-confd.py` | Controller conf.d configuration: ORM routing + dispatch + PubSub + wsrelay patches |
| `gateway-mssql-settings.py` | Gateway settings append: ORM routing + dispatch + xDS + PingView patches |
| `eda-mssql-settings.py` | EDA Dynaconf wrapper: ORM routing + dispatcherd + DISTINCT ON patch + management command override |
| `lib/mssql_common.py` | Shared library: reusable ORM patches, broker config, pg_notify stub, TextField index fix |
| `service_broker.py` | Dispatcherd broker module: SQL Server notification bus (used by all three components) |

### SQL Scripts

| File | Description |
|------|-------------|
| `scripts/setup_notification_bus.sql` | Controller: create `awx` notification bus objects |
| `scripts/setup_gateway_db.sql` | Gateway: create `aap_gateway` database + notification bus objects |
| `scripts/setup_eda_db.sql` | EDA: create `eda` database + notification bus objects |
| `scripts/fix_schema_gaps.sql` | Controller: fix missing columns, tables, views, drop isjson constraints |
| `scripts/fix_gateway_schema_gaps.sql` | Gateway: drop isjson constraints, reseed IDENTITY counters |

### Data Migration

| File | Description |
|------|-------------|
| `scripts/migrate_pg_to_mssql.py` | Controller: PG → MSSQL data migration (run in controller pod) |
| `scripts/migrate_gateway_data.py` | Gateway: PG → MSSQL data migration (run in gateway pod) |
| `scripts/migrate_eda_data.py` | EDA: PG → MSSQL data migration (run in EDA pod) |
| `scripts/fix_gateway_identity_columns.py` | Gateway: add IDENTITY property to all `id` columns (rebuild tables) |
| `scripts/fix_eda_identity_columns.py` | EDA: add IDENTITY property to `id` columns (skips UUID PK tables) |

### Deployment

| File | Description |
|------|-------------|
| `scripts/deploy_gateway_mssql.sh` | Automated gateway MSSQL deployment script |
| `scripts/demo-video-script.sh` | Demo video automation script |
| `scripts/demo-video-script.md` | Demo video narration script |

---

## Known Limitations

- **Table partitioning**: SQL Server does not support PostgreSQL-style table partitioning. Migration 0144 is faked; event tables are unpartitioned.
- **mssql-django maintenance**: The `mssql-django` package (1.7.3) is community-maintained. Production use requires evaluation of long-term support.
- **Notification latency**: The WAITFOR RECEIVE cycle adds ~20s round-trip for the self-check health probe. Actual message delivery is near-instant when a Service Broker signal wakes the consumer. For production, the WAITFOR timeout could be reduced.
- **DATABASES['default']**: Django requires a `default` database entry. Resolved — see [Part 7: PostgreSQL-Free Operation](#part-7-postgresql-free-operation) for the full fix including the Gateway `load_custom_envvars()` caveat.
- **cache-clear race condition**: The `run_cache_clear` command imports `pg_bus_conn` before the deferred patch fires, getting the original PubSub with psycopg. Non-critical — it self-recovers on restart.
- **Gateway `dispatcherd_connected: false`**: The gateway ping endpoint may show `dispatcherd_connected: false`. Non-critical for the POC.
- **rotate_secret_key.py**: Uses `%s::jsonb` PostgreSQL-specific cast. Not critical (management command, not runtime).
- **cursor_store.py**: Uses `ON CONFLICT ... DO UPDATE` PostgreSQL upsert. Not critical (only runs during `migrate_service_data` command).
- **periodic_resource_sync**: Fails with 401 to Gateway. Pre-existing issue, unrelated to SQL Server.
- **EDA dispatcherd health check bypassed**: `check_dispatcherd_workers_health()` is stubbed to always return `True` because `control_with_reply("alive")` doesn't work through Service Broker. Workers ARE running; only the health probe mechanism is incompatible. A production implementation would need a Service Broker-compatible health check.
- **Post-migration data integrity**: Disabling `post_migrate` signals during schema migration means some RBAC data (DAB content types, permissions, join table entries) must be manually copied from PG. Content type IDs must also be remapped in the Resource registry. These are one-time fixes documented in Part 4.9.
