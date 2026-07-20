# AAP on MS SQL Server - POC Guide

**ANSTRAT-1887: Bring Your Own Database (BYOD) - MS SQL Server**

This guide covers running the full AAP platform (Controller + Gateway) with ALL ORM traffic on Microsoft SQL Server, with PostgreSQL completely eliminated. It documents a working deployment proven on a Kind cluster with AAP 2.7.

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Environment Reference](#environment-reference)
- [Part 1: SQL Server Setup](#part-1-sql-server-setup)
- [Part 2: Controller on MSSQL](#part-2-controller-on-mssql)
- [Part 3: Gateway on MSSQL](#part-3-gateway-on-mssql)
- [Part 4: Service Cluster Data Sync](#part-4-service-cluster-data-sync)
- [Part 5: End-to-End Verification](#part-5-end-to-end-verification)
- [Shared Library: mssql_common.py](#shared-library-mssql_commonpy)
- [Notification Bus Architecture](#notification-bus-architecture)
- [Monkey-Patch Rationale](#monkey-patch-rationale)
- [Gotchas and Lessons Learned](#gotchas-and-lessons-learned)
- [File Reference](#file-reference)
- [Known Limitations](#known-limitations)

## Architecture

Both Controller and Gateway run entirely on SQL Server. PostgreSQL is eliminated.

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
                         └───────────────────────────────┘
                                  │
                         ┌────────┴─────────────────────┐
                         │    AAP Controller Pods        │
                         │  (task + web)                 │
                         ├──────────────────────────────┤
                         │  Django ORM    dispatcherd    │
                         │  (all models)  + PubSub       │
                         │       │        + wsrelay      │
                         │  MSSQLRouter   (service_broker)│
                         │       ▼              │       │
                         │     mssql ◄──────────┘       │
                         └──────────────────────────────┘
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
| AAP admin password | `r9hwurZIPH1U9FlUWl2SuZMOUnGuBIY6` |
| Controller base image | `localhost:5001/aap27/controller-rhel9:2.7` |
| Gateway base image | `localhost:5001/aap27/gateway-rhel9:2.7` |
| MSSQL image tag | `2.7-mssql` |
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
```

### 1.3 Set Up Notification Bus

Run the notification bus setup SQL for **both** databases. The scripts create the polling table (`awx_notify_messages`) and Service Broker objects:

```bash
# Controller (awx database) — scripts/setup_notification_bus.sql
# Gateway (aap_gateway database) — scripts/setup_gateway_db.sql
```

Each script creates:
- `awx_notify_messages` table: `id BIGINT IDENTITY`, `channel NVARCHAR(200)`, `payload NVARCHAR(MAX)`, `created_at DATETIME2`
- Index `IX_notify_id_channel` on `(id, channel)`
- Service Broker: `AwxSignalMessage` type, `AwxSignalContract`, `AwxSignalQueue`, `AwxSignalService`
- Enables `ENABLE_BROKER` and `TRUSTWORTHY ON` on the database

### Connecting DBeaver

| Target | Host | Port | Database | User | Password |
|--------|------|------|----------|------|----------|
| SQL Server | `localhost` | `1433` | `awx` or `aap_gateway` | `SA` | `AAP_P0C_Password_2026!` |
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

## Part 4: Service Cluster Data Sync (Restoring Envoy Routing)

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

### 4.1 Understanding Service Clusters

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

### 4.2 Delete Order (FK constraints)

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

### 4.3 UNIQUE Constraint on service_type Column

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

### 4.4 IDENTITY_INSERT for Service Cluster Data

When inserting rows with explicit ID values (to match PostgreSQL IDs), you must enable `IDENTITY_INSERT`:

```sql
SET IDENTITY_INSERT aap_gateway_api_servicecluster ON;
INSERT INTO aap_gateway_api_servicecluster (id, name, ..., service_type_id)
VALUES (1, 'Gateway', ..., 1);
-- ... more inserts ...
SET IDENTITY_INSERT aap_gateway_api_servicecluster OFF;
```

### 4.5 Verify xDS Endpoints

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

### 4.6 Verify Controller Routing Through Gateway

```bash
# Controller API through Envoy
curl -s http://localhost:44927/api/controller/v2/ping/ | python3 -m json.tool
```

---

## Part 5: End-to-End Verification

### 5.1 Full Stack Health Check

```bash
# Gateway ping
curl -s http://localhost:44927/api/gateway/v1/ping/ | python3 -m json.tool

# Controller ping (through gateway)
curl -s http://localhost:44927/api/controller/v2/ping/ | python3 -m json.tool

# Login and run a job
# (see Part 2.8 for job execution commands)
```

### 5.2 Expected Results

- Gateway ping: `db_connected: true`
- Controller ping: `200 OK`
- Job execution: `status=successful`, `failed=False`, 9 events
- UI accessible at `http://localhost:44927`
- Controller visible in gateway UI under Resources

### 5.3 Check Logs

```bash
# Controller logs
kubectl logs -n aap27 -l app.kubernetes.io/component=automationcontroller \
  -c controller-task --since=5m | grep -iE 'error|exception' | \
  grep -v periodic_resource_sync

# Gateway logs
kubectl logs -n aap27 -l app.kubernetes.io/name=gateway \
  -c api --since=5m | grep -iE 'error|exception'
```

---

## Shared Library: mssql_common.py

The `lib/mssql_common.py` module centralises all reusable MSSQL patches so that controller and gateway don't duplicate code. It provides:

| Function | Purpose |
|----------|---------|
| `apply_orm_patches(DATABASES, db_name, ...)` | One-call setup: adds mssql database, installs router, advisory lock noop, transaction patches, UUID format fix, bulk_create fix |
| `get_sb_broker_config(db_name)` | Build Service Broker config dict for dispatcherd |
| `stub_pg_notify()` | Stub `dispatcherd.brokers.pg_notify` in `sys.modules` |
| `install_uuid_format_patch()` | Sets `DatabaseFeatures.has_native_uuid_field = True` (MSSQL's `uniqueidentifier` type needs this) |
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

The `service_broker.py` module implements the `dispatcherd` Broker Protocol. It's a standalone module with no AWX or gateway imports (only `pyodbc`, `dispatcherd.chunking`, `dispatcherd.protocols`). This means the same file works for both controller and gateway.

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
| **UNIQUE constraint on NULL** | Can only insert one row with NULL in UNIQUE column | Drop the UNIQUE constraint (see Part 4.3) |
| **IDENTITY_INSERT** | Must be ON when inserting explicit IDs, OFF after | Wrap inserts: `SET IDENTITY_INSERT [table] ON; ... OFF;` |
| **pyodbc parameter passing** | `TypeError: takes from 2 to 3 positional arguments` | pyodbc 5.x: `cursor.execute(sql, [p1, p2])` not `cursor.execute(sql, p1, p2)` |
| **Table partitioning** | Migration 0144 fails | Fake it — SQL Server doesn't support PG-style partitioning |
| **UUID format** | `uniqueidentifier` rejects hex strings | `DatabaseFeatures.has_native_uuid_field = True` |

### Django / Python Specific

| Issue | Symptom | Fix |
|-------|---------|-----|
| **Circular import** | `ImportError` from `awx.main.dispatch` | Defer patches to `connection_created` or `AppConfig.ready()` |
| **Python import binding** | Patching `module.name` doesn't affect `from module import name` | Patch before the import, or patch the attribute on the target module |
| **pg_notify stub empty** | `AttributeError: module has no attribute 'Broker'` | Stub must include Broker class (dispatcherd resolves broker at config time) |
| **Broker init params** | `RuntimeError: Must specify config` | Broker expects `config=dict`, not `**dict` spread |
| **DATABASES['default']** | Django requires it | Keep pointing at PG or redirect to MSSQL too |

### Deployment Specific

| Issue | Symptom | Fix |
|-------|---------|-----|
| **Gateway MSSQL breaks controller routing** | Controller disappears from UI, `/api/controller/` returns 503, xDS returns `RelatedObjectDoesNotExist` | Service cluster data must be synced to MSSQL before/after gateway switchover — xDS queries now hit MSSQL not PG (see Part 4) |
| **Controller image reverts to base** | `ModuleNotFoundError: No module named 'awx.main.dispatch.brokers'`, dispatcher crash-loops | Deployment spec wasn't updated — use `kubectl set image` to persist the `2.7-mssql` tag in the deployment, not just the running pod. Verify with `kubectl get deploy -o jsonpath='{.spec.template.spec.containers[*].image}'` |
| **FK constraint ordering** | `DELETE conflicted with REFERENCE constraint` | Delete in order: serviceapiroute → additionalroute → route → servicekey → servicenode → httpport → servicecluster |
| **Gateway login is form-encoded** | 403 or redirect to login page | POST to `/api/gateway/v1/login/` with `Content-Type: application/x-www-form-urlencoded` and CSRF token, not JSON |
| **cache-clear psycopg error** | `psycopg.errors.UndefinedTable: relation "awx_notify_messages"` | Non-critical — `run_cache_clear` imports `pg_bus_conn` before deferred patch fires. Self-recovers on restart |
| **servicenode has no port column** | `Invalid column name 'port'` | MSSQL servicenode table doesn't have a `port` column (only: id, name, address, tags, etc.) |
| **dispatcherd_connected: false in gateway ping** | Gateway ping shows false | Non-critical for POC — dispatcherd status check may not fully initialize in all modes |

---

## File Reference

### Container Build

| File | Description |
|------|-------------|
| `Dockerfile.controller` | Full Dockerfile for controller MSSQL image (ODBC + pyodbc + mssql-django + shared lib + broker) |
| `Dockerfile.gateway` | Full Dockerfile for gateway MSSQL image (same structure, gateway paths) |
| `Dockerfile.fragment` | Container build fragment for controller (ODBC driver + Python packages only) |
| `Dockerfile.gateway.fragment` | Container build fragment for gateway |

### Configuration

| File | Description |
|------|-------------|
| `mssql-confd.py` | Controller conf.d configuration: ORM routing + dispatch + PubSub + wsrelay patches |
| `gateway-mssql-settings.py` | Gateway settings append: ORM routing + dispatch + xDS + PingView patches |
| `lib/mssql_common.py` | Shared library: reusable ORM patches, broker config, pg_notify stub |
| `service_broker.py` | Dispatcherd broker module: SQL Server notification bus (used by both components) |

### SQL Scripts

| File | Description |
|------|-------------|
| `scripts/setup_notification_bus.sql` | Controller: create `awx` notification bus objects |
| `scripts/setup_gateway_db.sql` | Gateway: create `aap_gateway` database + notification bus objects |
| `scripts/fix_schema_gaps.sql` | Controller: fix missing columns, tables, views, drop isjson constraints |
| `scripts/fix_gateway_schema_gaps.sql` | Gateway: drop isjson constraints, reseed IDENTITY counters |

### Data Migration

| File | Description |
|------|-------------|
| `scripts/migrate_pg_to_mssql.py` | Controller: PG → MSSQL data migration (run in controller pod) |
| `scripts/migrate_gateway_data.py` | Gateway: PG → MSSQL data migration (run in gateway pod) |
| `scripts/fix_gateway_identity_columns.py` | Gateway: add IDENTITY property to all `id` columns (rebuild tables) |

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
- **DATABASES['default']**: Django requires a `default` database entry. After killing PG, `default` may need to point at SQL Server too.
- **cache-clear race condition**: The `run_cache_clear` command imports `pg_bus_conn` before the deferred patch fires, getting the original PubSub with psycopg. Non-critical — it self-recovers on restart.
- **Gateway `dispatcherd_connected: false`**: The gateway ping endpoint may show `dispatcherd_connected: false`. Non-critical for the POC.
- **rotate_secret_key.py**: Uses `%s::jsonb` PostgreSQL-specific cast. Not critical (management command, not runtime).
- **cursor_store.py**: Uses `ON CONFLICT ... DO UPDATE` PostgreSQL upsert. Not critical (only runs during `migrate_service_data` command).
- **periodic_resource_sync**: Fails with 401 to Gateway. Pre-existing issue, unrelated to SQL Server.
