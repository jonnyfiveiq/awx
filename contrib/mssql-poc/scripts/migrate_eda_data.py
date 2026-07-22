"""
Phase 3.1: Migrate EDA data from PostgreSQL to SQL Server.
Runs inside an EDA pod via kubectl exec.
ANSTRAT-1887: EDA MSSQL Migration

Usage:
  kubectl exec -it deploy/myaap-eda-api -c eda-api -- \
    PYTHONPATH=/opt/mssql-poc DJANGO_SETTINGS_MODULE=eda_mssql_settings \
    python3 /tmp/migrate_eda_data.py
"""
import django
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'eda_mssql_settings')
django.setup()

from django.db import connections

pg = connections['default']
ms = connections['mssql']


def get_columns(conn, table):
    if conn.vendor == 'postgresql':
        with conn.cursor() as c:
            c.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position", [table]
            )
            return [r[0] for r in c.fetchall()]
    else:
        with conn.cursor() as c:
            c.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = %s ORDER BY ORDINAL_POSITION", [table]
            )
            return [r[0] for r in c.fetchall()]


def get_pk_column(conn, table):
    cols = get_columns(conn, table)
    if 'id' in cols:
        return 'id'
    for c in cols:
        if c.endswith('_id') or c == 'key':
            return c
    return cols[0]


def has_identity(ms_conn, table):
    with ms_conn.cursor() as c:
        c.execute(
            "SELECT COUNT(*) FROM sys.identity_columns ic "
            "JOIN sys.tables t ON ic.object_id = t.object_id "
            "WHERE t.name = %s", [table]
        )
        return c.fetchone()[0] > 0


def table_exists(conn, table):
    if conn.vendor == 'postgresql':
        with conn.cursor() as c:
            c.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = %s AND table_type = 'BASE TABLE'", [table]
            )
            return c.fetchone()[0] > 0
    else:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) FROM sys.tables WHERE name = %s", [table])
            return c.fetchone()[0] > 0


def migrate_table(table, pk_col=None):
    if not table_exists(pg, table):
        print(f"  SKIP {table}: not in PG")
        return 0
    if not table_exists(ms, table):
        print(f"  SKIP {table}: not in MSSQL")
        return 0

    pg_cols = get_columns(pg, table)
    ms_cols = get_columns(ms, table)
    common_cols = [c for c in pg_cols if c in ms_cols]
    if not common_cols:
        print(f"  SKIP {table}: no common columns")
        return 0

    if pk_col is None:
        pk_col = get_pk_column(pg, table)

    with ms.cursor() as c:
        c.execute(f"SELECT [{pk_col}] FROM [{table}]")
        existing_pks = set(r[0] for r in c.fetchall())

    col_list = ', '.join(f'"{c}"' for c in common_cols)
    with pg.cursor() as c:
        c.execute(f'SELECT {col_list} FROM "{table}"')
        pg_rows = c.fetchall()

    pk_idx = common_cols.index(pk_col)
    new_rows = [r for r in pg_rows if r[pk_idx] not in existing_pks]

    if not new_rows:
        print(f"  SKIP {table}: no new rows to migrate")
        return 0

    ms_col_list = ', '.join(f'[{c}]' for c in common_cols)
    placeholders = ', '.join(['%s'] * len(common_cols))
    identity = has_identity(ms, table)

    inserted = 0
    with ms.cursor() as c:
        if identity:
            c.execute(f"SET IDENTITY_INSERT [{table}] ON")

        for row in new_rows:
            try:
                values = []
                for v in row:
                    if isinstance(v, memoryview):
                        values.append(bytes(v))
                    else:
                        values.append(v)
                c.execute(
                    f"INSERT INTO [{table}] ({ms_col_list}) VALUES ({placeholders})",
                    values
                )
                inserted += 1
            except Exception as e:
                err = str(e)[:120]
                print(f"  WARN {table} row pk={row[pk_idx]}: {err}")

        if identity:
            c.execute(f"SET IDENTITY_INSERT [{table}] OFF")

    print(f"  OK {table}: migrated {inserted}/{len(new_rows)} rows")
    return inserted


def migrate_m2m_table(table):
    if not table_exists(pg, table) or not table_exists(ms, table):
        print(f"  SKIP {table}: missing in PG or MSSQL")
        return 0

    pg_cols = get_columns(pg, table)
    ms_cols = get_columns(ms, table)
    common_cols = [c for c in pg_cols if c in ms_cols]

    col_list_pg = ', '.join(f'"{c}"' for c in common_cols)
    col_list_ms = ', '.join(f'[{c}]' for c in common_cols)

    with pg.cursor() as c:
        c.execute(f'SELECT {col_list_pg} FROM "{table}"')
        pg_rows = c.fetchall()

    with ms.cursor() as c:
        c.execute(f'SELECT {col_list_ms} FROM [{table}]')
        ms_rows = set(tuple(r) for r in c.fetchall())

    new_rows = [r for r in pg_rows if tuple(r) not in ms_rows]

    if not new_rows:
        print(f"  SKIP {table}: no new rows")
        return 0

    placeholders = ', '.join(['%s'] * len(common_cols))
    identity = has_identity(ms, table)

    inserted = 0
    with ms.cursor() as c:
        if identity:
            c.execute(f"SET IDENTITY_INSERT [{table}] ON")

        for row in new_rows:
            try:
                values = [bytes(v) if isinstance(v, memoryview) else v for v in row]
                c.execute(
                    f"INSERT INTO [{table}] ({col_list_ms}) VALUES ({placeholders})",
                    values
                )
                inserted += 1
            except Exception as e:
                print(f"  WARN {table}: {str(e)[:120]}")

        if identity:
            c.execute(f"SET IDENTITY_INSERT [{table}] OFF")

    print(f"  OK {table}: migrated {inserted}/{len(new_rows)} rows")
    return inserted


print("=== EDA Phase 3.1: PG -> MSSQL Data Migration ===\n")

total = 0

# --- Django framework tables ---
print("Group A: Django framework tables")
for t in [
    'django_content_type',
    'auth_permission',
    'auth_group',
    'auth_group_permissions',
    'django_session',
    'django_migrations',
]:
    try:
        total += migrate_table(t)
    except Exception as e:
        print(f"  WARN {t}: {str(e)[:100]}")

# --- EDA users and orgs ---
print("\nGroup B: EDA users and organizations")
total += migrate_table('core_user')
total += migrate_m2m_table('core_user_groups')
total += migrate_m2m_table('core_user_user_permissions')
total += migrate_table('core_organization')
total += migrate_table('core_team')

# --- EDA credentials and environments ---
print("\nGroup C: Credentials and environments")
total += migrate_table('core_credential_type')
total += migrate_table('core_eda_credential')
total += migrate_table('core_credential_input_source')
total += migrate_table('core_decision_environment')
total += migrate_table('core_awxtoken')

# --- EDA projects and rulebooks ---
print("\nGroup D: Projects and rulebooks")
total += migrate_table('core_project')
total += migrate_table('core_rulebook')

# --- EDA activations ---
print("\nGroup E: Activations and event streams")
total += migrate_table('core_eventstream')
total += migrate_table('core_activation')
total += migrate_m2m_table('core_activation_eda_credentials')
total += migrate_m2m_table('core_activation_event_streams')
total += migrate_table('core_activation_request_queue')

# --- EDA jobs and instances ---
print("\nGroup F: Jobs and instances")
total += migrate_table('core_job')
total += migrate_table('core_job_instance')
total += migrate_table('core_activation_instance_job_instance')
total += migrate_table('core_job_instance_event')
total += migrate_table('core_job_instance_host')

# --- EDA rulebook processes ---
print("\nGroup G: Rulebook processes")
total += migrate_table('core_rulebook_process')
total += migrate_table('core_rulebook_process_log')
total += migrate_table('core_rulebookprocessqueue')

# --- EDA audit ---
print("\nGroup H: Audit")
total += migrate_table('core_audit_rule')
total += migrate_table('core_audit_action')
total += migrate_table('core_audit_event')
total += migrate_m2m_table('core_audit_event_audit_actions')

# --- EDA settings ---
print("\nGroup I: Settings and flags")
total += migrate_table('core_setting')
total += migrate_table('flags_flagstate')

# --- DAB tables ---
print("\nGroup J: DAB RBAC")
total += migrate_table('dab_rbac_dabcontenttype')
total += migrate_table('dab_rbac_dabpermission')
total += migrate_table('dab_rbac_roledefinition')
total += migrate_m2m_table('dab_rbac_roledefinition_permissions')
total += migrate_table('dab_rbac_objectrole')
total += migrate_m2m_table('dab_rbac_objectrole_provides_teams')
total += migrate_table('dab_rbac_roleuserassignment')
total += migrate_table('dab_rbac_roleteamassignment')
total += migrate_table('dab_rbac_roleevaluation')
total += migrate_table('dab_rbac_roleevaluationuuid')

print("\nGroup K: DAB Resource Registry")
total += migrate_table('dab_resource_registry_resource')
total += migrate_table('dab_resource_registry_resourcetype')
total += migrate_table('dab_resource_registry_serviceid')

print("\nGroup L: DAB Feature Flags")
total += migrate_table('dab_feature_flags_aapflag')

print(f"\n=== Migration complete: {total} total rows migrated ===")
