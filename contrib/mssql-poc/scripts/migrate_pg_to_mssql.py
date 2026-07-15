"""
Phase 3.1: Migrate missing reference data from PostgreSQL to SQL Server.
Runs inside the controller-task pod via kubectl exec.
"""
import django, os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'awx.settings')
django.setup()

from django.db import connections

pg = connections['default']
ms = connections['mssql']


def get_columns(conn, table):
    """Get column names for a table."""
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
    """Get primary key column name."""
    cols = get_columns(conn, table)
    if 'id' in cols:
        return 'id'
    # For tables without 'id', try common patterns
    for c in cols:
        if c.endswith('_id') or c == 'key':
            return c
    return cols[0]


def has_identity(ms_conn, table):
    """Check if a SQL Server table has an IDENTITY column."""
    with ms_conn.cursor() as c:
        c.execute(
            "SELECT COUNT(*) FROM sys.identity_columns ic "
            "JOIN sys.tables t ON ic.object_id = t.object_id "
            "WHERE t.name = %s", [table]
        )
        return c.fetchone()[0] > 0


def migrate_table(table, pk_col=None):
    """Migrate rows from PG to MSSQL that don't exist in MSSQL."""
    pg_cols = get_columns(pg, table)
    ms_cols = get_columns(ms, table)

    # Use intersection of columns (handle schema differences)
    common_cols = [c for c in pg_cols if c in ms_cols]
    if not common_cols:
        print(f"  SKIP {table}: no common columns")
        return 0

    if pk_col is None:
        pk_col = get_pk_column(pg, table)

    # Get PKs already in MSSQL
    with ms.cursor() as c:
        c.execute(f"SELECT [{pk_col}] FROM [{table}]")
        existing_pks = set(r[0] for r in c.fetchall())

    # Get all rows from PG
    col_list = ', '.join(f'"{c}"' for c in common_cols)
    with pg.cursor() as c:
        c.execute(f'SELECT {col_list} FROM "{table}"')
        pg_rows = c.fetchall()

    # Filter to only rows not in MSSQL
    pk_idx = common_cols.index(pk_col)
    new_rows = [r for r in pg_rows if r[pk_idx] not in existing_pks]

    if not new_rows:
        print(f"  SKIP {table}: no new rows to migrate")
        return 0

    # Insert into MSSQL
    ms_col_list = ', '.join(f'[{c}]' for c in common_cols)
    placeholders = ', '.join(['%s'] * len(common_cols))

    identity = has_identity(ms, table)

    inserted = 0
    with ms.cursor() as c:
        if identity:
            c.execute(f"SET IDENTITY_INSERT [{table}] ON")

        for row in new_rows:
            try:
                # Convert any memoryview/bytes to proper types
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
    """Migrate M2M through table (composite key, check all columns)."""
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


print("=== Phase 3.1: PG → MSSQL Data Migration ===\n")

total = 0

print("Group B: Core reference data")
total += migrate_table('conf_setting', pk_col='id')

print("\nGroup C: RBAC / DAB tables")
total += migrate_table('dab_rbac_roledefinition', pk_col='id')
total += migrate_m2m_table('dab_rbac_roledefinition_permissions')

print("\nGroup D: Resource registry")
total += migrate_table('dab_resource_registry_resource', pk_col='ansible_id')

print("\nGroup E: Activity stream (audit log)")
total += migrate_table('main_activitystream', pk_col='id')
# Also migrate the M2M through tables for activity stream
as_m2m_tables = []
with pg.cursor() as c:
    c.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name LIKE 'main_activitystream_%%' AND table_type = 'BASE TABLE'"
    )
    as_m2m_tables = [r[0] for r in c.fetchall()]

for t in sorted(as_m2m_tables):
    try:
        total += migrate_m2m_table(t)
    except Exception as e:
        print(f"  WARN {t}: {str(e)[:100]}")

print(f"\n=== Migration complete: {total} total rows migrated ===")
