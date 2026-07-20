"""
Fix IDENTITY property on id columns for all gateway MSSQL tables.

SQL Server doesn't support ALTER COLUMN to add IDENTITY, so we must:
1. Drop all FK constraints referencing the table
2. Create a new table with IDENTITY on id
3. Copy data with IDENTITY_INSERT ON
4. Drop old table, rename new
5. Recreate FK constraints
6. Reseed identity counter
"""
import pyodbc

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};SERVER=host.docker.internal,1433;'
    'DATABASE=aap_gateway;UID=SA;PWD=AAP_P0C_Password_2026!;TrustServerCertificate=yes;',
    autocommit=False
)
c = conn.cursor()

# Skip uniqueidentifier columns (UUIDs don't use IDENTITY)
SKIP_TABLES = {'dab_resource_registry_serviceid'}

# Get all tables missing IDENTITY on id
c.execute('''
    SELECT t.name
    FROM sys.tables t
    JOIN sys.columns col ON t.object_id = col.object_id AND col.name = 'id'
    WHERE NOT EXISTS (
        SELECT 1 FROM sys.identity_columns ic
        WHERE ic.object_id = t.object_id AND ic.name = 'id'
    )
    ORDER BY t.name
''')
tables = [r[0] for r in c.fetchall() if r[0] not in SKIP_TABLES]
print(f'Tables to fix: {len(tables)}')

def get_column_defs(table):
    """Get column definitions for a table."""
    c.execute('''
        SELECT
            col.name,
            tp.name as type_name,
            col.max_length,
            col.precision,
            col.scale,
            col.is_nullable,
            col.is_identity,
            dc.definition as default_def,
            dc.name as default_name
        FROM sys.columns col
        JOIN sys.types tp ON col.user_type_id = tp.user_type_id
        LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = col.object_id
            AND dc.parent_column_id = col.column_id
        WHERE col.object_id = OBJECT_ID(?)
        ORDER BY col.column_id
    ''', table)
    return c.fetchall()

def get_indexes(table):
    """Get non-PK indexes for a table."""
    c.execute('''
        SELECT
            i.name,
            i.is_unique,
            i.type_desc,
            STRING_AGG(col.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
        WHERE i.object_id = OBJECT_ID(?)
          AND i.is_primary_key = 0
          AND i.name IS NOT NULL
        GROUP BY i.name, i.is_unique, i.type_desc
    ''', table)
    return c.fetchall()

def get_check_constraints(table):
    """Get CHECK constraints for a table."""
    c.execute('''
        SELECT cc.name, cc.definition
        FROM sys.check_constraints cc
        WHERE cc.parent_object_id = OBJECT_ID(?)
    ''', table)
    return c.fetchall()

def get_fks_referencing(table):
    """Get FK constraints that reference this table (incoming)."""
    c.execute('''
        SELECT
            fk.name,
            OBJECT_NAME(fk.parent_object_id) as from_table,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as from_col,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as to_col,
            fk.delete_referential_action_desc,
            fk.update_referential_action_desc
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        WHERE fk.referenced_object_id = OBJECT_ID(?)
    ''', table)
    return c.fetchall()

def get_fks_from(table):
    """Get FK constraints on this table (outgoing)."""
    c.execute('''
        SELECT
            fk.name,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as from_col,
            OBJECT_NAME(fk.referenced_object_id) as to_table,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as to_col,
            fk.delete_referential_action_desc,
            fk.update_referential_action_desc
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        WHERE fk.parent_object_id = OBJECT_ID(?)
    ''', table)
    return c.fetchall()

def col_type_str(col):
    """Build SQL type string from column metadata."""
    name, type_name, max_len, prec, scale, nullable, is_identity, default_def, default_name = col
    if type_name in ('nvarchar', 'varchar', 'nchar', 'char', 'varbinary'):
        if max_len == -1:
            t = f'{type_name}(max)'
        elif type_name.startswith('n'):
            t = f'{type_name}({max_len // 2})'
        else:
            t = f'{type_name}({max_len})'
    elif type_name in ('decimal', 'numeric'):
        t = f'{type_name}({prec},{scale})'
    elif type_name == 'datetime2':
        t = f'datetime2({scale})'
    elif type_name == 'datetimeoffset':
        t = f'datetimeoffset({scale})'
    else:
        t = type_name
    return t

def action_sql(action_desc):
    """Convert referential action desc to SQL clause."""
    return action_desc.replace('_', ' ')

fixed = 0
errors = 0

for table in tables:
    try:
        print(f'\n--- Fixing {table} ---')

        columns = get_column_defs(table)
        indexes = get_indexes(table)
        checks = get_check_constraints(table)
        incoming_fks = get_fks_referencing(table)
        outgoing_fks = get_fks_from(table)

        c.execute(f'SELECT COUNT(*) FROM [{table}]')
        row_count = c.fetchone()[0]

        col_names = [col[0] for col in columns]

        # Step 1: Drop incoming FK constraints
        for fk_name, from_table, from_col, to_col, del_act, upd_act in incoming_fks:
            c.execute(f'ALTER TABLE [{from_table}] DROP CONSTRAINT [{fk_name}]')
            print(f'  Dropped incoming FK {fk_name} from {from_table}')

        # Step 2: Drop outgoing FK constraints
        for fk_name, from_col, to_table, to_col, del_act, upd_act in outgoing_fks:
            c.execute(f'ALTER TABLE [{table}] DROP CONSTRAINT [{fk_name}]')
            print(f'  Dropped outgoing FK {fk_name}')

        # Step 3: Drop check constraints
        for cc_name, cc_def in checks:
            c.execute(f'ALTER TABLE [{table}] DROP CONSTRAINT [{cc_name}]')

        # Step 4: Drop default constraints
        for col in columns:
            if col[8]:  # default_name
                c.execute(f'ALTER TABLE [{table}] DROP CONSTRAINT [{col[8]}]')

        # Step 5: Drop indexes
        for idx_name, is_unique, type_desc, idx_cols in indexes:
            c.execute(f'DROP INDEX [{idx_name}] ON [{table}]')

        # Step 6: Build new table DDL
        new_table = f'{table}_new'
        col_defs = []
        for col in columns:
            name = col[0]
            type_str = col_type_str(col)
            nullable = 'NULL' if col[5] else 'NOT NULL'
            if name == 'id':
                col_defs.append(f'  [{name}] {type_str} IDENTITY(1,1) NOT NULL')
            else:
                col_defs.append(f'  [{name}] {type_str} {nullable}')

        create_sql = f'CREATE TABLE [{new_table}] (\n'
        create_sql += ',\n'.join(col_defs)
        create_sql += f',\n  CONSTRAINT [PK_{new_table}] PRIMARY KEY CLUSTERED ([id])'
        create_sql += '\n)'

        c.execute(create_sql)
        print(f'  Created {new_table} with IDENTITY')

        # Step 7: Copy data
        if row_count > 0:
            col_list = ', '.join(f'[{n}]' for n in col_names)
            c.execute(f'SET IDENTITY_INSERT [{new_table}] ON')
            c.execute(f'INSERT INTO [{new_table}] ({col_list}) SELECT {col_list} FROM [{table}]')
            c.execute(f'SET IDENTITY_INSERT [{new_table}] OFF')
            print(f'  Copied {row_count} rows')

        # Step 8: Drop old table (this also drops its PK)
        c.execute(f'DROP TABLE [{table}]')

        # Step 9: Rename new table
        c.execute(f"EXEC sp_rename '{new_table}', '{table}'")
        c.execute(f"EXEC sp_rename 'PK_{new_table}', 'PK_{table}', 'OBJECT'")
        print(f'  Renamed to {table}')

        # Step 10: Recreate default constraints
        for col in columns:
            if col[7]:  # default_def
                c.execute(f'ALTER TABLE [{table}] ADD DEFAULT {col[7]} FOR [{col[0]}]')

        # Step 11: Recreate check constraints
        for cc_name, cc_def in checks:
            c.execute(f'ALTER TABLE [{table}] ADD CONSTRAINT [{cc_name}] CHECK {cc_def}')

        # Step 12: Recreate indexes
        for idx_name, is_unique, type_desc, idx_cols in indexes:
            unique = 'UNIQUE' if is_unique else ''
            cols = ', '.join(f'[{c.strip()}]' for c in idx_cols.split(','))
            c.execute(f'CREATE {unique} NONCLUSTERED INDEX [{idx_name}] ON [{table}] ({cols})')

        # Step 13: Recreate outgoing FK constraints
        for fk_name, from_col, to_table_name, to_col, del_act, upd_act in outgoing_fks:
            on_del = f'ON DELETE {action_sql(del_act)}' if del_act != 'NO_ACTION' else ''
            on_upd = f'ON UPDATE {action_sql(upd_act)}' if upd_act != 'NO_ACTION' else ''
            c.execute(f'''ALTER TABLE [{table}] ADD CONSTRAINT [{fk_name}]
                FOREIGN KEY ([{from_col}]) REFERENCES [{to_table_name}] ([{to_col}])
                {on_del} {on_upd}''')

        # Step 14: Recreate incoming FK constraints
        for fk_name, from_table, from_col, to_col, del_act, upd_act in incoming_fks:
            on_del = f'ON DELETE {action_sql(del_act)}' if del_act != 'NO_ACTION' else ''
            on_upd = f'ON UPDATE {action_sql(upd_act)}' if upd_act != 'NO_ACTION' else ''
            c.execute(f'''ALTER TABLE [{from_table}] ADD CONSTRAINT [{fk_name}]
                FOREIGN KEY ([{from_col}]) REFERENCES [{table}] ([{to_col}])
                {on_del} {on_upd}''')

        # Step 15: Reseed identity
        if row_count > 0:
            c.execute(f"DBCC CHECKIDENT ('{table}', RESEED)")

        conn.commit()
        fixed += 1
        print(f'  DONE ({row_count} rows preserved)')

    except Exception as e:
        conn.rollback()
        errors += 1
        print(f'  ERROR: {e}')

print(f'\n=== Fixed {fixed}/{len(tables)} tables, {errors} errors ===')

# Verify
c.execute('''
    SELECT t.name
    FROM sys.tables t
    JOIN sys.columns col ON t.object_id = col.object_id AND col.name = 'id'
    WHERE NOT EXISTS (
        SELECT 1 FROM sys.identity_columns ic
        WHERE ic.object_id = t.object_id AND ic.name = 'id'
    )
    AND t.name NOT IN ('dab_resource_registry_serviceid')
    ORDER BY t.name
''')
remaining = [r[0] for r in c.fetchall()]
if remaining:
    print(f'\nStill missing IDENTITY: {remaining}')
else:
    print('\nAll tables now have IDENTITY on id column!')

conn.close()
