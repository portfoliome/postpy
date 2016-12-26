"""
Database administration queries
"""

import psycopg2

from postpy.base import Table, Column, Database, PrimaryKey
from postpy.ddl import compile_qualified_name
from postpy.sql import select_dict


def get_user_tables(conn):
    """Retrieve all user tables."""

    query_string = "select schemaname, relname from pg_stat_user_tables;"
    with conn.cursor() as cursor:
        cursor.execute(query_string)
        tables = cursor.fetchall()

    return tables


def get_primary_keys(conn, table: str, schema='public'):
    """Returns primary key columns for a specific table."""

    query = """\
SELECT
  c.constraint_name as pkey_constraint_name,
  c.column_name as column_name
FROM
  information_schema.key_column_usage AS c
LEFT JOIN information_schema.table_constraints AS t
  ON t.constraint_name=c.constraint_name
WHERE t.constraint_type = 'PRIMARY KEY'
  AND c.table_schema=%s
  AND c.table_name=%s;"""

    for record in select_dict(conn, query, params=(schema, table)):
        yield record['column_name']


def get_column_metadata(conn, table: str, schema='public'):
    """Returns column data following db.Column parameter specification."""
    query = """\
SELECT
  attname as name,
  format_type(atttypid, atttypmod) AS data_type,
  NOT attnotnull AS nullable
FROM pg_catalog.pg_attribute
WHERE attrelid=%s::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;"""

    qualified_name = compile_qualified_name(table, schema=schema)

    for record in select_dict(conn, query, params=(qualified_name,)):
        yield record


def reflect_table(conn, table_name, schema='public'):
    """Reflect basic table attributes."""

    column_meta = list(get_column_metadata(conn, table_name, schema=schema))
    primary_key_columns = list(get_primary_keys(conn, table_name, schema=schema))

    columns = [Column(**column_data) for column_data in column_meta]
    primary_key = PrimaryKey(primary_key_columns)

    return Table(table_name, columns, primary_key, schema=schema)


def pg_stats_extension_statement():
    """postgres stats extensions statement."""

    return 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements;'


def reset(db_name):
    """Reset database."""

    conn = psycopg2.connect(database='postgres')
    db = Database(db_name)
    conn.autocommit = True

    with conn.cursor() as cursor:
        cursor.execute(db.drop_statement())
        cursor.execute(db.create_statement())
    conn.close()

