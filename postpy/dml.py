"""
dml.py contains the Data Manipulation Language for Postgresql Server.
"""

from random import randint

from foil.iteration import chunks
from psycopg2.extras import NamedTupleCursor

from postpy.base import make_delete_table, Table
from postpy.sql import execute_transaction


def create_insert_statement(qualified_name, column_names, table_alias=''):
    column_string = ', '.join(column_names)
    value_string = ', '.join(['%s']*len(column_names))

    if table_alias:
        table_alias = ' AS %s' % table_alias

    return 'INSERT INTO {0}{1} ({2}) VALUES ({3})'.format(qualified_name,
                                                          table_alias,
                                                          column_string,
                                                          value_string)


def insert(conn, qualified_name: str, column_names, records):
    """Insert a collection of namedtuple records."""

    query = create_insert_statement(qualified_name, column_names)

    with conn:
        with conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
            for record in records:
                cursor.execute(query, record)


def insert_many(conn, tablename, column_names, records, chunksize=2500):
    """Insert many records by chunking data into insert statements.

    Notes
    -----
    records should be Iterable collection of namedtuples or tuples.
    """

    groups = chunks(records, chunksize)
    column_str = ','.join(column_names)
    insert_template = 'INSERT INTO {table} ({columns}) VALUES {values}'.format(
        table=tablename, columns=column_str, values='{0}')

    with conn:
        with conn.cursor() as cursor:
            for recs in groups:
                record_group = list(recs)
                records_template_str = ','.join(['%s'] * len(record_group))
                insert_query = insert_template.format(records_template_str)
                cursor.execute(insert_query, record_group)


def upsert_records(conn, records, upsert_statement):
    """Upsert records."""

    with conn:
        with conn.cursor() as cursor:
            for record in records:
                cursor.execute(upsert_statement, record)


def format_upsert(qualified_name, column_names, constraint, clause='',
                  table_alias='current'):
    insert_template = create_insert_statement(qualified_name, column_names,
                                              table_alias=table_alias)
    statement = format_upsert_expert(insert_template, column_names,
                                     constraint, clause, table_alias)
    return statement


def format_upsert_expert(insert_template, column_names, constraint, clause='',
                         table_alias='current'):

    constraint_str = ', '.join(constraint)
    non_key_columns = [column for column in column_names if column not in constraint]
    non_key_column_str = ', '.join(non_key_columns)
    excluded_str = ', '.join('EXCLUDED.' + column for column in non_key_columns)

    statement = (
        '{insert_template}'
        ' ON CONFLICT ({constraint})'
        ' DO UPDATE'
        ' SET ({non_key_columns}) = ({excluded})'
        ' {clause};').format(insert_template=insert_template,
                             constraint=constraint_str,
                             non_key_columns=non_key_column_str,
                             excluded=excluded_str, clause=clause,
                             table_alias=table_alias)
    return statement


class DeleteManyPrimaryKey:
    """Deletes subset of table rows.

    Uses DELETE FROM in conjunction with a where clause
    through a temporary table reference containing primary keys
    to delete.
    """

    def __init__(self, table):
        self.table = table
        self.delete_table = make_delete_table(table)

    def __call__(self, conn, records, chunksize=2500):
        with conn:
            execute_transaction(conn,
                                [self.delete_table.create_temporary_statement()])

            insert_many(
                conn, self.delete_table.name, self.delete_table.column_names,
                records, chunksize=chunksize)

            delete_from_statement = delete_joined_table_sql(
                self.table.qualified_name, self.delete_table.name,
                self.table.primary_key.column_names)

            execute_transaction(conn, [delete_from_statement])

        with conn:
            execute_transaction(conn,
                                [self.delete_table.drop_temporary_statement()])


class UpsertPrimaryKey:
    def __init__(self, qualified_name, column_names, primary_key_names):

        if column_names != primary_key_names:
            self.query = format_upsert(qualified_name, column_names,
                                       primary_key_names)
        else:
            # Note: When upsert columns are all primary keys, it's an insert.
            self.query = create_insert_statement(qualified_name, column_names)

    def __call__(self, conn, records):
        upsert_records(conn, records, self.query)


class BulkUpsertPrimaryKey:

    RAND_MIN = 0
    RAND_MAX = 100000
    _TEMP_FORMATTER = 'tmp_bulk_upsert_{random}_{table_name}'

    _INSERT_TEMPLATE = (
        'INSERT INTO {table} ({columns})\n'
        '  SELECT {columns} FROM {temp_table}\n'
    )

    def __init__(self, table: Table, delimiter=',', encoding='utf8',
                 null_str='', header=True, escape_str='\\', quote_char='"',
                 force_not_null=None, force_null=None):

        self.table = table
        self.temp_table = self.make_temp_copy_table()
        self.upsert_query = self.make_upsert_query()
        self.copy_sql = copy_from_csv_sql(self.temp_table.name,
                                          delimiter, encoding,
                                          null_str=null_str, header=header,
                                          escape_str=escape_str,
                                          quote_char=quote_char,
                                          force_not_null=force_not_null,
                                          force_null=force_null)

    def __call__(self, conn, file_object):
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(self.temp_table.create_temporary_statement())
                cursor.copy_expert(self.copy_sql, file_object)
                cursor.execute(self.upsert_query)
                cursor.execute(self.temp_table.drop_temporary_statement())

    def make_upsert_query(self):
        # Note: When upsert columns are all primary keys, it's an insert.
        query = self._INSERT_TEMPLATE.format(
            table=self.table.qualified_name, columns=self.column_str,
            temp_table=self.temp_table.name
        )

        if self.table.column_names != self.table.primary_key_columns:
            query = format_upsert_expert(query,
                                         self.table.column_names,
                                         self.table.primary_key_columns)

        return query

    def make_temp_copy_table(self):
        rand_char = randint(self.RAND_MIN, self.RAND_MAX)
        temp_table_name = self._TEMP_FORMATTER.format(
            table_name=self.table.name, random=rand_char
        )
        table_attributes = self.table._asdict()
        table_attributes['name'] = temp_table_name

        return Table(**table_attributes)

    @property
    def column_str(self):
        return ', '.join(self.table.column_names)


def delete_joined_table_sql(qualified_name, removing_qualified_name, primary_key):
    """SQL statement for a joined delete from.
    Generate SQL statement for deleting the intersection of rows between
    both tables from table referenced by tablename.
    """

    condition_template = 't.{}=d.{}'
    where_clause = ' AND '.join(condition_template.format(pkey, pkey)
                                for pkey in primary_key)
    delete_statement = (
        'DELETE FROM {table} t'
        ' USING {delete_table} d'
        ' WHERE {where_clause}').format(table=qualified_name,
                                        delete_table=removing_qualified_name,
                                        where_clause=where_clause)
    return delete_statement


def compile_truncate_table(qualfied_name):
    """Delete all data in table and vacuum."""

    return 'TRUNCATE %s CASCADE;' % qualfied_name


def copy_from_csv(conn, file, qualified_name: str, delimiter=',', encoding='utf8',
                  null_str='', header=True, escape_str='\\', quote_char='"',
                  force_not_null=None, force_null=None):
    """Copy file-like object to database table.

    Notes
    -----
    Implementation defaults to postgres standard except for encoding.
    Postgres falls back on client encoding, while function defaults to utf-8.

    References
    ----------
    https://www.postgresql.org/docs/current/static/sql-copy.html

    """

    copy_sql = copy_from_csv_sql(qualified_name, delimiter, encoding,
                                 null_str=null_str, header=header,
                                 escape_str=escape_str, quote_char=quote_char,
                                 force_not_null=force_not_null,
                                 force_null=force_null)

    with conn:
        with conn.cursor() as cursor:
            cursor.copy_expert(copy_sql, file)


def copy_from_csv_sql(qualified_name: str, delimiter=',', encoding='utf8',
                      null_str='', header=True, escape_str='\\', quote_char='"',
                      force_not_null=None, force_null=None):

    options = []
    options.append("DELIMITER '%s'" % delimiter)
    options.append("NULL '%s'" % null_str)

    if header:
        options.append('HEADER')

    options.append("QUOTE '%s'" % quote_char)
    options.append("ESCAPE '%s'" % escape_str)

    if force_not_null:
        options.append(_format_force_not_null(column_names=force_not_null))

    if force_null:
        options.append(_format_force_null(column_names=force_null))

    options.append("ENCODING '%s'" % encoding)

    copy_sql = _format_copy_csv_sql(qualified_name, copy_options=options)

    return copy_sql


def _format_copy_csv_sql(qualified_name: str, copy_options: list) -> str:
    options_str = ',\n    '.join(copy_options)

    copy_sql = """\
COPY {table} FROM STDIN
  WITH (
    FORMAT CSV,
    {options})""".format(table=qualified_name, options=options_str)

    return copy_sql


def _format_force_not_null(column_names):
    column_str = ', '.join(column_names)
    force_not_null_str = 'FORCE_NOT_NULL ({})'.format(column_str)
    return force_not_null_str


def _format_force_null(column_names):
    column_str = ', '.join(column_names)
    force_null_str = 'FORCE_NULL ({})'.format(column_str)
    return force_null_str
