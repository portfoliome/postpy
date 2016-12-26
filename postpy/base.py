from collections import namedtuple

from foil.formatters import format_repr
from foil.filters import create_indexer

from postpy.ddl import (
    compile_column, compile_qualified_name, compile_primary_key,
    compile_create_table, compile_create_temporary_table
)


__all__ = ('Database', 'Schema', 'Table', 'Column', 'PrimaryKey', 'View')


class Database:
    __slots__ = 'name',

    def __init__(self, name):
        self.name = name

    def create_statement(self):
        return 'CREATE DATABASE %s;' % self.name

    def drop_statement(self):
        return 'DROP DATABASE IF EXISTS %s;' % self.name

    def __repr__(self):
        return format_repr(self, self.__slots__)


class Schema:
    __slots__ = 'name',

    def __init__(self, name):
        self.name = name

    def create_statement(self):
        return 'CREATE SCHEMA IF NOT EXISTS %s;' % self.name

    def drop_statement(self):
        return 'DROP SCHEMA IF EXISTS %s CASCADE;' % self.name

    def __repr__(self):
        return format_repr(self, self.__slots__)


class Table(namedtuple('Table', 'name columns primary_key schema')):
    """Table statement formatter."""

    __slots__ = ()

    def __new__(cls, name: str, columns, primary_key, schema='public'):
        return super(Table, cls).__new__(cls, name, columns,
                                         primary_key,
                                         schema)

    def create_statement(self):
        return compile_create_table(self.qualified_name,
                                    self.column_statement,
                                    self.primary_key_statement)

    def drop_statement(self):
        return 'DROP TABLE IF EXISTS {};'.format(self.qualified_name)

    def create_temporary_statement(self):
        """Temporary Table Statement formatter."""

        return compile_create_temporary_table(self.name,
                                              self.column_statement,
                                              self.primary_key_statement)

    def drop_temporary_statement(self):
        return 'DROP TABLE IF EXISTS {};'.format(self.name)

    @property
    def qualified_name(self):
        return compile_qualified_name(self.name, schema=self.schema)

    @property
    def column_names(self):
        return [column.name for column in self.columns]

    @property
    def primary_key_columns(self):
        return self.primary_key.column_names

    @property
    def column_statement(self):
        return ' '.join(c.create_statement() for c in self.columns)

    @property
    def primary_key_statement(self):
        return self.primary_key.create_statement()


class Column(namedtuple('Column', 'name data_type nullable')):
    __slots__ = ()

    def __new__(cls, name: str, data_type: str, nullable=False):
        return super(Column, cls).__new__(cls, name, data_type, nullable)

    def create_statement(self):
        return compile_column(self.name, self.data_type, self.nullable)


class PrimaryKey(namedtuple('PrimaryKey', ['column_names'])):
    __slots__ = ()

    def __new__(cls, column_names: list):
        return super(PrimaryKey, cls).__new__(cls, column_names)

    def create_statement(self):
        return compile_primary_key(self.column_names)


class View:
    """Postgresql View statement formatter.

    Attributes
    ----------
    name : view name
    statement: the select or join statement the view is based on.

    """

    def __init__(self, name: str, statement: str):
        self.name = name
        self.statement = statement

    def drop_statement(self):
        return 'DROP VIEW IF EXISTS {};'.format(self.name)

    def create_statement(self):
        return 'CREATE VIEW {name} AS {statement};'.format(
            name=self.name, statement=self.statement)


def make_delete_table(table: Table, delete_prefix='delete_from__') -> Table:
    """Table referencing a delete from using primary key join."""

    name = delete_prefix + table.name
    primary_key = table.primary_key
    key_names = set(primary_key.column_names)
    columns = [column for column in table.columns if column.name in key_names]
    table = Table(name, columns, primary_key)

    return table


def split_qualified_name(qualified_name: str, schema='public'):
    if '.' in qualified_name:
        schema, table = qualified_name.split('.')
    else:
        table = qualified_name

    return schema, table


def order_table_columns(table: Table, column_names: list) -> Table:
    """Record table column(s) and primary key columns by specified order."""

    unordered_columns = table.column_names
    index_order = [unordered_columns.index(name) for name in column_names]
    indexer = create_indexer(index_order)
    ordered_columns = indexer(table.columns)
    ordered_pkey_names = [column for column in column_names
                          if column in table.primary_key_columns]
    primary_key = PrimaryKey(ordered_pkey_names)

    return Table(table.name, ordered_columns, primary_key, table.schema)
