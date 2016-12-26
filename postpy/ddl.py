"""
ddl.py contains the Data Definition Language for Postgresql Server.
"""

from psycopg2.extensions import AsIs


def compile_qualified_name(table: str, schema='public') -> str:
    """Format table's fully qualified name string."""

    return '{}.{}'.format(schema, table)


def compile_create_table(qualified_name: str, column_statement: str,
                         primary_key_statement: str) -> str:
    """Postgresql Create Table statement formatter."""

    statement = """
                CREATE TABLE {table} ({columns} {primary_keys});
                """.format(table=qualified_name,
                           columns=column_statement,
                           primary_keys=primary_key_statement)
    return statement


def compile_create_temporary_table(table_name: str,
                                   column_statement: str,
                                   primary_key_statement: str) -> str:
    """Postgresql Create Temporary Table statement formatter."""

    statement = """
                CREATE TEMPORARY TABLE {table} ({columns} {primary_keys});
                """.format(table=table_name,
                           columns=column_statement,
                           primary_keys=primary_key_statement)
    return statement


def compile_column(name: str, data_type: str, nullable: bool) -> str:
    """Create column definition statement."""

    null_str = 'NULL' if nullable else 'NOT NULL'

    return '{name} {data_type} {null},'.format(name=name,
                                               data_type=data_type,
                                               null=null_str)


def compile_primary_key(column_names):
    return 'PRIMARY KEY ({})'.format(', '.join(column_names))


class CreateTableAs:
    def __init__(self, table, parent_table, columns=('*',), *, clause):
        self.table = table
        self.parent_table = parent_table
        self.columns = columns
        self.column_str = ', '.join(columns)
        self.clause = clause

    def compile(self):
        statement = '{create} ({select} {clause})'.format(
            create=self._create_statement(),
            select=self._select_statement(),
            clause=self._clause_statement())

        return statement

    def compile_with_cte(self, common_table_expression):
        statement = '{create} (WITH {cte} {select} {clause})'.format(
            create=self._create_statement(),
            cte=common_table_expression,
            select=self._select_statement(),
            clause=self._clause_statement())

        return statement

    def _create_statement(self):
        return 'CREATE TABLE {} AS'.format(self.table)

    def _select_statement(self):
        return '\n  SELECT {column_str} \n  FROM {parent_table}'.format(
            column_str=self.column_str, parent_table=self.parent_table)

    def _clause_statement(self):
        return '\n  WHERE %s' % self.clause


class MaterializedView:
    """Postgres materialized view declaration formatter."""

    def __init__(self, name, query='', query_values=None):
        self.name = name
        self.query = query
        self.query_values = query_values

    def create(self, no_data=False):
        """Declare materalized view."""

        if self.query:
            ddl_statement = self.compile_create_as()
        else:
            ddl_statement = self.compile_create()

        if no_data:
            ddl_statement += '\nWITH NO DATA'

        return ddl_statement, self.query_values

    def compile_create(self):
        """Materalized view."""

        return 'CREATE MATERIALIZED VIEW {}'.format(AsIs(self.name))

    def compile_create_as(self):
        """Build from a select statement."""

        return '{} AS \n {}'.format(self.compile_create(), self.query)

    def refresh(self):
        """Refresh a materialized view."""

        return 'REFRESH MATERIALIZED VIEW {}'.format(AsIs(self.name))

    def drop(self):
        return 'DROP MATERIALIZED VIEW {}'.format(AsIs(self.name))
