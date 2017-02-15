"""Copy specific dml statement generators."""

from abc import ABC, abstractmethod
from random import randint

from postpy.base import Table
from postpy.pg_encodings import get_postgres_encoding


class CopyFromCsvBase(ABC):
    def __init__(self, table, delimiter=',', encoding='utf8',
                 null_str='', header=True, escape_str='\\', quote_char='"',
                 force_not_null=None, force_null=None):
        self.table = table
        self.copy_table, self.copy_name = self.get_copy_table(self.table)
        self.copy_sql = copy_from_csv_sql(self.copy_name,
                                          delimiter, encoding,
                                          null_str=null_str, header=header,
                                          escape_str=escape_str,
                                          quote_char=quote_char,
                                          force_not_null=force_not_null,
                                          force_null=force_null)

    def get_copy_table(self, table):
        return table, table.qualified_name

    @abstractmethod
    def __call__(self, conn, file_object):
        NotImplemented


class BulkDmlPrimaryKey(CopyFromCsvBase):
    """Row record changes on primary key join."""

    RAND_MIN = 0
    RAND_MAX = 10000000
    _TEMP_FORMATTER = '{temp_prefix}_{random}_{table_name}'
    TEMP_PREFIX = ''

    def __init__(self, table, **kwargs):
        super().__init__(table, **kwargs)
        self.dml_query = self.make_dml_query()

    def __call__(self, conn, file_object):
        with conn.cursor() as cursor:
            cursor.execute(self.copy_table.create_temporary_statement())
            cursor.copy_expert(self.copy_sql, file_object)
            cursor.execute(self.dml_query)
            cursor.execute(self.copy_table.drop_temporary_statement())

    def get_copy_table(self, table):
        temp_table = self.make_temp_copy_table()
        qualified_name = temp_table.name

        return temp_table, qualified_name

    def make_temp_copy_table(self):
        temp_table_name = self.generate_temp_table_name()
        table_attributes = self.table._asdict()
        table_attributes['name'] = temp_table_name

        return Table(**table_attributes)

    def generate_temp_table_name(self):
        rand_char = randint(self.RAND_MIN, self.RAND_MAX)
        temp_table_name = self._TEMP_FORMATTER.format(
            temp_prefix=self.TEMP_PREFIX,
            table_name=self.table.name,
            random=rand_char
        )

        return temp_table_name

    @property
    def column_str(self):
        return ', '.join(self.table.column_names)

    @abstractmethod
    def make_dml_query(self):
        NotImplemented


def copy_from_csv_sql(qualified_name: str, delimiter=',', encoding='utf8',
                      null_str='', header=True, escape_str='\\', quote_char='"',
                      force_not_null=None, force_null=None):
    """Generate copy from csv statement."""

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

    postgres_encoding = get_postgres_encoding(encoding)
    options.append("ENCODING '%s'" % postgres_encoding)

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
