"""Copy specific dml statement generators."""

from abc import ABC, abstractmethod

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


class CopyFromCsv(CopyFromCsvBase):
    """Copy from CSV file object."""

    def __call__(self, conn, file_object):
        with conn.cursor() as cursor:
            cursor.copy_expert(self.copy_sql, file_object)


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
