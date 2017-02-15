"""Copy specific dml statement generators."""

from postpy.pg_encodings import get_postgres_encoding


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
