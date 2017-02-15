import unittest

from postpy.fixtures import PostgresStatementFixture
from postpy.dml_copy import copy_from_csv_sql


class TestDmlCopyStatements(PostgresStatementFixture, unittest.TestCase):
    def test_copy_from_csv_sql(self):
        table = 'my_table'
        delimiter = '|'
        encoding = 'latin1'
        force_not_null = ['foo', 'bar']

        expected = ("COPY my_table FROM STDIN"
                    "  WITH ("
                    "    FORMAT CSV,"
                    "    DELIMITER '|',"
                    "    NULL 'NULL',"
                    "    QUOTE '\"',"
                    "    ESCAPE '\\',"
                    "    FORCE_NOT_NULL (foo, bar),"
                    "    ENCODING 'iso8859_1')")
        result = copy_from_csv_sql(table, delimiter=delimiter, null_str='NULL',
                                   header=False, encoding=encoding,
                                   force_not_null=force_not_null)

        self.assertSQLStatementEqual(expected, result)
