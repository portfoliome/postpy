import unittest
import psycopg2
from collections import namedtuple

from postpy import sql
from .common import PostgreSQLFixture


TABLE_QUERY = ("select table_name from information_schema.tables"
               " where table_schema = 'public';")


class TestExecute(PostgreSQLFixture, unittest.TestCase):

    def test_execute_transactions(self):
        table = "execute_transactions"
        reset_query = "DROP TABLE IF EXISTS execute_transactions;"
        mock_statements = ["CREATE TABLE execute_transactions();"]

        sql.execute_transactions(self.conn, mock_statements)

        with self.conn.cursor() as cursor:
            cursor.execute(TABLE_QUERY)

            expected = (table,)
            result = cursor.fetchone()

            self.assertEqual(expected, result)
            cursor.execute(reset_query)

        self.conn.commit()

    def test_execute_transaction(self):
        table = "execute_transaction"
        reset_query = "DROP TABLE IF EXISTS execute_transaction;"
        mock_statements = ["CREATE TABLE execute_transaction();"]

        sql.execute_transaction(self.conn, mock_statements)

        with self.conn.cursor() as cursor:
            cursor.execute(TABLE_QUERY)

            expected = (table,)
            result = cursor.fetchone()

            self.assertEqual(expected, result)
            cursor.execute(reset_query)

        self.conn.commit()

    def test_doesnt_raise_exception(self):
        query = ["insert nothing into nothing"]
        try:
            sql.execute_transactions(self.conn, query)
        except psycopg2.ProgrammingError:
            self.fail('Raised DB Programming Error')


class TestClosingTransaction(PostgreSQLFixture, unittest.TestCase):
    def test_execute_closing_transaction(self):
        statements = [
            'CREATE TABLE close_foo();',
            'DROP TABLE close_foo;']
        sql.execute_closing_transaction(statements)

        with self.conn.cursor() as cursor:
            cursor.execute(TABLE_QUERY)

            expected = None
            result = cursor.fetchone()

            self.assertEqual(expected, result)


class TestSelectQueries(PostgreSQLFixture, unittest.TestCase):

    def setUp(self):
        self.query = 'select * from generate_series(1,3) as col1;'

    def test_select_dict(self):

        expected = [{'col1': 1}, {'col1': 2}, {'col1': 3}]
        result = list(sql.select_dict(self.conn, self.query))

        self.assertEqual(expected, result)

    def test_select(self):
        Record = namedtuple('Record', 'col1')

        expected = [Record(col1=1), Record(col1=2), Record(col1=3)]
        result = list(sql.select(self.conn, self.query))

        self.assertEqual(expected, result)

    def test_query_columns(self):
        query = "SELECT 1 AS foo, 'cip' AS bar;"

        expected = ['foo', 'bar']
        result = list(sql.query_columns(self.conn, query))

        self.assertEqual(expected, result)
