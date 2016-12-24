import unittest

from pgdabble.admin import (get_user_tables, get_primary_keys,
                            get_column_metadata, reflect_table, reset)
from pgdabble.base import Database, Column, PrimaryKey, Table
from pgdabble.connections import connect
from .common import PostgreSQLFixture


class TestTableStats(PostgreSQLFixture, unittest.TestCase):

    @classmethod
    def _prep(cls):
        cls.conn.autocommit = True
        cls.schema = 'stats_test'
        cls.table = 'admin_table_tests'
        create_table_statement = """\
            CREATE TABLE {schema}.{table} (
              mycol CHAR(2),
              mycol2 CHAR(3) NULL,
              PRIMARY KEY (mycol));""".format(schema=cls.schema,
                                              table=cls.table)

        with cls.conn.cursor() as cursor:
            cursor.execute('CREATE SCHEMA {};'.format(cls.schema))
            cursor.execute(create_table_statement)

    def test_get_user_tables(self):

        expected = (self.schema, self.table)
        result = get_user_tables(self.conn)

        self.assertIn(expected, result)

    def test_get_column_meta_data(self):
        expected = [
            {'name': 'mycol',
             'data_type': 'character(2)',
             'nullable': False},
            {'name': 'mycol2',
             'data_type': 'character(3)',
             'nullable': True}
        ]
        result = list(
            get_column_metadata(self.conn, self.table, schema=self.schema)
        )

        self.assertEqual(expected, result)

    def test_get_primary_keys(self):
        expected = ['mycol']
        result = list(get_primary_keys(self.conn, self.table, self.schema))

        self.assertEqual(expected, result)

    def test_reflect_table(self):
        columns = [Column('mycol', data_type='character(2)', nullable=False),
                   Column('mycol2', data_type='character(3)', nullable=True)]
        primary_key = PrimaryKey(['mycol'])

        expected = Table(self.table, columns, primary_key, schema=self.schema)
        result = reflect_table(self.conn, self.table, self.schema)

        self.assertEqual(expected, result)

    @classmethod
    def _clean(cls):
        statement = 'DROP SCHEMA IF EXISTS {} CASCADE;'.format(cls.schema)

        with cls.conn.cursor() as cursor:
            cursor.execute(statement)


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.db = Database('reset_db_test')
        self.db_query = """SELECT datname
                           FROM pg_database
                           WHERE datistemplate=false;"""
        self.conn = connect()
        self.conn.autocommit = True

    def test_reset(self):
        reset(self.db.name)

        with self.conn.cursor() as cursor:
            cursor.execute(self.db_query)
            result = [item[0] for item in cursor.fetchall()]

        self.assertIn(self.db.name, result)

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute(self.db.drop_statement())

        self.conn.close()
