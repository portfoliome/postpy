import unittest

from postpy.base import (Schema, Column, Table, PrimaryKey, View,
                         make_delete_table, order_table_columns,
                         split_qualified_name)
from postpy.fixtures import PostgreSQLFixture, PostgresStatementFixture


def table_columns():
    columns = [
        Column(name='city', data_type='VARCHAR(50)', nullable=False),
        Column(name='state', data_type='CHAR(2)', nullable=False),
        Column(name='population', data_type='INTEGER', nullable=True)
    ]
    return columns


def table_primary_keys():
    return PrimaryKey(['city', 'state'])


class TestColumn(PostgresStatementFixture, unittest.TestCase):
    def setUp(self):
        self.column_name = 'brand'
        self.data_type = 'VARCHAR(20)'

    def test_create_column_statement(self):
        column = Column(self.column_name, self.data_type)

        expected = 'brand VARCHAR(20) NOT NULL,'
        result = column.create_statement()

        self.assertSQLStatementEqual(expected, result)

    def test_create_null_column_statement(self):
        column = Column(self.column_name, self.data_type, nullable=True)

        expected = 'brand VARCHAR(20) NULL,'
        result = column.create_statement()

        self.assertSQLStatementEqual(expected, result)


class TestPrimaryKey(PostgresStatementFixture, unittest.TestCase):
    def test_primary_key_create(self):
        pkey = PrimaryKey(['brand', 'item'])

        expected = 'PRIMARY KEY (brand, item)'
        result = pkey.create_statement()

        self.assertSQLStatementEqual(expected, result)


class TestSchemaStatements(PostgresStatementFixture, unittest.TestCase):
    def setUp(self):
        self.schema_name = 'test_schema'
        self.schema = Schema(self.schema_name)

    def test_create_schema_statement(self):
        expected = 'CREATE SCHEMA IF NOT EXISTS test_schema;'
        result = self.schema.create_statement()

        self.assertSQLStatementEqual(expected, result)

    def test_drop_schema_statement(self):
        expected = 'DROP SCHEMA IF EXISTS test_schema CASCADE;'
        result = self.schema.drop_statement()

        self.assertSQLStatementEqual(expected, result)


class TestTableDDL(PostgresStatementFixture, unittest.TestCase):
    def setUp(self):
        self.schema = 'ddl_schema'
        self.tablename = 'create_table_test'
        self.qualified_name = 'ddl_schema.create_table_test'
        self.column_statement = ("city VARCHAR(50) NOT NULL,"
                                 " state CHAR(2) NOT NULL,"
                                 " population INTEGER NULL,")
        self.primary_key_statement = 'PRIMARY KEY (city, state)'
        self.columns = table_columns()
        self.primary_keys = table_primary_keys()

        self.table = Table(self.tablename, self.columns, self.primary_keys, schema=self.schema)

    def test_drop_table_statement(self):
        expected = 'DROP TABLE IF EXISTS ddl_schema.create_table_test;'
        result = self.table.drop_statement()

        self.assertSQLStatementEqual(expected, result)

    def test_column_statement(self):
        expected = 'city VARCHAR(50) NOT NULL, state CHAR(2) NOT NULL, population INTEGER NULL,'
        result = self.table.column_statement

        self.assertSQLStatementEqual(expected, result)

    def test_primary_key_statement(self):
        expected = 'PRIMARY KEY (city, state)'
        result = self.table.primary_key_statement

        self.assertSQLStatementEqual(expected, result)

    def test_primary_key_columns(self):
        expected = ['city', 'state']
        result = self.table.primary_key_columns

        self.assertEqual(expected, result)

    def test_column_names(self):
        expected = ['city', 'state', 'population']
        result = self.table.column_names

        self.assertEqual(expected, result)

    def test_create_statement(self):
        expected = ('CREATE TABLE ddl_schema.create_table_test ('
                    'city VARCHAR(50) NOT NULL, '
                    'state CHAR(2) NOT NULL, '
                    'population INTEGER NULL, '
                    'PRIMARY KEY (city, state));')
        result = self.table.create_statement()

        self.assertSQLStatementEqual(expected, result)

    def test_create_temporary_statement(self):
        temp_table = Table(self.tablename, self.columns, self.primary_keys)

        expected = ('CREATE TEMPORARY TABLE create_table_test ('
                    'city VARCHAR(50) NOT NULL, '
                    'state CHAR(2) NOT NULL, '
                    'population INTEGER NULL, '
                    'PRIMARY KEY (city, state));')
        result = temp_table.create_temporary_statement()

        self.assertSQLStatementEqual(expected, result)

    def test_split_qualified_name(self):
        expected = self.schema, self.tablename
        result = split_qualified_name(self.qualified_name)

        self.assertEqual(expected, result)

        expected = 'public', self.tablename
        result = split_qualified_name(self.tablename)

        self.assertEqual(expected, result)




class TestCreateTableEvent(PostgreSQLFixture, unittest.TestCase):
    def setUp(self):
        self.tablename = 'create_table_event'
        self.columns = table_columns()
        self.primary_key = table_primary_keys()
        self.table_query = "select relname from pg_stat_user_tables where relname=%s;"
        self.table = Table(self.tablename, self.columns, self.primary_key)

    def test_create_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute(self.table.create_statement())
        self.conn.commit()

        with self.conn.cursor() as cursor:
            cursor.execute(self.table_query, (self.tablename,))
            table = cursor.fetchone()

        self.assertEqual((self.tablename,), table)

    def test_temporary_table(self):
        with self.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.table.create_temporary_statement())
                cursor.execute(self.table_query, (self.tablename,))
                temp_table = cursor.fetchone()
                cursor.execute(self.table.drop_temporary_statement())
                cursor.execute(self.table_query, (self.tablename,))
                no_table = cursor.fetchone()

        self.assertEqual((self.tablename,), temp_table)
        self.assertTrue(no_table is None)

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS {table};'.format(
                table=self.tablename))
        self.conn.commit()


class TestViewStatements(PostgresStatementFixture, unittest.TestCase):
    def setUp(self):
        self.name = 'test_view'
        self.statement = '(select * from other_table)'
        self.view = View(self.name, self.statement)

    def test_create_statement(self):
        expected = 'CREATE VIEW test_view AS (select * from other_table);'
        result = self.view.create_statement()

        self.assertSQLStatementEqual(expected, result)

    def test_drop_statement(self):
        expected = 'DROP VIEW IF EXISTS test_view;'
        result = self.view.drop_statement()

        self.assertSQLStatementEqual(expected, result)


class TestMakeDeleteTable(unittest.TestCase):
    def setUp(self):
        self.delete_prefix = 'delete_from__'
        self.primary_key_column = Column(name='city',
                                         data_type='VARCHAR(50)',
                                         nullable=False)
        self.columns = [self.primary_key_column, Column(name='population',
                                                        data_type='INTEGER',
                                                        nullable=True)]
        self.primary_key = PrimaryKey(['city'])
        self.tablename = 'original_table'
        self.schema = 'to_delete'
        self.table = Table(self.tablename, self.columns,
                           self.primary_key, self.schema)

    def test_make_delete_table(self):
        result = make_delete_table(self.table, delete_prefix=self.delete_prefix)

        self.assertEqual(self.delete_prefix + self.tablename, result.name)
        self.assertEqual([self.primary_key_column], result.columns)
        self.assertEqual(self.primary_key, result.primary_key)


class TestReOrderTableColumns(unittest.TestCase):
    def setUp(self):
        self.schema = 'foo'
        self.table_name = 'foobar'
        columns = table_columns()
        primary_key = table_primary_keys()
        self.table = Table(self.table_name, columns, primary_key, self.schema)

    def reorder_columns(self):
        column_names = ['state', 'city', 'population']

        expect_columns = [Column('state', 'CHAR(2)', False),
                          Column('city', 'VARCHAR(50)', False),
                          Column('population', 'INTEGER', True)]
        expect_pkey = PrimaryKey(['state', 'city'])

        expected = Table(self.table_name, expect_columns, expect_pkey, self.schema)
        result = order_table_columns(self.table, column_names)

        self.assertEqual(expected, result)
