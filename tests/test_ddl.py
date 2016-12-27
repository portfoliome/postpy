import unittest

from postpy import ddl
from postpy.fixtures import PostgresStatementFixture


class TestCompileDDLStatements(PostgresStatementFixture, unittest.TestCase):
    def test_compile_qualified_name(self):
        schema = 'mockschema'
        tablename = 'mocktable'

        expected = 'mockschema.mocktable'
        result = ddl.compile_qualified_name(tablename, schema=schema)

        self.assertSQLStatementEqual(expected, result)

    def test_compile_column(self):
        expected = "c VARCHAR(45) NOT NULL,"
        result = ddl.compile_column('c', 'VARCHAR(45)', False)

        self.assertSQLStatementEqual(expected, result)

    def test_compile_null_column(self):
        expected = 'c INTEGER NULL,'
        result = ddl.compile_column('c', 'INTEGER', True)

        self.assertSQLStatementEqual(expected, result)

    def test_compile_primary_key(self):
        expected = 'PRIMARY KEY (c2)'
        result = ddl.compile_primary_key(['c2'])

        self.assertSQLStatementEqual(expected, result)

    def test_compile_create_table(self):
        expected = 'CREATE TABLE tname (c1 INTEGER NULL, PRIMARY KEY (c1));'
        result = ddl.compile_create_table(
            qualified_name='tname',
            column_statement='c1 INTEGER NULL,',
            primary_key_statement='PRIMARY KEY (c1)')

        self.assertSQLStatementEqual(expected, result)


class TestCreateTableAs(PostgresStatementFixture, unittest.TestCase):

    def setUp(self):
        table = 't'
        parent_table = 'p'
        columns = ['one', 'two']
        clause = "one=two"
        self.create = ddl.CreateTableAs(table, parent_table, columns, clause=clause)

    def test_compile(self):
        expected = "CREATE TABLE t AS (\nSELECT one, two FROM p WHERE one=two)"
        result = self.create.compile()

        self.assertSQLStatementEqual(expected, result)

    def test_compile_with_cte(self):
        cte = 't2 AS (SELECT * FROM t3)'

        expected = "CREATE TABLE t AS (WITH t2 AS (SELECT * FROM t3) SELECT one, two FROM p WHERE one=two)"
        result = self.create.compile_with_cte(cte)

        self.assertSQLStatementEqual(expected, result)


class TestMaterializedView(PostgresStatementFixture, unittest.TestCase):
    def setUp(self):
        self.name = 'my_view'
        self.query = 'SELECT * FROM FOOBAR'

    def test_compile_create(self):
        expected = 'CREATE MATERIALIZED VIEW my_view'
        result = ddl.MaterializedView(self.name).compile_create()

        self.assertSQLStatementEqual(expected, result)

    def test_create_as(self):
        expected = 'CREATE MATERIALIZED VIEW my_view AS SELECT * FROM FOOBAR'
        result = ddl.MaterializedView(self.name, self.query).create()[0]

        self.assertSQLStatementEqual(expected, result)

    def test_no_data(self):
        expected = 'CREATE MATERIALIZED VIEW my_view WITH NO DATA'
        result = ddl.MaterializedView(self.name).create(no_data=True)[0]

        self.assertSQLStatementEqual(expected, result)
