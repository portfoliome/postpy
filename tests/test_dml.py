import io
import textwrap
import unittest
from collections import namedtuple
from datetime import date

from postpy.base import Table, Column, PrimaryKey
from postpy import dml
from postpy.fixtures import (PostgresStatementFixture, skipPGVersionBefore,
                             get_records, PG_UPSERT_VERSION, PostgresDmlFixture,
                             fetch_one_result)


def make_records():
    columns = ['city', 'state']
    Record = namedtuple('Record', columns)
    records = [Record('Chicago', 'IL'),
               Record('New York', 'NY'),
               Record('Zootopia', None),
               Record('Miami', 'FL')]
    return columns, records


def delimited_text():
    file_content = textwrap.dedent("""
"city"|"state"
"Chicago"|"IL"
"New York"|"NY"
"Zootopia"|""
"Miami"|"FL"
""").strip()

    return file_content


class TestStatementFormatting(PostgresStatementFixture, unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_create_insert_statement(self):
        columns = ['one']
        qualified_name = 'tname'

        expected = "INSERT INTO tname (one) VALUES (%s)"
        result = dml.create_insert_statement(qualified_name, columns)

        self.assertSQLStatementEqual(expected, result)

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
                    "    ENCODING 'latin1')")
        result = dml.copy_from_csv_sql(table, delimiter=delimiter, null_str='NULL',
                                       header=False, encoding=encoding,
                                       force_not_null=force_not_null)

        self.assertSQLStatementEqual(expected, result)

    def test_compile_truncate_table(self):
        qualified_name = 'my_schema.my_table'

        expected = 'TRUNCATE my_schema.my_table CASCADE;'
        result = dml.compile_truncate_table(qualified_name)

        self.assertSQLStatementEqual(expected, result)


class TestInsertRecords(PostgresDmlFixture, unittest.TestCase):

    def setUp(self):
        self.columns, self.records = make_records()
        self.table_name = 'insert_record_table'
        create_table_stmt = """CREATE TABLE {table} (
                                 city VARCHAR(50),
                                 state char(2) NULL,
                                 PRIMARY KEY (city));
                            """.format(table=self.table_name)

        with self.conn.cursor() as cursor:
            cursor.execute(create_table_stmt)
            self.conn.commit()

    def test_insert_namedtuple(self):
        dml.insert(self.conn, self.table_name, self.columns, self.records)

        expected = self.records
        result = get_records(self.conn, self.table_name)

        self.assertEqual(expected, result)

    def test_insert_many_namedtuples(self):
        dml.insert_many(self.conn, self.table_name, self.columns,
                        self.records, chunksize=2)

        expected = self.records
        result = get_records(self.conn, self.table_name)

        self.assertEqual(expected, result)

    def test_insert_many_tuples(self):
        records = [record[:] for record in self.records]
        dml.insert_many(self.conn, self.table_name,
                        self.columns, records, chunksize=4)

        expected = self.records
        result = get_records(self.conn, self.table_name)

        self.assertEqual(expected, result)

    def test_copy_from_csv(self):
        self.columns, self.records = make_records()
        file_object = io.StringIO(delimited_text())

        dml.copy_from_csv(self.conn, file_object, self.table_name, '|',
                          force_null=['state'], encoding='utf-8', null_str='')

        result = get_records(self.conn, self.table_name)

        self.assertEqual(self.records, result)


class TestUpsert(PostgresDmlFixture, unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.table_name = 'upsert_test1'
        self.column_names = ['ticker', 'report_date', 'score']

        with self.conn.cursor() as cursor:
            cursor.execute("""\
                          CREATE TABLE {tablename} (
                            ticker CHAR(4),
                            report_date DATE,
                            score INT,
                            PRIMARY KEY(ticker));""".format(tablename=self.table_name))
        self.conn.commit()
        constraint = ['ticker']
        clause = 'WHERE current.report_date < EXCLUDED.report_date'
        self.upsert_statement = dml.format_upsert(self.table_name,
                                                  self.column_names,
                                                  constraint,
                                                  clause)
        self.result_query = "select * from {tablename} where ticker='AAPL'".format(
            tablename=self.table_name)

    @skipPGVersionBefore(*PG_UPSERT_VERSION)
    def test_new_record(self):
        from datetime import date
        expected = ('AAPL', date(2014, 4, 1), 5)
        dml.upsert_records(self.conn, [expected], self.upsert_statement)

        result = fetch_one_result(self.conn, self.result_query)

        self.assertEqual(expected, result)

    @skipPGVersionBefore(*PG_UPSERT_VERSION)
    def test_conflict_replace_record(self):
        first = ('AAPL', date(2014, 4, 1), 5)
        expected = ('AAPL', date(2015, 4, 1), 5)
        dml.upsert_records(self.conn, [first], self.upsert_statement)
        dml.upsert_records(self.conn, [expected], self.upsert_statement)

        result = fetch_one_result(self.conn, self.result_query)

        self.assertEqual(expected, result)

    @skipPGVersionBefore(*PG_UPSERT_VERSION)
    def test_conflict_no_update(self):
        expected = ('AAPL', date(2014, 4, 1), 5)
        second = ('AAPL', date(2013, 4, 1), 5)
        dml.upsert_records(self.conn,  [expected], self.upsert_statement)
        dml.upsert_records(self.conn,  [second], self.upsert_statement)

        result = fetch_one_result(self.conn, self.result_query)

        self.assertEqual(expected, result)

    @skipPGVersionBefore(*PG_UPSERT_VERSION)
    def test_update_on_primary_key(self):
        primary_keys = ['ticker']
        upserter = dml.UpsertPrimaryKey(self.table_name, self.column_names,
                                        primary_keys)
        first = ('AAPL', date(2014, 4, 1), 5)
        expected = ('AAPL', date(2014, 4, 1), 6)

        upserter(self.conn, [first])
        upserter(self.conn, [expected])

        result = fetch_one_result(self.conn, self.result_query)

        self.assertEqual(expected, result)


class TestUpsertPrimary(PostgresStatementFixture, unittest.TestCase):

    def setUp(self):
        self.table_name = 'foobar'
        self.columns = ['foo', 'bar']
        self.primary_keys = self.columns

    def test_when_all_columns_are_primary_keys(self):
        upserter = dml.UpsertPrimaryKey(self.table_name,
                                        self.columns,
                                        self.primary_keys)

        expected = 'INSERT INTO foobar (foo, bar) VALUES (%s, %s)'
        result = upserter.query

        self.assertSQLStatementEqual(expected, result)


class TestBulkUpsertPrimaryKey(PostgresDmlFixture, unittest.TestCase):

    def setUp(self):
        self.table_name = 'insert_record_table'
        self.column_names, self.records = make_records()
        self.columns = [Column(self.column_names[0], 'VARCHAR(50)'),
                        Column(self.column_names[1], 'CHAR(2)', nullable=True)]
        self.primary_key_names = ['city']
        self.primary_key = PrimaryKey(self.primary_key_names)
        self.table = Table(self.table_name, self.columns, self.primary_key)
        self.delimiter = '|'
        self.force_null = ['state']
        self.null_str = ''

        self.insert_query = 'INSERT INTO {} VALUES (%s, %s)'.format(
            self.table_name
        )

        records = [('Miami', 'TX'), ('Chicago', 'MI')]

        with self.conn.cursor() as cursor:
            cursor.execute(self.table.create_statement())
            cursor.executemany(self.insert_query, records)
            self.conn.commit()

    @skipPGVersionBefore(*PG_UPSERT_VERSION)
    def test_bulk_upsert(self):
        bulk_upserter = dml.BulkUpsertPrimaryKey(
            self.table,
            delimiter=self.delimiter, null_str=self.null_str,
            force_null=self.force_null
        )
        file_object = io.StringIO(delimited_text())

        bulk_upserter(self.conn, file_object)

        result = get_records(self.conn, self.table_name)

        self.assertEqual(self.records, result)


class TestDeleteRecordStatements(PostgresStatementFixture, unittest.TestCase):

    def test_delete_joined_table_sql(self):
        table_name = 'table_foo'
        delete_table = 'delete_from_foo'
        primary_key = ['city', 'state']

        expected = (
            'DELETE FROM table_foo t'
            ' USING delete_from_foo d'
            ' WHERE t.city=d.city AND t.state=d.state')
        result = dml.delete_joined_table_sql(table_name, delete_table, primary_key)
        self.assertSQLStatementEqual(expected,  result)


class TestDeletePrimaryKeyRecords(PostgresDmlFixture, unittest.TestCase):

    def setUp(self):
        self.column_names, self.records = make_records()
        self.primary_key_name = self.column_names[0]
        self.delete_records = [(self.records[0].city,),
                               (self.records[2].city,),
                               (self.records[3].city,)]
        self.table_name = 'insert_test'
        self.columns = [Column(name='city', data_type='VARCHAR(50)',
                               nullable=False),
                        Column(name='state', data_type='CHAR(2)',
                               nullable=True)]
        self.primary_key = PrimaryKey([self.primary_key_name])
        self.table = Table(self.table_name, self.columns, self.primary_key)
        self._setup_table_data()
        self.delete_processor = dml.DeleteManyPrimaryKey(self.table)

    def test_process_delete_insert(self):
        self.delete_processor(self.conn, self.delete_records)

        expected = set([self.records[1]])
        result = set(get_records(self.conn, self.table.qualified_name))

        self.assertSetEqual(expected, result)

    def _setup_table_data(self):
        insert_statement = 'INSERT INTO insert_test (city, state) VALUES (%s, %s)'
        with self.conn.cursor() as cursor:
            cursor.execute(self.table.create_statement())

            for record in self.records:
                cursor.execute(insert_statement, record)
            self.conn.commit()
