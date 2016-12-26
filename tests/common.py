from unittest.util import safe_repr
from functools import wraps

from psycopg2.extras import NamedTupleCursor

from postpy.connections import connect


PG_UPSERT_VERSION = (9, 5)


class PostgreSQLFixture(object):

    @classmethod
    def setUpClass(cls):
        cls.conn = connect()
        cls._prep()

    @classmethod
    def _prep(cls):
        pass

    @classmethod
    def _clean(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        cls._clean()
        cls.conn.close()


class PostgresDmlFixture(PostgreSQLFixture):

    def tearDown(self):
        with self.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute('DROP TABLE IF EXISTS %s;' % self.table_name)
            self.conn.commit()
        self.conn.commit()


class PostgresStatementFixture(object):
    maxDiff = True

    def assertSQLStatementEqual(self, first, second, msg=None):
        if squeeze_whitespace(first) != squeeze_whitespace(second):
            standardMsg = 'SQL statement {0} != {1}'.format(
                safe_repr(first), safe_repr(second))
            self.fail(self._formatMessage(msg, standardMsg))


def get_records(conn, qualified_name):
    with conn:
        with conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
            cursor.execute('select * from %s;' % qualified_name)
            records = cursor.fetchall()
    return records


def fetch_one_result(conn, result_query):
    with conn.cursor() as cursor:
        cursor.execute(result_query)
        result = cursor.fetchone()
    return result


def skipPGVersionBefore(*ver):
    """Skip PG versions below specific version i.e. (9, 5)."""

    ver = ver + (0,) * (3 - len(ver))

    def skip_before_postgres_(func):
        @wraps(func)
        def skip_before_postgres__(obj, *args, **kwargs):

            if hasattr(obj.conn, 'server_version'):
                server_version = obj.conn.server_version
            else:  # Assume Sqlalchemy
                server_version = obj.conn.connection.connection.server_version

            if server_version < int('%d%02d%02d' % ver):
                return obj.skipTest("Skipped because PostgreSQL {}".format(
                    server_version))
            else:
                return func(obj, *args, **kwargs)
        return skip_before_postgres__
    return skip_before_postgres_


def squeeze_whitespace(text):
    """Remove extra whitespace, newline and tab characters from text."""

    return ' '.join(text.split())
