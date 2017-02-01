import unittest

import psycopg2

from postpy.fixtures import PostgreSQLFixture
from postpy.extensions import check_extension


class TestExtensions(PostgreSQLFixture, unittest.TestCase):
    @classmethod
    def _prep(cls):
        cls.pg_extension = 'sslinfo'

    def test_check_no_extension(self):
        with self.assertRaises(psycopg2.ProgrammingError):
            check_extension(self.conn, 'fake_extension')

    def test_check_uninstalled_extension(self):

        self.assertFalse(check_extension(self.conn, self.pg_extension))
