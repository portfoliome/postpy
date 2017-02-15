import unittest

from postpy.pg_encodings import get_postgres_encoding


class TestPGEncodings(unittest.TestCase):
    def test_get_postgres_encoding(self):
        expected = 'utf_8'
        result = get_postgres_encoding('utf8')

        self.assertEqual(expected, result)
