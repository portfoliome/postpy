import unittest

from postpy import uuids


class TestUUIDFunctions(unittest.TestCase):

    def test_function_formatters(self):

        expected = 'gen_random_uuid()'
        result = uuids.random_uuid_function()

        self.assertEqual(expected, result)

        expected = 'my_schema.uuid_generate_v1mc()'
        result = uuids.uuid_sequence_function('my_schema')

        self.assertEqual(expected, result)
