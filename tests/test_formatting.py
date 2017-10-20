import unittest

from postpy.formatting import pyformat_parameters, named_style_parameters


class TestParamFormatting(unittest.TestCase):

    def setUp(self):
        self.column_names = ['foo', 'bar', 'foobar']

    def test_pyformat_parameters(self):
        expected = '%s, %s, %s'
        result = pyformat_parameters(self.column_names)

        self.assertEqual(expected, result)

    def test_compile_truncate_table(self):
        expected = '%(foo)s, %(bar)s, %(foobar)s'
        result = named_style_parameters(self.column_names)

        self.assertEqual(expected, result)
