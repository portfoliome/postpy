import unittest

from postpy.data_types import generate_numeric_range, NumericRange


class TestNumericRange(unittest.TestCase):
    def test_generate_numeric_range(self):
        items = ('freezing', 'cold', 'cool', 'hot')
        lower_bound = 0.
        upper_bound = 100.

        expected = [
            ('freezing', NumericRange(0., 25.)),
            ('cold', NumericRange(25., 50.)),
            ('cool', NumericRange(50., 75.)),
            ('hot', NumericRange(75., 100.)),
        ]
        result = list(generate_numeric_range(items, lower_bound, upper_bound))

        self.assertEqual(expected, result)
