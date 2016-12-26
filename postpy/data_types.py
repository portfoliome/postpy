from datetime import date, datetime
from decimal import Decimal
from types import MappingProxyType

from foil.compose import create_quantiles
from psycopg2.extras import NumericRange


TYPE_MAP = MappingProxyType({
    'bool': bool,
    'boolean': bool,
    'smallint': int,
    'integer': int,
    'bigint': int,
    'real': float,
    'float': float,
    'double precision': float,
    'decimal': Decimal,
    'numeric': Decimal,
    'char': str,
    'character': str,
    'text': str,
    'varchar': str,
    'character varying': str,
    'date': date,
    'timestamp': datetime
})


def generate_numeric_range(items, lower_bound, upper_bound):
    """Generate postgresql numeric range and label for insertion.

    Parameters
    ----------
    items: iterable labels for ranges.
    lower_bound: numeric lower bound
    upper_bound: numeric upper bound
    """

    quantile_grid = create_quantiles(items, lower_bound, upper_bound)
    labels, bounds = (zip(*quantile_grid))
    ranges = ((label, NumericRange(*bound))
              for label, bound in zip(labels, bounds))
    return ranges
