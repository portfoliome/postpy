from datetime import date, datetime
from decimal import Decimal
from types import MappingProxyType


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
