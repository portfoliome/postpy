from encodings import normalize_encoding, aliases
from types import MappingProxyType

from psycopg2.extensions import encodings as _PG_ENCODING_MAP


PG_ENCODING_MAP = MappingProxyType(_PG_ENCODING_MAP)

# python to postgres encoding map
_PYTHON_ENCODING_MAP = {
    v: k for k, v in PG_ENCODING_MAP.items()
}


def get_postgres_encoding(python_encoding: str) -> str:
    """Python to postgres encoding map."""

    encoding = normalize_encoding(python_encoding.lower())
    encoding_ = aliases.aliases[encoding.replace('_', '', 1)].upper()
    pg_encoding = PG_ENCODING_MAP[encoding_.replace('_', '')]

    return pg_encoding
