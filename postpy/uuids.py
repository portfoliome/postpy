"""Configure psycopg2 to support UUID conversion."""

import psycopg2.extras

from postpy.admin import install_extensions


CRYPTO_EXTENSION = 'pgcrypto'
UUID_OSSP_EXTENSION = 'uuid-ossp'


def register_client():
    """Have psycopg2 marshall UUID objects automatically."""

    psycopg2.extras.register_uuid()


def register_crypto():
    """Support for UUID's on server side.

    Lighter dependency than uuid-ossp supporting
    random_uuid_function for UUID generation.
    """

    install_extensions([CRYPTO_EXTENSION])


def register_uuid():
    """Support for UUID's on server side.

    Notes
    -----
    uuid-ossp can be problematic on some platforms. See:
    https://www.postgresql.org/docs/current/static/uuid-ossp.html
    """

    install_extensions([UUID_OSSP_EXTENSION])


def random_uuid_function(schema=None):
    """Cryptographic random UUID function.

    Generates random database side UUID's.

    Notes
    -----
    Lighter dependency than uuid-ossp, but higher
    fragmentation on disk if used as auto-generating primary key UUID.
    """

    return '{}gen_random_uuid()'.format(_format_schema(schema))


def uuid_sequence_function(schema=None):
    """Sequential UUID generation.

    Sequential UUID creation on database side offering
    less table fragmentation issues when used as UUID primary key.
    """

    return '{}uuid_generate_v1mc()'.format(_format_schema(schema))


def _format_schema(schema):
    return '{}.'.format(schema) if schema else ''
