import os
import psycopg2


__all__ = ('connect',)


def connect(host=None, database=None, user=None, password=None, **kwargs):
    """Create a database connection."""

    host = host or os.environ['PGHOST']
    database = database or os.environ['PGDATABASE']
    user = user or os.environ['PGUSER']
    password = password or os.environ['PGPASSWORD']

    return psycopg2.connect(host=host,
                            database=database,
                            user=user,
                            password=password,
                            **kwargs)
