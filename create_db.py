import os
import sys

import psycopg2
from psycopg2 import connect


def main():
    db_name = os.environ['PGDATABASE']
    connection_parameters = {
        'host': os.environ['PGHOST'],
        'database': db_name,
        'user': os.environ['PGUSER'],
        'password': os.environ['PGPASSWORD']
    }
    drop_statement = 'DROP DATABASE IF EXISTS {};'.format(db_name)
    ddl_statement = 'CREATE DATABASE {};'.format(db_name)
    conn = connect(**connection_parameters)
    conn.autocommit = True

    try:
        with conn.cursor() as cursor:
            cursor.execute(drop_statement)
            cursor.execute(ddl_statement)
        conn.close()
        sys.stdout.write('Created database environment successfully.\n')
    except psycopg2.Error:
        raise SystemExit(
            'Failed to setup Postgres environment.\n{0}'.format(sys.exc_info())
        )


if __name__ == '__main__':
    main()
