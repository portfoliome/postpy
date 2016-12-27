[![Build Status](https://travis-ci.org/portfoliome/postpy.svg?branch=master)](https://travis-ci.com/portfoliome/postpy)
[![codecov.io](http://codecov.io/github/portfoliome/postpy/coverage.svg?branch=master)](http://codecov.io/github/portfoliome/postpy?branch=master)
[![Code Health](https://landscape.io/github/portfoliome/postpy/master/landscape.svg?style=flat)](https://landscape.io/github/portfoliome/postpy/master)

# postpy
Postgresql utilities for ETL and data analysis.

# Purpose
postpy focuses on processes that typically arise from ETL processes and data analysis. Generally, these situtations arise when third-party data providers provide a default schema and handle data migration. The benefits over sqlalchemy are dml statements accepting iterable sequences, and upsert statements prior to sqlalchemy 1.1. While the library protects against SQL injection, ddl compiler functions do not check against things like reserved keywords.

# Example Usage

Let's say a third-party provider has given you a JSON schema file, all referring to different zipped data files.

Mocking out a single file load might look something like:

```python
import csv

from foil.fileio import DelimitedReader
from foil.parsers import parse_str, parse_int, passthrough

from postpy import dml

ENCODING = 'utf-8'

class DataDialect(csv.Dialect):
    delimiter = '|'
    quotechar = '"'
    lineterminator = '\r\n'
    doublequote = False
    quoting = csv.QUOTE_NONE

dialect = DataDialect()

# Gathering table/file attributes

tablename = 'my_table'
fields = DelimitedReader.from_zipfile(zip_path, filename, encoding=ENCODING,
                                      dialect=dialect, fields=[], converters=[]).header
field_parsers = [parse_str, parse_int, passthrough, parse_it]  # would get through reflection or JSON file

# loading one file and insert
reader = DelimitedReader.from_zipfile(zip_path, filename, encoding=ENCODING,
                                      dialect=dialect, fields=fields, converters=field_parsers)

# Insert records by loading only 10,000 records/file lines into memory each iteration
dml.insert_many(conn, tablename, fields, records=reader, chunksize=10000)
```

Since each process is very light-weight, each loader can reside on a micro-instance. Queues like RabbitMQ or SNS/SQS can be setup to handle message notifications between each process.

Instead of worrying about async/threads, each miro-instance can handle a single table load and pass off a message upon completion.

# Potential Near-term Plans
The ddl compilers maybe converted to sqlalchemy compilers to allow for greater flexibility in constraint definitions without adding code maintainability. Python 3.6's f-strings may be incorporated into the ddl compilers, breaking 3.5 compatibility.
