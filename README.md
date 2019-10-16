# jx-sqlite 

JSON query expressions using SQLite

## Motivation

JSON is a nice format to store data, and it has become quite prevalent. Unfortunately, databases do not handle it well, often a human is required to declare a schema that can hold the JSON before it can be queried. If we are not overwhelmed by the diversity of JSON now, we soon will be. There will be more JSON, of more different shapes, as the number of connected devices( and the information they generate) continues to increase.


## Synopsis

An attempt to store JSON documents in SQLite so that they are accessible via SQL. The hope is this will serve a basis for a general document-relational map (DRM), and leverage the database's query optimizer.
jx-sqlite  is also responsible for making the schema, and changing it dynamically as new JSON schema are encountered and to ensure that the old queries against the new schema have the same meaning.

The most interesting, and most important feature is that we query nested object arrays as if they were just another table.  This is important for two reasons:

1. Inner objects `{"a": {"b": 0}}` are a shortcut for nested arrays `{"a": [{"b": 0}]}`, plus
2. Schemas can be expanded from one-to-one  to one-to-many `{"a": [{"b": 0}, {"b": 1}]}`.


## Tests

There are over 200 tests used to confirm the expected behaviour: They test a variety of JSON forms, and the queries that can be performed on them. Most tests are further split into three different output formats ( list, table and cube).




## How to Use: Example

Create a table object from `QueryTable` class. The two useful methods of `QueryTable` class are `insert()` and `query()`. To insert data, use `insert(docs)` method where `docs` is a `list` of documents to be inserted in the table and to query, use `query(your_query)` method where `your_query` is a `dict` object following JSON Query Expressions (see docs on JSON Query Expressions below). A sample example is shown here for better understanding.
And yes, don't forget to wrap the query.


    from jx_sqlite.query_table import QueryTable
    from mo_dots import wrap
    from copy import deepcopy

    index = QueryTable("dummy_table")

    sample_data = [
        {"a": "c", "v": 13},
        {"a": "b", "v": 2},
        {"v": 3},
        {"a": "b"},
        {"a": "c", "v": 7},
        {"a": "c", "v": 11}
    ]

    index.insert(sample_data)

    sample_query = {
        "from": "dummy_table"
    }

    result = index.query(deepcopy(wrap(sample_query)))





## Installation
Python2.7 required. Package can be installed via pypi see below:
        
    pip install jx-sqlite

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

        $ git clone https://github.com/mozilla/jx-sqlite
        $ cd jx-sqlite
   
## Running tests

    export PYTHONPATH=.
    python -m unittest discover -v -s tests
    

## Docs

* [Json Query Expression](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md)
* [Nomenclature](https://github.com/mozilla/jx-sqlite/blob/master/docs/Nomenclature.md)
* [Snowflake](https://github.com/mozilla/jx-sqlite/blob/master/docs/Perspective.md)
* [JSON in Database](https://github.com/mozilla/jx-sqlite/blob/master/docs/JSON%20in%20Database.md)
* [The Future](https://github.com/mozilla/jx-sqlite/blob/master/docs/The%20Future.md)


## Contributors

Contributions are always welcome!

## License

This project is licensed under Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.



## GSOC

Work done up to the deadline of GSoC'17:

* [Pull Requests](https://github.com/mozilla/jx-sqlite/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Arohit-rk)
* [Commits](https://github.com/mozilla/jx-sqlite/commits?author=rohit-rk)


Future Work

* [Issues](https://github.com/mozilla/jx-sqlite/issues)
* [The Future](https://github.com/mozilla/jx-sqlite/blob/master/docs/The%20Future.md)
