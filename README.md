# jx-sqlite 

JSON query expressions using SQLite

## Summary

This library will manage your database schema to store JSON documents. You get all the speed of a well-formed database schema without the schema migration headaches. 

https://www.youtube.com/watch?v=0_YLzb7BegI&list=PLSE8ODhjZXja7K1hjZ01UTVDnGQdx5v5U&index=26&t=260s

## Status

Significant updates to the supporting libraries has broken this ode.  It still works works for the simple cases that require it

**Jan 2020** - 96/283 test failing  



## Installation

    pip install jx-sqlite

## Code Example

Open a database 

```python
container = Container()
```

Declare a table

```python
table = container.get_or_create_facts("my_table")
```

Pour JSON documents into it

```python
table.add({"os":"linux", "value":42})
```

Query the table

```python
table.query({
    "select": "os", 
    "where": {"gt": {"value": 0}}
})
```

## More

An attempt to store JSON documents in SQLite so that they are accessible via SQL. The hope is this will serve a basis for a general document-relational map (DRM), and leverage the database's query optimizer.
jx-sqlite  is also responsible for making the schema, and changing it dynamically as new JSON schema are encountered and to ensure that the old queries against the new schema have the same meaning.

The most interesting, and most important feature is that we query nested object arrays as if they were just another table.  This is important for two reasons:

1. Inner objects `{"a": {"b": 0}}` are a shortcut for nested arrays `{"a": [{"b": 0}]}`, plus
2. Schemas can be expanded from one-to-one  to one-to-many `{"a": [{"b": 0}, {"b": 1}]}`.


## Motivation

JSON is a nice format to store data, and it has become quite prevalent. Unfortunately, databases do not handle it well, often a human is required to declare a schema that can hold the JSON before it can be queried. If we are not overwhelmed by the diversity of JSON now, we soon will be. There will be more JSON, of more different shapes, as the number of connected devices( and the information they generate) continues to increase.

## Contributing

Contributions are always welcome! The best thing to do is find a failing test, and try to fix it.

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

    $ git clone https://github.com/mozilla/jx-sqlite
    $ cd jx-sqlite

### Running tests

There are over 200 tests used to confirm the expected behaviour: They test a variety of JSON forms, and the queries that can be performed on them. Most tests are further split into three different output formats ( list, table and cube).

    export PYTHONPATH=.
    python -m unittest discover -v -s tests

### Technical Docs

* [Json Query Expression](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md)
* [Nomenclature](https://github.com/mozilla/jx-sqlite/blob/master/docs/Nomenclature.md)
* [Snowflake](https://github.com/mozilla/jx-sqlite/blob/master/docs/Perspective.md)
* [JSON in Database](https://github.com/mozilla/jx-sqlite/blob/master/docs/JSON%20in%20Database.md)
* [The Future](https://github.com/mozilla/jx-sqlite/blob/master/docs/The%20Future.md)

## License

This project is licensed under Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.


## History

*Sep 2018* - Upgrade libs, start refactoring to work with other libs

*Dec 2017* - A number of tests were added, but they do not pass.

*Sep 2017* - GSoC work completed, all but a few tests pass.
 

## GSOC

Work done upto the deadline of GSoC'17:

* [Pull Requests](https://github.com/mozilla/jx-sqlite/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Arohit-rk)
* [Commits](https://github.com/mozilla/jx-sqlite/commits?author=rohit-rk)


## More Documentation

* [The Future](https://github.com/mozilla/jx-sqlite/blob/master/docs/The%20Future.md)
