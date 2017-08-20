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


## Status
## Code Example

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
All the commits done by Rohit Kumar in GSoC'17 can be found here:
* [GSoC work]()