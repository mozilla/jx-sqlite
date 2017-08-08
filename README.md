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


There are over 200 tests used to confirm the expected behaviour: They test a variety of JSON forms, and the queries that can be performed on them. Most tests are further split into three different output formats ( list, table and cube).


## Status
## Code Example

## Getting Started
still to write:
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## Design

### Nomenclature

* **Nested Object** - An object in an array
* **Inner Object** - A child object, no arrays

This nomenclature is different than most documentation that talks about JSON documents: Other documentation will use the word *nested* to refer to either sub-structure ambiguously.

### Overloading property types

A distinction between databases and document stores is the ability to store different primitive types at the same property. To overcome this, we markup the columns of the database with the type. Let's put two objects into the `example` table:

    {"a": 1}
    {"a": "hello"}

We markup the columns with `$`+typename, with the hope we avoid namespace collisions.

### `example`

| _id | a.$integer | a.$string |
|-----|------------|-----------|
|  0  |      1     |    null   |
|  1  |    null    |  "hello"  |

We add an `_id` column as a UID, so we can distinguish documents.

The good thing about adding the type to the name is we can store primitive values:

    "hello world"


| _id | a.$integer | a.$string |    $string    |
|-----|------------|-----------|---------------|
|  2  |    null    |    null   | "hello world" |


### Inner objects

Limiting ourselves to inner objects, with no arrays, we can store them in the database so that each column represents a *path* to a literal value

    {"a": {"b": 1, "c": "test"}}

there are only two leaves in this tree of documents:

### `example`

| _id | a.b.$integer | a.c.$string |
|-----|--------------|-------------|
|  3  |       1      |    "test"   |


When we encounter objects with different structures, we can perform schema expansion

    {"a": {"b": {"d": 3}}}

we do this by adding columns to the table so we can store the new leaf values

### `example`

| _id | a.b.$integer | a.c.$string | a.b.d.$integer |
|-----|--------------|-------------|----------------|
|  4  |     null     |     null    |        3       |


### Nested Objects

When it comes to nesting objects, a new table will be required 

    {"a": [{"b": 4}, {"b":5}]}

Our fact table has no primitive values

### `example`

| _id | a.b.$integer | a.c.$string | a.b.d.$integer |
|-----|--------------|-------------|----------------|
|  5  |     null     |     null    |      null      |

Our nested documents are stored in a new table, called `example.a`

### `example.a`

| _id | _order | _parent | b.integer |
| --- | ------ | ------- | --------- |
|  6  |    0   |    5    |     4     | 
|  7  |    1   |    5    |     5     | 

Child tables have a `_id` column, plus two others: `_order` so we can reconstruct the original JSON and `_parent` which is used to refer to the immediate parent of the array.

## More Design Docs

* [Snowflake](https://github.com/mozilla/jx-sqlite/blob/master/docs/Perspective.md)
* [JSON in Database](https://github.com/mozilla/jx-sqlite/blob/master/docs/JSON%20in%20Database.md)



## Open problems

**Do we copy the `a.*` columns from the `example` table to our new child table?** As I see it, there are two possible answers:

1. **Do not copy** - If there is just one nested document, or it is an inner object, then it will fit in a single row of the parent, and reduce the number of rows returned when querying. The query complexity increases because we must consider the case of inner objects and the case of nested objects.
2. **Copy** - Effectively move the columns to the new child table: This will simplify the queries because each path is realized in only one table but may be more expensive because every inner object will demand an SQL-join, it may be expensive to perform the alter table.

**How to handle arrays of arrays?** I have not seen many examples in the wild yet. Usually, arrays of arrays represent a multidimensional array, where the number of elements in every array is the same. Maybe we can reject JSON that does not conform to a multidimensional interpretation. 


## Contributors
Contributions are always welcome!

## License
This project is licensed under Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
