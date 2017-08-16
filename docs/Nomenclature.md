## Nomenclature

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

