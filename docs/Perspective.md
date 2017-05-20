

# Column Perspectives in a Snowflake Schema

This document is incomplete, with mistakes.

## Justification

The snowflake concept is an attempt to model the intersection between JSON documents and relational databases. Hopefully, this model will be useful for conceptualizing a document-relational map (DRM) while we program.

## Definitions

A snowflake is a set of tables, and the hierarchical foreign relations between them, where one table is deemed the key "fact table". Any relational database can have multiple snowflake interpretations; which is likely necessary if we want to cover the whole of a database. There is a lot we can say about covering relational databases, but we will leave that exciting topic for later.  

### Path taken to row is important

The snowflake assumes the database is denormalized; every fact in the fact table and all its foreign rows, are not shared with any other fact. This assumption is only a conceptual model, an not seriously expected in practice. A database is expected to be normalized for minimal redundancy. It is important not to talk about any row in a table, but rather the path taken along the foreign relations; starting from the fact to the row in question. 

### Paths to tables

The path along the foreign keys, from the fact table to any other table, is important. Tables can not be referred to by name, they must be referred to by path. The snowflake model calls this the `nested_path` of a table. "nested" refers to nested JSON documents

### Perspectives inside a snowflake

A snowflake is a set of database tables, and it also provides a way to talk about querying those tables in a denormalized perspective. We can query any of the tables in the snowflake as if it was a fact table, for clarity we will called this the "focus table": Any table in the snowflake can act as a basis for a perspective; the tables in the snowflake have names relative to that focus.

----------

**Example: Nested Objects (one to many)**

The simplest example of perspective is a document with nested objects: 

	[
	    {
	        "a": {
	            "b": [
					{"x": 1},
					{"x": 2}
				],
				"x": 3
			},
			"x": 4
		}
	]

...and here is the schema the document came from

![schema](nested1.png)

**Table `.`**
| `_id` |  `x`  |  `a.x`  |
|-------|-------|---------|
|   1   |   4   |    1    |

**Table `a.b`**

| `_id` | `_parent` | `_order` |  `x`  |
|-------|-----------|----------|-------|
|   2   |     1     |     0    |   1   |
|   3   |     1     |     1    |   2   |

There are points I should make here.

* The tables are all given relative names: The fact table is named dot (`.`).
* Arrows indicate foreign key relations: The table at the tail of the arrow is assumed to have a property that refers to the table at the head
* The foreign keys are not shown in the document: If the relational database uses foreign keys to only indicate relations, then it is good remove those properties that have no other meaning. Sometimes the foreign key is used for business logic, and must be exposed.

Let us focus on `nested_path=["a.b", "."]`

![schema](nested2.png)



	[
		{
			"x": 1
			"..": {
		    	"a": {
		            "b": [
						{"x": 1},
						{"x": 2}
					],
					"x": 3
				},
				"x": 4
			}
		},
		{
			"x": 2
			"..": {
		    	"a": {
		            "b": [
						{"x": 1},
						{"x": 2}
					],
					"x": 3
				},
				"x": 4
			}
		}
	]

Some liberty was taken here: The `..` property does not exist, it is shown to demonstrate the Snowflake schema uses "`..`" in the namespace to refer to the parent document.

----------

**Example: Reference (many to one)**

A normalized relational database can have foreign keys from the fact table to a lookup table; a many-to-one relation. No JSON document will can create such a relation because documents are denormalized entities, but a snowflake schema must be able to interpret all records in its schema as documents anyway.

![schema](ref1.png)


**Table `.`**
| `_id` |  `x`  |  `_e` |
|-------|-------|-------|
|   1   |   3   |   3   |
|   2   |   4   |   3   |

**Table `e`**

| `_id` |  `x`  |
|-------|-------|
|   3   |   1   |


We assume there is only one record in `e`, and let us focus on `nested_path=["e", "."]` 

![schema](ref2.png)

and show that record as JSON:


	{
		"x": 1,
		"..":[
			{"x": 3},
			{"x": 4}
		]
	}

More liberties:

* The "`..`" property does not exist, it is used to demonstrate that you can access the fact table from the perspective of `e` using the snowflake schema.  
 
![schema](ref3.png)

`nested_path=["."]`

With a focus on our fact table (`.`), we see each of our fact records, along with the `e` property duplicated (denormalized).

	[
	    {
	        "e": {"x": 1},
			"x": 3
		},
	    {
	        "e": {"x": 1},
			"x": 4
		}
	]
  

Mapping from a relational database to a document, and back to a relational database, may not result in the same schema: These two documents will map easily to a single table:

| `_id` |  `x`  |  `e.x`  |
|-------|-------|---------|
|   1   |   3   |    1    |
|   1   |   4   |    1    |


### Filesystem Metaphor

It may help to use the unix filesystem as a metaphor: The tables are directories in that filesystem, The fact table is root (`/`), while the focus table is your current directory (`cwd`). You can refer to any file either in the absolute sense, from root, or in a relative sense, from `cwd`.  Each file has as many names as there are directories.


### Referring to columns

The fully qualified name of a column in the schema of a relational database can be given by `<table_name>.<column_name>`. This strategy does not work for a snowflake because paths are important.  We require a data structure that is able to represent the multiple names that can be given to a single column, based on the perspective we are interested in.  

The `nested_path` is the absolute reference to the database table the column is in.  And `names['.']` is set to the relative name of the database column. Other perspectives can markup that column with additional names    


The 
### 


canonical snowflake
    
