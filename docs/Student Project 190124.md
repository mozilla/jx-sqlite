# Query JSON Documents using SQLite (GSOC) 

This is a continuation part 2 [read the original document](


## The Actual Task 

Please fix and existing Python Sqlite connector capable of passing the [JSONQueryExpressionTests](https://github.com/klahnakoski/JSONQueryExpressionTests) suite.

There are over 200 tests used to confirm the expected behaviour: They test a variety of JSON forms, and the queries that can be performed on them. Most tests are further split into 3 different output formats. Success means passing all tests using a Sqlite database. 


1. get tests running on `master` branch
2. look at the `jump` branch - which is the master, but with all vendor libs updated
3. Look through 


Guidance 

The 



Slight improvements to the 
Integrate into new vendor library
Pass more tests that have been added over the years
Add window functions?
Add compound queries?




improve the API, 



## Overview

Could you write a database schema to store the following JSON?

	{
		"name": "The Parent Trap",
		"released": "29 July 1998",
		"imdb": "http://www.imdb.com/title/tt0120783/",
		"rating": "PG"
		"director": {
			"name": "Nancy Meyers"
			"dob": "December 8, 1949"
		}
	} 

Could you write a query to extract all movies directed by Nancy Meyers?

Can you modify your database schema to additionally store JSON with nested objects (plus other changes)?

	{
		"name": "The Parent Trap",
		"released": 901670400,
		"director": "Nancy Meyers"
		"cast": [
			{"name": "Lindsay Lohan"},
			{"name": "Dennis Quaid"},
			{"name": "Natasha Richardson"}
		]
	}

Could you write a query to return all movies with Lindsay Lohan in the cast?
		
Now. Can you program a machine to do this for you!!? Can your program modify the database, on the fly, as you receive more documents like the above? **Most important:** How do your queries change?

## Problem 

JSON is a nice format to store data, and it has become quite prevalent. Unfortunately, databases do not handle it well; often a human is required to declare a schema that can hold the JSON before it can be queried. If we are not overwhelmed by the diversity of JSON now, I expect we soon will be. I expect there to be more JSON, of more different shapes, as the number of connected devices (and the information they generate) continues to increase.   

https://www.youtube.com/watch?v=4N_ktE4NFIk

## The solution

The easy part is making the schema, and changing it dynamically as new JSON schema are encountered. The harder problem is ensuring the old queries against the new schema have the same meaning. In general this is impossible, but there are particular schema migrations that can leave the meaning of existing queries unchanged.  

By dealing with JSON documents we are limiting ourselves to [snowflake schemas](https://en.wikipedia.org/wiki/Snowflake_schema). This limitation reduces the scope of the problem. Let's further restrict ourselves to a subset of schema transformations that can be handled automatically; we will call them "schema expansions":

1.	Adding a property - This is a common migration
2.	Changing the datatype of a property, or allowing multiple types - It is nice if numbers can be queried like numbers and strings as strings even if they are found in the same property..
3.	Change a single-valued property to a multi-valued property - Any JSON property `{"a": 1}` can be interpreted as multi-valued `{"a": [1]}`. Then assigning multiple values is trivial expansion `{"a": [1, 2, 3]}`.
4.	Change an inner object to nested array of objects - Like the multi-valued case: `{"a":{"b":"c"}}`, each inner object can be interpreted as a nested array `{"a": [{"b":"c"}]}`.  Which similarly trivializes schema expansion.

Each of these schema expansions should not change the meaning of old queries. Have no fear! The depths of history gives us a language that is already immutable under all these transformations!

## Schema-Independent Query Language?

Under an expanding schema, can we write queries that do not change meaning? For hierarchical data, data that fits in a [snowflake schema](https://en.wikipedia.org/wiki/Snowflake_schemahierarchical): **Yes! Yes we can!!!**

Each JSON document can be seen as a single point in a multidimensional Cartesian space; where the properties represent coordinates in that space. Inner objects simply add dimensions, and nested object arrays represent constellations of points in an even-higher dimensional space. These multidimensional [data cubes](https://en.wikipedia.org/wiki/OLAP_cube) can be represented by [fact tables](https://en.wikipedia.org/wiki/Fact_table) in a [data warehouse](https://en.wikipedia.org/wiki/Data_warehouse). Fact tables can be queried with [MDX](https://en.wikipedia.org/wiki/MultiDimensional_eXpressions). 

With this in mind, we should be able to use MDX as inspiration to query JSON datastores. Seeing data as occupying a Cartesian space gives us hints about the semantics of queries, and how they might be invariant over the particular schema expansions listed above.

**Some user documentation may help with understanding the query language**: [JSON Query Expressions](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md)

## Benefits

Having the machine manage the data schema gives us a new set of tools: 

* **Easy interface to diverse JSON** - a query language optimized for JSON documents
* **No schema management** - Schemas, and migrations of schemas, are managed by the machine.
* **Scales well** - Denormalized databases, with snowflake schemas, can be sharded naturally, which allows us to scale.    
* **Handle diverse datasources** - Relieving humans of schema management means we can ingest more diverse data faster. The familiar [ETL process](https://en.wikipedia.org/wiki/Extract,_load,_transform) can be replaced with [ELT](https://en.wikipedia.org/wiki/Extract,_transform,_load) Links: [A](http://hexanika.com/why-shift-from-etl-to-elt/), [B](https://www.ironsidegroup.com/2015/03/01/etl-vs-elt-whats-the-big-difference/)
* **Mitigate the need for a (key, value) table** - Automatic schema management allows us to annotate records, or objects, without manual migrations: This prevents the creation of a (key, value) table (the "junk drawer" found in many database schemas) where those annotations usually reside.  
* **Automatic ingestion of changing relational databases** - Despite the limited set of schema expansions, we can handle more general relational database migrations: Relational databases can [extracted as a series of De-normailzed fact cubes](https://github.com/klahnakoski/MySQL-to-S3) As a relational database undergoes migrations (adding columns, adding relations, splitting tables), the extraction process can continue to capture the changes because each fact table is merely a snowflake subset of the relational whole.
* **Leverage existing database optimization** - This project is about using MDX-inspired query semantics and translating to database-specific query language: We leverage the powerful features of the underlying datastore.  

## Existing solutions

* We might be able to solve the problem of schema management by demanding all JSON comes with a formal JSON schema spec. That is unrealistic; it pushes the workload upstream, and is truly unnecessary given the incredible amount of computer power at our fingertips.
* Elasticsearch 1.x has limited automatic schema detection which has proven useful for indexing and summarizing data of unknown shapes. We would like to generalize this nice feature and and bring machine managed schemas to other datastores.   
* Oracle uses [json_*](http://www.oracle.com/technetwork/database/sql-json-wp-2604702.pdf) functions to define views which can operate on JSON. It has JSON path expressions; mimicking MDX, but the overall query syntax is clunky. Links: [A](https://docs.oracle.com/database/121/ADXDB/json.htm#ADXDB6246), [B](https://blogs.oracle.com/jsondb/entry/s)
* Spark has [Schema Merging](http://spark.apache.org/docs/latest/sql-programming-guide.html#schema-merging) and nested object arrays can be accessed using [explode()](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode). Spark is a little more elegant, despite the the fact it exposes *how* the query executes.
* Sqlite has the [JSON1 connector](https://www.sqlite.org/json1.html) - Which is a limited form of Oracle's solution; it requires manual schema translation which complicates queries. 

These existing solutions solve the hard problems from the bottom up; managing file formats, organizing indexes, managing resources and query planning. Each built their own stack with their own query conventions guided by the limitations of architecture they built. 

This project is about working from the top down: A consistent way to query messy data; identical, no matter the underlying data store; so we can swap datastores based on scale of the problem.     

## The Actual Task 

Please make a Python Sqlite connector capable of passing the [JSONQueryExpressionTests](https://github.com/klahnakoski/JSONQueryExpressionTests) suite.

There are over 200 tests used to confirm the expected behaviour: They test a variety of JSON forms, and the queries that can be performed on them. Most tests are further split into 3 different output formats. Success means passing all tests using a Sqlite database. 

## Non-Objectives

* **Translation Speed** - Once we are able to hoist the miserable state of database schema management out of the realm of human intervention, we can worry about optimizing the query translation pipeline.   
* **Record Insert Speed** - Query response time over large data is most important, insert speed is not. It is a known problem that altering table schemas on big tables can take a long time. Solving this is not a priority.
* **Big data support** - We are focusing on data scale that fits on a single machine; millions, not billions. 


## Variations

For this project, a connector for Sqlite is preferred: It provides us with a fast, in-memory, dependency-light JSON datastore for use in Python programs.  

Even though Sqlite is preferred, the choice of datastore is not very important to this project. Additional usefulness comes from being able to use the same query language on a diverse set of datastores; each has strengths and weaknesses, swapping one for another gives us flexibility. 


* **MySQL** - Use MySQL instead of SQLite - this may be slightly easier, but depends on a MySQL service to be useful.  
* **Columnar DB** - Still use a database, but use columnar strategies: Give each JSON property its own table with foreign keys pointing to the document id. Adding new columns will be fast because they are whole new tables.  Queries may be faster because rows are smaller, or queries may be slower because of join costs. Any work on this variation would be experimental. 
* **Numpy** - Use the columnar storage strategy, and use Numpy to store the columns. This could give us a very fast query response, albeit limited to memory.


## The Future?

* **Hetrogenous Shards** - Being able to send the same query to multiple backends allows us to pick a backend that best meets requirements; very big, or very fast
* **Dimensions** - The next step in isolating queries from schema migrations involves declaring "dimensions": [Dimensions](https://en.wikipedia.org/wiki/Dimension_(data_warehouse)) are a level of indirection responsible for translating the complex reality of the data to a cleaner, and easy-to-query property. This can involve renaming, simple parsing, and adding business meaning to vectors in the multidimensional fact table.  
* **Machine Managed indexes** - Databases indexes act much like a columnar datastore. If we account for the common queries received, we may be able to choose the right indexes to improve query response. We might just beat Elasticsearch!
* **Subqueries** - Allowing heterogeneous datastores also allows us to split queries across platforms so each backend can handle the part it is best at; By dispatching data aggregation and filtering to a cluster we get fast response over big data, while a local database can use previous query results to perform sophisticated cross referencing and window functions.

## Questions and Answers

**Where do I start?**

> Be sure to read this document, and read the links. Especially [JSON Query Expressions](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md), which will give you a high level idea of what you will be building.
> 
> I attempted a solution ([jx-sqlite](https://github.com/klahnakoski/jx-sqlite/tree/master)) but it is far from a complete. The tests that do pass are the easy tests the hard tests remain; It might require a refactoring of the code. I only suggest extending this code if you are unusually good at understanding other people's code.
>
> Building your own solution from scratch is a reasonable path to take.  This will allow you to understand the problem without understanding how my incomplete solution tried to solve the problem. It may be less work overall. You can fork the [jx-sqlite](https://github.com/klahnakoski/jx-sqlite/tree/master) code, remove all the implementation and start writing code that will pass tests. 


**Can I fork ActiveData?**

> Forking ActiveData is not a good idea: ActiveData works with ElasticSearch 1.7 which can already deal with nested object arrays, and perform schema merging. Elasticsearch uses a completely different data model from relational databases.Tracking the nested object array schema and translating the queries is the hard part of using Sqlite.


**What is to be done next in `jx-sqlite`?**

> If you are comfortable with extending [jx-sqlite](https://github.com/klahnakoski/jx-sqlite/tree/master), then it is obvious to ask where is the hole to be filled:
> The majority of the problem is performing the deep queries: When JSON documents are added, the library is responsible for creating tables to hold the contents of any arrays - this is broken because the schema management is not clear and there are bugs. The current code is too complicated to understand in a single sitting; so I started a refactor to that will provide an API to a snowflake schema; it will map a hierarchical (snowflake) schema to a sqlite relational database. Hopefully, the JSON <-> Snowflake <-> sqlite transform will prove easier to understand and we can pass some of the more complicated tests.

**What about the JSON1 connector for Sqlite?**

> The [JSON1 connector](https://www.sqlite.org/json1.html) is a limited form of Oracle's JSON query features; it provides functions over a BLOB, and this limits what the database can do to make queries faster. I want a solution that can leverage the features of a database; like profile statistics, query optimization, ability to add indexes, materialized views, and SQL.
>
> Yes, including SQL: JSON query expressions are designed to make complex data easy to query, but they are limited to viewing data as belonging to a data cube; having general SQL expressions opens the data up to more complex analysis. With JSON properties fully decomposed to the database we get both complex and optimized queries.


**Why does `jx-sqlite` use pyLibrary?**

> The [jx-sqlite](https://github.com/klahnakoski/jx-sqlite/tree/master) implementation has an unfortunate interdependence with pyLibrary; It would be nice to decouple these two.


**May you give me a test that is easy to solve?**

> Here is a test that is relatively easy to solve: [`test_select3_object(self)`](https://github.com/klahnakoski/jx-sqlite/blob/master/tests/test_jx/test_set_ops.py#L942) It is returning the wrong number of columns when returning `"format":"table"`.
>
> This test is about interpreting the meaning of `{"select": ["o", "a.*"]}` in the context of formatting as a table.  As per [the user documentation on `select`](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx_clause_select.md#selecting-leaves-), we expect the star (`*`) to expand all leaves into individual columns, which did not happen in this test.
>
> Put a breakpoint in the code at [setop_table.py, line 323](https://github.com/klahnakoski/jx-sqlite/blob/04752922974a84f225dd9b058c4c939989b613e9/jx_sqlite/setop_table.py#L323) (Notice the test is named "set_ops" and the code is named similarly as "setop") This is the point just before the data is formatted into table form; we first ensure the data (in `result.data`) has all the records we require; it could be that the query is wrong (but the data looks good). Then we can check to see why we are getting less columns than we expect: Step through the formatting code to understand what it is doing. Also, understand how the properties for the columns in `for c in cols` are used to decide what the `header` should be.


**May you give me test that is complicated to solve?**

> When you run the tests you will notice many "deep" tests are failing.  Here is one of the failing tests [`test_deep_select_column(self)`](https://github.com/klahnakoski/jx-sqlite/blob/master/tests/test_jx/test_deep_ops.py#L25)
> 
>This test is performing a query on the following data:

```javascript
	"data": [
    	{"_a": [
	        {"b": "x", "v": 2},
        	{"b": "y", "v": 3}
    	]},
    	{"_a": {"b": "x", "v": 5}},
    	{"_a": [
	        {"b": "x", "v": 7},
	    ]},
	    {"c": "x"}
	]
```

> The important feature of this is the nested array of objects; which is what we are interested in querying.  This test is ensuring you can groupby `_a.b` and calculate the aggregate sum of `_a.v`
> 
> But the problem is greater than just getting the correct result; this test can not even insert the data into the database correctly:

```
	caused by
	    ERROR: Problem with
	    ALTER TABLE "testing._a" ADD COLUMN "_a.b.$string" TEXT
   		File "C:\Python27\lib\site-packages\mo_threads\threads.py", line 237, in _run
	caused by
    	ERROR: duplicate column name: _a.b.$string
```

> So, the problem appears to be some confusion about how the schema is modified before the records are inserted into the database. I have determined that this confusion is caused by bad programming; [so I started refactoring the parts dealing with managing the schema](https://github.com/klahnakoski/jx-sqlite/blob/master/jx_sqlite/alter_table.py). With all the methods in one place, I can now come up with some coherent design for this API: Something that is easy for the rest of the `jx-sqlite` code to manipulate snowflake schemas, and how to build them. The code for this API will be responsible for translating a snowflake schema into a plain relational database schema.


**Why Python 2.7? Are you a dinosaur?**

> Python 2.7 is used because when this project started a few years ago, newer versions of Python did not have reasonable binary library support for Windows. When enough package providers provide Python3+ support for Windows, then we can migrate. Feel free to modify the code to better match Python3 style where possible: For example: `except Exception, e` was a result of me simply not knowing about the `except Exception as e` format. Maybe we can advance the code to version 3, but that would be too much for this project I think

**Where will this code be used?**

> 1. It will enhance ActiveData: When ActiveData pulls data from the cluster, it would be nice to handle sub queries on that data before it gets sent back to the requester. Python is too slow to manipulate data, so we either require a temporary database or Numpy or Pandas. The hope is this project can handle the JSON coming from Elasticsearch queries and perform the required post-processing queries on that data.
>
> 2. It will enhance ActiveData: ActiveData tracks the Elasticsearch cluster metadata to help translate queries. This is too much data to handle quickly with pure Python. The hope is this project will serve as a fast metadata database.   
>
> 3. SpotManager:  The SpotManager is responsible for bidding for spot instances on AWS. It deals with a reasonable amount of data, but is slow because the queries are implemented in Python. Sqlite can go much faster for the given data volume.
>
> 4. esShardBalancer - Another pure-python program than can be made to go faster if queries were implemented in Sqlite.

> In general, code can make better decisions if it has lots of data. Python is too slow for this task, so we need a module that can handle data for us; something that accepts queries that will return short results to make decisions on. This data can come from any number of systems, in large quantities, and of changing schema over time. **We do not want to be manually declaring schemas for relations and properties not used by the code.** At the same time, we want to keep all the data in case we want to make decisions on it later, either as feature enhancement or for manual debugging.

**Can we add indexes and materialized views?**

>For this project query speed is not the highest priority: I am concerned about achieving feature parity with the existing Elasticsearch connector (aka passing all the tests), and I believe it may be more difficult than it looks: I am sure the test suite will grow as you find corner cases that appear while implementing a Sqlite connector. You can add indexes, or materialized views to make things faster, but I suggest you work on that after your solution can pass all the tests.

