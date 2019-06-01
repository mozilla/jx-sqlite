
## Definitions

Some nomenclature is required to help follow the logic of these modules.

* **Table** - Same as with database terminology; it is a single, unordered, set of rows.  This is used to refer to real database tables, but also refer to a [fully joined snowflake from a single perspective](../docs/Perspective.md). 
* **Schema** - A set of columns that describe all the (possibly optional) properties available on all rows of a table. This is different than conventional database nomenclature: "Schema" does not refer to multiple tables.  
* **Facts** - Represents the multiple tables in the hierarchical database
* **Snowflake** - Snowflake is a list of all columns, for all the tables, in the hierarchical database.
* **Container** - Datastore that has multiple facts
* **Namespace** - Metadata for a container: Information on multiple snowflakes. Notice a "database schema" is referred to as a "namespace".
* **Database** - Refers to a relational database, possibly hierarchical.

JSON Query Expressions operate on Facts. Queries are simple because they assume any path between any two tables is unique: Any column in the hierarchical database can be accessed using a unique combination of joins with the origin.


* **relative table names** - All the tables in a snowflake have the same prefix: The name of the fact table. The relative name excludes this, so the fact table itself has a relative name of `"."`   
* **nested_path** - The sequence of relative table names; from the focal table to the fact table. The `nested_path` of the fact table is `['.']`
* **query_paths** - A snowflake represents many tables, and the `query_paths` is the set of all relative table names for that snowflake.  