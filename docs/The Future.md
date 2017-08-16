## The Future

* **Hetrogenous Shards** - Being able to send the same query to multiple backends allows us to pick a backend that best meets requirements; very big, or very fast
* **Dimensions** - The next step in isolating queries from schema migrations involves declaring "dimensions": [Dimensions](https://en.wikipedia.org/wiki/Dimension_(data_warehouse)) are a level of indirection responsible for translating the complex reality of the data to a cleaner and easy-to-query property. This can involve renaming, simple parsing, and adding business meaning to vectors in the multidimensional fact table.  
* **Machine Managed indexes** - Databases indexes act much like a columnar data store. If we account for the common queries received, we may be able to choose the right indexes to improve query response. We might just beat Elasticsearch!
* **Subqueries** - Allowing heterogeneous data stores also allows us to split queries across platforms so each backend can handle the part it is best at; By dispatching data aggregation and filtering to a cluster we get fast response over big data, while a local database can use previous query results to perform sophisticated cross referencing and window functions.
* **Add indexes and materialized views**



## Benefits

Having the machine manage the data schema gives us a new set of tools: 

* **Easy interface to diverse JSON** - a query language optimized for JSON documents
* **No schema management** - Schemas, and migrations of schemas are managed by the machine.
* **Scales well** - Denormalized databases, with snowflake schemas, can be sharded naturally, which allows us to scale.    
* **Handle diverse data sources** - Relieving humans of schema management means we can ingest more diverse data faster. The familiar [ETL process](https://en.wikipedia.org/wiki/Extract,_load,_transform) can be replaced with [ELT](https://en.wikipedia.org/wiki/Extract,_transform,_load) Links: [A](http://hexanika.com/why-shift-from-etl-to-elt/), [B](https://www.ironsidegroup.com/2015/03/01/etl-vs-elt-whats-the-big-difference/)
* **Mitigate the need for a (key, value) table** - Automatic schema management allows us to annotate records, or objects, without manual migrations: This prevents the creation of a (key, value) table (the "junk drawer" found in many database schemas) where those annotations usually reside.  
* **Automatic ingestion of changing relational databases** - Despite the limited set of schema expansions, we can handle more general relational database migrations: Relational databases can [extracted as a series of De-normalized fact cubes](https://github.com/klahnakoski/MySQL-to-S3) As a relational database undergoes migrations (adding columns, adding relations, splitting tables), the extraction process can continue to capture the changes because each fact table is merely a snowflake subset of the relational whole.
* **Leverage existing database optimization** - This project is about using MDX-inspired query semantics and translating to database-specific query language: We leverage the powerful features of the underlying datastore.  



## Open problems

**Do we copy the `a.*` columns from the `example` table to our new child table?** As I see it, there are two possible answers:

1. **Do not copy** - If there is just one nested document, or it is an inner object, then it will fit in a single row of the parent, and reduce the number of rows returned when querying. The query complexity increases because we must consider the case of inner objects and the case of nested objects.
2. **Copy** - Effectively move the columns to the new child table: This will simplify the queries because each path is realized in only one table but may be more expensive because every inner object will demand an SQL-join, it may be expensive to perform the alter table.

**How to handle arrays of arrays?** I have not seen many examples in the wild yet. Usually, arrays of arrays represent a multidimensional array, where the number of elements in every array is the same. Maybe we can reject JSON that does not conform to a multidimensional interpretation. 
