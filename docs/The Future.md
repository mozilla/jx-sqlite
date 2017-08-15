
## Open problems

**Do we copy the `a.*` columns from the `example` table to our new child table?** As I see it, there are two possible answers:

1. **Do not copy** - If there is just one nested document, or it is an inner object, then it will fit in a single row of the parent, and reduce the number of rows returned when querying. The query complexity increases because we must consider the case of inner objects and the case of nested objects.
2. **Copy** - Effectively move the columns to the new child table: This will simplify the queries because each path is realized in only one table but may be more expensive because every inner object will demand an SQL-join, it may be expensive to perform the alter table.

**How to handle arrays of arrays?** I have not seen many examples in the wild yet. Usually, arrays of arrays represent a multidimensional array, where the number of elements in every array is the same. Maybe we can reject JSON that does not conform to a multidimensional interpretation. 

