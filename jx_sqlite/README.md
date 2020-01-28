
## Definitions

To provide clarity between the relational model and the data warehouse model, we use the following terms

* Table - A set of rows
* Schema - Describes the columns for a table
* Facts - A number of tables connected by relations (hierarchical database)
* Snowflake - Describes all columns in a set of facts
* Container - Contains many facts
* Namespace - Describes the container, and contains all the snowflakes


## Directory structure

This directory is a stack of classes which used to be one massive class
We hope these clears up the separation enough to perform better refactorings

* **QueryTable** - implement the Container interface
* **AggsTable** - aggregates
* **SetOpTable** - simple set operations
* **InsertTable** - methods for inserting documents
* **BaseTable** - Constructor



