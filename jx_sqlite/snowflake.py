from collections import OrderedDict
from copy import copy

from mo_dots import relative_field, listwrap, split_field, join_field, wrap, startswith_field, concat_field, Null, coalesce, set_default
from mo_logs import Log

from jx_sqlite import quote_table, typed_column, UID, quoted_UID, quoted_GUID,sql_types, quoted_PARENT, ORDER, quoted_ORDER
from jx_sqlite import untyped_column
from jx_base.queries import get_property_name
from jx_python import jx
from jx_python.meta import Column
from jx_python.containers import STRUCT
from pyLibrary.sql.sqlite import quote_column


class Snowflake(object):
    """
    MANAGE SQLITE DATABASE
    """
    def __init__(self, fact, uid, db):
        self.fact = fact  # THE CENTRAL FACT TABLE
        self.uid = uid
        self.db = db
        self.columns = []  # EVERY COLUMN IS ACCESSIBLE BY EVERY TABLE IN THE SNOWFLAKE
        self.tables = OrderedDict()  # MAP FROM NESTED PATH TO Table OBJECT, PARENTS PROCEED CHILDREN
        if not self.read_db():
            self.create_fact(uid)

    # def __del__(self):
    #     for nested_path, table in self.tables.items():
    #         self.db.execute("DROP TABLE " + quote_table(concat_field(self.fact, nested_path)))

    def read_db(self):
        """
        PULL SCHEMA FROM DATABASE, BUILD THE MODEL
        :return: None
        """

        # FIND ALL TABLES
        result = self.db.query("SELECT * FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = wrap([{k: d[i] for i, k in enumerate(result.header)} for d in result.data])
        tables_found = False
        for table in tables:
            if table.name.startswith("__"):
                continue
            tables_found = True
            nested_path = [join_field(split_field(tab.name)[1:]) for tab in jx.reverse(tables) if startswith_field(table.name, tab.name)]
            self.add_table_to_schema(nested_path)

            # LOAD THE COLUMNS
            command = "PRAGMA table_info(" + quote_table(table.name) + ")"
            details = self.db.query(command)

            for cid, name, dtype, notnull, dfft_value, pk in details.data:
                if name.startswith("__"):
                    continue
                cname, ctype = untyped_column(name)
                column = Column(
                    names={np: relative_field(cname, np) for np in nested_path},
                    type=coalesce(ctype, {"TEXT": "string", "REAL": "number", "INTEGER": "integer"}.get(dtype)),
                    nested_path=nested_path,
                    es_column=name,
                    es_index=table.name
                )

                self.add_column_to_schema(column)

        return tables_found

    def create_fact(self, uid=UID):
        """
        MAKE NEW TABLE WITH GIVEN guid
        :param uid: name, or list of names, for the GUID
        :return: None
        """
        self.add_table_to_schema(["."])

        uid = listwrap(uid)
        new_columns = []
        for u in uid:
            if u == UID:
                pass
            else:
                c = Column(
                    names={".": u},
                    type="string",
                    es_column=typed_column(u, "string"),
                    es_index=self.fact
                )
                self.add_column_to_schema(c)
                new_columns.append(c)

        command = (
            "CREATE TABLE " + quote_table(self.fact) + "(" +
            (",".join(
                [quoted_GUID + " TEXT "] +
                [quoted_UID + " INTEGER"] +
                [quote_column(c.es_column) + " " + sql_types[c.type] for c in self.tables["."].schema.columns]
            )) +
            ", PRIMARY KEY (" +
            (", ".join(
                [quoted_GUID] +
                [quoted_UID] +
                [quote_column(c.es_column) for c in self.tables["."].schema.columns]
            )) +
            "))"
        )

        self.db.execute(command)

    def change_schema(self, required_changes):
        """
        ACCEPT A LIST OF CHANGES
        :param required_changes:
        :return: None
        """
        required_changes = wrap(required_changes)
        for required_change in required_changes:
            if required_change.add:
                self._add_column(required_change.add)
            elif required_change.nest:
                column, cname = required_change.nest
                self._nest_column(column, cname)
                # REMOVE KNOWLEDGE OF PARENT COLUMNS (DONE AUTOMATICALLY)
                # TODO: DELETE PARENT COLUMNS? : Done

    def _add_column(self, column):
        cname = column.names["."]
        if column.type == "nested":
            # WE ARE ALSO NESTING
            self._nest_column(column, cname)

        table = concat_field(self.fact, column.nested_path[0])

        self.db.execute(
            "ALTER TABLE " + quote_table(table) +
            " ADD COLUMN " + quote_column(column.es_column) + " " + sql_types[column.type]
        )

        self.add_column_to_schema(column)

    def _nest_column(self, column, new_path):
        destination_table = concat_field(self.fact, new_path)
        existing_table = concat_field(self.fact, column.nested_path[0])

        # FIND THE INNER COLUMNS WE WILL BE MOVING
        moving_columns = []
        for c in self.columns:
            if destination_table!=column.es_index and column.es_column==c.es_column:
                moving_columns.append(c)
                c.nested_path = [new_path] + c.nested_path

        # TODO: IF THERE ARE CHILD TABLES, WE MUST UPDATE THEIR RELATIONS TOO?

        # DEFINE A NEW TABLE?
        # LOAD THE COLUMNS
        command = "PRAGMA table_info(" + quote_table(destination_table) + ")"
        details = self.db.query(command)
        if not details.data:
            command = (
                "CREATE TABLE " + quote_table(destination_table) + "(" +
                (",".join(
                    [quoted_UID + " INTEGER", quoted_PARENT + " INTEGER", quoted_ORDER+" INTEGER"]
                )) +
                ", PRIMARY KEY (" + quoted_UID + ")" +
                ", FOREIGN KEY (" + quoted_PARENT + ") REFERENCES " + quote_table(existing_table) + "(" + quoted_UID + ")"
                ")"
            )
            self.db.execute(command)
            self.add_table_to_schema([new_path])

        # TEST IF THERE IS ANY DATA IN THE NEW NESTED ARRAY
        if not moving_columns:
            return

        column.es_index = destination_table
        self.db.execute(
            "ALTER TABLE " + quote_table(destination_table) +
            " ADD COLUMN " + quote_column(column.es_column) + " " + sql_types[column.type]
        )

        # Deleting parent columns
        for col in moving_columns:
            column = col.es_column
            tmp_table = "tmp_" + existing_table
            columns = self.db.query("select * from " + quote_table(existing_table) + " LIMIT 0").header
            self.db.execute(
                "ALTER TABLE " + quote_table(existing_table) +
                " RENAME TO " + quote_table(tmp_table)
            )
            self.db.execute(
                "CREATE TABLE " + quote_table(existing_table) +
                " AS SELECT " + (", ".join([quote_table(c) for c in columns if c!=column])) +
                " FROM " + quote_table(tmp_table)
            )
            self.db.execute("DROP TABLE " + quote_table(tmp_table))
            
    def add_table_to_schema(self, nested_path):
        table = Table(nested_path)
        self.tables[table.name] = table
        path = table.name

        for c in self.columns:
            rel_name = c.names[path] = relative_field(c.names["."], path)
            table.schema.add(rel_name, c)
        return table

    def add_column_to_schema(self, column):
        self.columns.append(column)
        abs_name = column.names["."]

        for table in self.tables.values():
            rel_name = column.names[table.name] = relative_field(abs_name, table.name)
            table.schema.add(rel_name, column)


class Table(object):

    def __init__(self, nested_path):
        self.nested_path = nested_path
        self.schema = Schema(nested_path)  # MAP FROM RELATIVE NAME TO LIST OF COLUMNS

    @property
    def name(self):
        """
        :return: THE TABLE NAME RELATIVE TO THE FACT TABLE
        """
        return self.nested_path[0]


class Schema(object):
    """
    A Schema MAPS ALL COLUMNS IN SNOWFLAKE FROM THE PERSPECTIVE OF A SINGLE TABLE (a nested_path)
    """

    def __init__(self, nested_path):
        self.map = {}
        self.nested_path = nested_path

    def add(self, column_name, column):
        if column_name != column.names[self.nested_path[0]]:
            Log.error("Logic error")

        container = self.map.get(column_name)
        if not container:
            container = self.map[column_name] = []
        container.append(column)

    def remove(self, column_name, column):
        if column_name != column.names[self.nested_path[0]]:
            Log.error("Logic error")

        self.map[column_name]=[c for c in self.map[column_name] if c != column]

    def __getitem__(self, item):
        output = self.map.get(item)
        return output if output else Null

    def __copy__(self):
        output = Schema(self.nested_path)
        for k, v in self.map.items():
            output.map[k] = copy(v)
        return output

    def get_column_name(self, column):
        """
        RETURN THE COLUMN NAME, FROM THE PERSPECTIVE OF THIS SCHEMA
        :param column:
        :return: NAME OF column
        """
        return get_property_name(column.names[self.nested_path[0]])

    def keys(self):
        return self.map.keys()

    def items(self):
        return self.map.items()

    @property
    def columns(self):
        return [c for cs in self.map.values() for c in cs]

    def map_to_sql(self, var=""):
        """
        RETURN A MAP FROM THE RELATIVE AND ABSOLUTE NAME SPACE TO COLUMNS 
        """
        origin = self.nested_path[0]
        fact_dict={}
        origin_dict={}
        for k, cs in self.map.items():
            for c in cs :
                if c.type not in STRUCT:
                    if (startswith_field(get_property_name(k), var)):
                        if c.names[origin] in origin_dict:
                            origin_dict[c.names[origin]].append(c)
                        else:
                            origin_dict[c.names[origin]] = [c]
    
                        if origin!=c.nested_path[0]:
                            if c.names["."] in fact_dict:
                                fact_dict[c.names["."]].append(c)
                            else:
                                fact_dict[c.names["."]] = [c]
                    elif origin==var:
                        if concat_field(var, c.names[origin]) in origin_dict:
                            origin_dict[concat_field(var, c.names[origin])].append(c)
                        else:
                            origin_dict[concat_field(var, c.names[origin])] = [c]

                        if origin!=c.nested_path[0]:
                            if c.names["."] in fact_dict:
                                fact_dict[concat_field(var, c.names["."])].append(c)
                            else:
                                fact_dict[concat_field(var, c.names["."])] = [c]                        

        return set_default(origin_dict, fact_dict)

