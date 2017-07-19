from collections import OrderedDict
from copy import copy

from mo_dots import relative_field, listwrap, split_field, join_field, wrap, startswith_field, concat_field, Null, coalesce
from mo_logs import Log

from jx_sqlite import quote_table, typed_column, UID, quoted_UID, quoted_GUID,sql_types, quoted_PARENT, ORDER, quoted_ORDER
from jx_sqlite import untyped_column
from jx_python import jx
from jx_python.meta import Column
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
        c=[]
        for required_change in required_changes:
            if required_change.add:
                command = self._add_column(required_change.add)
                if command!=None:
                    c.append(command)
            elif required_change.nest:
                command=self._nest_column(**required_change)
                if command!=None:
                    c.append(command)                
                # REMOVE KNOWLEDGE OF PARENT COLUMNS (DONE AUTOMATICALLY)
                # TODO: DELETE PARENT COLUMNS?
        required_changes.append(c)
    def _add_column(self, column):
        cname = column.names["."]
        command = None
        if column.type == "nested":
            # WE ARE ALSO NESTING
            command = self._nest_column(column, cname)

        table = concat_field(self.fact, column.nested_path[0])

        self.db.execute(
            "ALTER TABLE " + quote_table(table) +
            " ADD COLUMN " + quote_column(column.es_column) + " " + sql_types[column.type]
        )

        self.add_column_to_schema(column)
        if command:
            return command

    def _nest_column(self, column, new_path):
        destination_table = concat_field(self.fact, new_path)
        existing_table = concat_field(self.fact, column.nested_path[0])

        # FIND THE INNER COLUMNS WE WILL BE MOVING
        moving_columns = []
        for c in self.columns:
            if startswith_field(c.names["."], new_path):
                moving_columns.append(c)
                c.nested_path = [new_path]

        # TODO: IF THERE ARE CHILD TABLES, WE MUST UPDATE THEIR RELATIONS TOO?

        # DEFINE A NEW TABLE?
        # LOAD THE COLUMNS
        command = "PRAGMA table_info(" + quote_table(destination_table) + ")"
        details = self.db.query(command)
        if details.data:
            raise Log.error("not expected, new nesting!")

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

        if len(moving_columns) == 1:
            has_nested_data = quote_table(moving_columns[0].es_column) + """ IS NOT NULL"""
        else:
            has_nested_data = (
                "COALESCE(" +
                ",".join(quote_table(c.es_column) for c in moving_columns) +
                ") IS NOT NULL"
            )

        # FILL TABLE WITH EXISTING COLUMN DATA
        command = (
            "INSERT INTO " + quote_table(destination_table) + "(\n" +
            ",\n".join(
                [quoted_UID, quoted_PARENT, quote_table(ORDER)] +
                [quote_column(c.es_column) for c in moving_columns]
            ) +
            "\n)\n" +
            "\nSELECT " + ",".join(
                [quoted_UID, quoted_UID, "0"] +
                [quote_column(c.es_column) for c in moving_columns]
            ) +
            "\nFROM " + quote_table(existing_table) +
            "\nWHERE " + has_nested_data
        )
        
        return [command, moving_columns]

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

    def remove_col_from_db(self, col):
        org_table = col.es_index
        column = col.es_column
        tmp_table = "tmp_" + col.es_index
        columns = self.db.query("select * from " + quote_table(org_table) + "LIMIT 0").header
        self.db.execute(
            "ALTER TABLE " + quote_table(org_table) +
            " RENAME TO " + quote_table(tmp_table)
        )
        self.db.execute(
            "CREATE TABLE " + quote_table(org_table) +
            " AS SELECT " + (", ".join([quote_table(c) for c in columns if c!=column])) +
            " FROM " + quote_table(tmp_table)
        )
        self.db.execute("DROP TABLE " + quote_table(tmp_table))


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
    A Schema MAPS ALL COLUMNS IN SNOWFLAKE FROM THE PERSPECTIVE OF A SINGLE TABLE
    """

    def __init__(self, nested_path):
        self.map = {}
        self.nested_path = nested_path

    def add(self, column_name, column):
        if column_name!=column.names[self.nested_path[0]]:
            Log.error("Logic error")

        container = self.map.get(column_name)
        if not container:
            container = self.map[column_name] = []
        container.append(column)

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
        return column.names[self.nested_path[0]]

    def keys(self):
        return self.map.keys()

    def items(self):
        return self.map.items()

    @property
    def columns(self):
        return [c for cs in self.map.values() for c in cs]
