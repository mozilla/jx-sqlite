from mo_dots import join_field, relative_field, listwrap
from mo_dots import literal_field, split_field

from jx_sqlite import quote_table, typed_column, GUID, quoted_UID, sql_types, _quote_column
from jx_sqlite import untyped_column
from pyLibrary.queries.meta import Column


class Snowflake(object):
    """
    MANAGE SQLITE DATABASE
    """
    def __init__(self, db):
        self.db = db
        self.columns = []

    def read_db(self):
        """
        PULL SCHEMA FROM DATABASE, BUILD THE MODEL
        :return: None
        """

        # FIND ALL TABLES
        tables = self.db.query("SELECT * FROM sqlite_master WHERE type='table'")
        for table in tables:
            # LOAD THE COLUMNS
            command = "PRAGMA table_info(" + quote_table(table.name) + ")"
            details = self.db.query(command)

            for r in details:
                cname = untyped_column(r[1])
                ctype = r[2].lower()
                column = Column(
                    names={table.name: cname},
                    type=ctype,
                    nested_path=['.'],
                    es_column=typed_column(cname, ctype),
                    es_index=table.name
                )

                self.add_column_to_schema(self.columns, column)
                # TODO: FOR ALL TABLES, FIND THE MAX ID

        pass

    def create_table(self, table_name, guid):
        """
        MAKE NEW TABLE WITH GIVEN guid
        :param guid: name, or list of names, for the GUID
        :return: None
        """
        guid = listwrap(guid)

        for u in guid:
            if u == GUID:
                pass
            else:
                c = Column(
                    names={table_name: u},
                    type="string",
                    es_column=typed_column(u, "string"),
                    es_index=table_name
                )
                self.add_column_to_schema(self.nested_tables, c)

        command = (
            "CREATE TABLE " + quote_table(table_name) + "(" +
            (",".join(
                [quoted_UID + " INTEGER"] +
                [_quote_column(c) + " " + sql_types[c.type] for u, cs in self.columns.items() for c in cs]
            )) +
            ", PRIMARY KEY (" +
            (", ".join(
                [quoted_UID] +
                [_quote_column(c) for u in self.uid for c in self.columns[u]]
            )) +
            "))"
        )

        self.db.execute(command)


    def add_column(self, ):
        pass



    def add_column_to_schema(self, nest_to_schema, column):
        abs_table = literal_field(self.name)
        abs_name = column.names[abs_table]

        for nest, schema in nest_to_schema.items():
            rel_table = literal_field(join_field([self.name] + split_field(nest)))
            rel_name = relative_field(abs_name, nest)

            column.names[rel_table] = rel_name

