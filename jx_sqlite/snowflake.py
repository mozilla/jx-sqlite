# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import OrderedDict
from copy import copy

import jx_base
from jx_base import STRUCT, OBJECT, EXISTS, STRING, Facts
from jx_base.queries import get_property_name
from jx_python.meta import Column, ColumnList
from jx_sqlite import typed_column, UID, quoted_UID, quoted_GUID, sql_types, quoted_PARENT, quoted_ORDER, GUID, untyped_column
from mo_dots import relative_field, listwrap, wrap, startswith_field, concat_field, Null, coalesce, set_default, tail_field
from mo_future import text_type
from mo_logs import Log
from pyLibrary.sql import SQL_FROM, sql_iso, sql_list, SQL_LIMIT, SQL_SELECT, SQL_ZERO, SQL_STAR
from pyLibrary.sql.sqlite import quote_column


class Namespace(jx_base.Namespace):
    """
    MANAGE SQLITE DATABASE
    """
    def __init__(self, db):
        self.db = db
        self.tables = OrderedDict()  # MAP FROM NESTED PATH TO Table OBJECT, PARENTS PROCEED CHILDREN
        self._columns = self.read_db()

    def read_db(self):
        """
        PULL SCHEMA FROM DATABASE, BUILD THE MODEL
        :return: None
        """

        columns = ColumnList()

        # FIND ALL TABLES
        result = self.db.query("SELECT * FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = wrap([{k: d[i] for i, k in enumerate(result.header)} for d in result.data])
        for table in tables:
            if table.name.startswith("__"):
                continue
            base_table, nested_path = tail_field(table.name)

            # LOAD THE COLUMNS
            command = "PRAGMA table_info"+sql_iso(quote_column(table.name))
            details = self.db.query(command)

            for cid, name, dtype, notnull, dfft_value, pk in details.data:
                if name.startswith("__"):
                    continue
                cname, ctype = untyped_column(name)
                columns.add(Column(
                    names={'.': cname},  # I THINK COLUMNS HAVE THIER FULL PATH
                    jx_type=coalesce(ctype, {"TEXT": "string", "REAL": "number", "INTEGER": "integer"}.get(dtype)),
                    nested_path=nested_path,
                    es_type=dtype,
                    es_column=name,
                    es_index=table.name
                ))
        return columns

    def create_snowflake(self, fact_name, uid=UID):
        """
        MAKE NEW TABLE WITH GIVEN guid
        :param fact_name:  NAME FOR THE CENTRAL FACTS
        :param uid: name, or list of names, for the GUID
        :return: Facts
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
                    jx_type="string",
                    es_column=typed_column(u, "string"),
                    es_index=fact_name
                )
                self.add_column_to_schema(c)
                new_columns.append(c)

        command = (
            "CREATE TABLE " + quote_column(fact_name) + sql_iso(sql_list(
                [quoted_GUID + " TEXT "] +
                [quoted_UID + " INTEGER"] +
                [quote_column(c.es_column) + " " + sql_types[c.jx_type] for c in self.tables["."].schema.columns] +
                ["PRIMARY KEY " + sql_iso(sql_list(
                    [quoted_GUID] +
                    [quoted_UID] +
                    [quote_column(c.es_column) for c in self.tables["."].schema.columns]
                ))]
            ))
        )

        self.db.execute(command)

        snowflake = Snowflake(fact_name, self)
        return Facts(self, snowflake)


class Snowflake(jx_base.Snowflake):
    """
    MANAGE SQLITE DATABASE
    """
    def __init__(self, fact_name, namespace):
        self.fact_name = fact_name  # THE CENTRAL FACT TABLE
        self.namespace = namespace

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
            self._nest_column(column, [cname]+column.nested_path)

        table = concat_field(self.fact, column.nested_path[0])

        self.db.execute(
            "ALTER TABLE " + quote_column(table) +
            " ADD COLUMN " + quote_column(column.es_column) + " " + sql_types[column.type]
        )

        self.add_column_to_schema(column)

    def _nest_column(self, column, new_path):
        destination_table = concat_field(self.fact, new_path[0])
        existing_table = concat_field(self.fact, column.nested_path[0])

        # FIND THE INNER COLUMNS WE WILL BE MOVING
        moving_columns = []
        for c in self._columns:
            if destination_table!=column.es_index and column.es_column==c.es_column:
                moving_columns.append(c)
                c.nested_path = new_path

        # TODO: IF THERE ARE CHILD TABLES, WE MUST UPDATE THEIR RELATIONS TOO?

        # DEFINE A NEW TABLE?
        # LOAD THE COLUMNS
        command = "PRAGMA table_info"+sql_iso(quote_column(destination_table))
        details = self.db.query(command)
        if not details.data:
            command = (
                "CREATE TABLE " + quote_column(destination_table) + sql_iso(sql_list([
                    quoted_UID + "INTEGER",
                    quoted_PARENT + "INTEGER",
                    quoted_ORDER + "INTEGER",
                    "PRIMARY KEY " + sql_iso(quoted_UID),
                    "FOREIGN KEY " + sql_iso(quoted_PARENT) + " REFERENCES " + quote_column(existing_table) + sql_iso(quoted_UID)
                ]))
            )
            self.db.execute(command)
            self.add_table_to_schema(new_path)

        # TEST IF THERE IS ANY DATA IN THE NEW NESTED ARRAY
        if not moving_columns:
            return

        column.es_index = destination_table
        self.db.execute(
            "ALTER TABLE " + quote_column(destination_table) +
            " ADD COLUMN " + quote_column(column.es_column) + " " + sql_types[column.type]
        )

        # Deleting parent columns
        for col in moving_columns:
            column = col.es_column
            tmp_table = "tmp_" + existing_table
            columns = list(map(text_type, self.db.query(SQL_SELECT + SQL_STAR + SQL_FROM + quote_column(existing_table) + SQL_LIMIT + SQL_ZERO).header))
            self.db.execute(
                "ALTER TABLE " + quote_column(existing_table) +
                " RENAME TO " + quote_column(tmp_table)
            )
            self.db.execute(
                "CREATE TABLE " + quote_column(existing_table) + " AS " +
                SQL_SELECT + sql_list([quote_column(c) for c in columns if c != column]) +
                SQL_FROM + quote_column(tmp_table)
            )
            self.db.execute("DROP TABLE " + quote_column(tmp_table))

    def add_table_to_schema(self, nested_path):
        table = Table(nested_path)
        self.tables[table.name] = table
        path = table.name

        for c in self._columns:
            rel_name = c.names[path] = relative_field(c.names["."], path)
            table.schema.add(rel_name, c)
        return table

    @property
    def columns(self):
        return self._columns

    def add_column_to_schema(self, column):
        self._columns.append(column)
        abs_name = column.names["."]

        for table in self.tables.values():
            rel_name = column.names[table.name] = relative_field(abs_name, table.name)
            table.schema.add(rel_name, column)
            table.columns.append(column)


class Table(jx_base.Table):

    def __init__(self, nested_path):
        self.nested_path = nested_path
        self._schema = Schema(nested_path)
        self.columns = []  # PLAIN DATABASE COLUMNS

    @property
    def name(self):
        """
        :return: THE TABLE NAME RELATIVE TO THE FACT TABLE
        """
        return self.nested_path[0]

    @property
    def schema(self):
        return self._schema


class Schema(object):
    """
    A Schema MAPS ALL COLUMNS IN SNOWFLAKE FROM THE PERSPECTIVE OF A SINGLE TABLE (a nested_path)
    """

    def __init__(self, nested_path):
        if nested_path[-1] != '.':
            Log.error("Expecting full nested path")
        source = Column(
            names={".": "."},
            jx_type=OBJECT,
            es_type=OBJECT,
            es_column="_source",
            es_index=nested_path,
            nested_path=nested_path
        )
        guid = Column(
            names={".": GUID},
            jx_type=STRING,
            es_type='TEXT',
            es_column=GUID,
            es_index=nested_path,
            nested_path=nested_path
        )
        self.namespace = {".": {source}, GUID: {guid}}
        self._columns = [source, guid]
        self.nested_path = nested_path

    def add(self, column_name, column):
        if column_name != column.names[self.nested_path[0]]:
            Log.error("Logic error")

        self._columns.append(column)

        for np in self.nested_path:
            rel_name = column.names[np]
            container = self.namespace.setdefault(rel_name, set())
            hidden = [
                c
                for c in container
                if len(c.nested_path[0]) < len(np)
            ]
            for h in hidden:
                container.remove(h)

            container.add(column)

        container = self.namespace.setdefault(column.es_column, set())
        container.add(column)


    def remove(self, column_name, column):
        if column_name != column.names[self.nested_path[0]]:
            Log.error("Logic error")

        self.namespace[column_name] = [c for c in self.namespace[column_name] if c != column]

    def __getitem__(self, item):
        output = self.namespace.get(item, Null)
        return output

    def __copy__(self):
        output = Schema(self.nested_path)
        for k, v in self.namespace.items():
            output.namespace[k] = copy(v)
        return output

    def get_column_name(self, column):
        """
        RETURN THE COLUMN NAME, FROM THE PERSPECTIVE OF THIS SCHEMA
        :param column:
        :return: NAME OF column
        """
        return get_property_name(column.names[self.nested_path[0]])

    def keys(self):
        return set(self.namespace.keys())

    def items(self):
        return list(self.namespace.items())

    @property
    def columns(self):
        return [c for c in self._columns if c.es_column not in [GUID, '_source']]

    def leaves(self, prefix):
        head = self.namespace.get(prefix, None)
        if not head:
            return Null
        full_name = list(head)[0].names['.']
        return set(
            c
            for k, cs in self.namespace.items()
            if startswith_field(k, full_name) and k != GUID or k == full_name
            for c in cs
            if c.jx_type not in [OBJECT, EXISTS]
        )

    def map_to_sql(self, var=""):
        """
        RETURN A MAP FROM THE RELATIVE AND ABSOLUTE NAME SPACE TO COLUMNS
        """
        origin = self.nested_path[0]
        if startswith_field(var, origin) and origin != var:
            var = relative_field(var, origin)
        fact_dict = {}
        origin_dict = {}
        for k, cs in self.namespace.items():
            for c in cs:
                if c.jx_type in STRUCT:
                    continue

                if startswith_field(get_property_name(k), var):
                    origin_dict.setdefault(c.names[origin], []).append(c)

                    if origin != c.nested_path[0]:
                        fact_dict.setdefault(c.names["."], []).append(c)
                elif origin == var:
                    origin_dict.setdefault(concat_field(var, c.names[origin]), []).append(c)

                    if origin != c.nested_path[0]:
                        fact_dict.setdefault(concat_field(var, c.names["."]), []).append(c)

        return set_default(origin_dict, fact_dict)

