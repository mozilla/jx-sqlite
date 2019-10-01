# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

import jx_base
from jx_sqlite import quoted_ORDER, quoted_PARENT, quoted_UID
from jx_sqlite.schema import Schema
from jx_sqlite.table import Table
from mo_dots import concat_field, wrap
from mo_future import text_type
from mo_logs import Log
from pyLibrary.sql import SQL_FROM, SQL_LIMIT, SQL_SELECT, SQL_STAR, SQL_ZERO, sql_iso, sql_list
from pyLibrary.sql.sqlite import quote_column


class Snowflake(jx_base.Snowflake):
    """
    MANAGE SINGLE HIERARCHY IN SQLITE DATABASE
    """
    def __init__(self, fact_name, namespace):
        if not namespace._snowflakes[fact_name]:
            Log.error("{{name}} does not exist", name=fact_name)
        self.fact_name = fact_name  # THE CENTRAL FACT TABLE
        self.namespace = namespace
        self.column = Schema(".", self)

    def __copy__(self):
        Log.error("con not copy")

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
                column, nested_path = required_change.nest
                self._nest_column(column, nested_path)

    def _add_column(self, column):
        cname = column.name
        if column.jx_type == "nested":
            # WE ARE ALSO NESTING
            self._nest_column(column, [cname]+column.nested_path)

        table = concat_field(self.fact_name, column.nested_path[0])

        with self.namespace.db.transaction() as t:
            t.execute(
                "ALTER TABLE" + quote_column(table) +
                "ADD COLUMN" + quote_column(column.es_column) + " " + column.es_type
            )

        self.namespace.columns.add(column)

    def _nest_column(self, column, new_path):
        destination_table = concat_field(self.fact_name, new_path[0])
        existing_table = concat_field(self.fact_name, column.nested_path[0])

        # FIND THE INNER COLUMNS WE WILL BE MOVING
        moving_columns = []
        for c in self.columns:
            if destination_table != column.es_index and column.es_column == c.es_column:
                moving_columns.append(c)
                c.nested_path = new_path

        # TODO: IF THERE ARE CHILD TABLES, WE MUST UPDATE THEIR RELATIONS TOO?

        # DEFINE A NEW TABLE?
        # LOAD THE COLUMNS
        command = "PRAGMA table_info"+sql_iso(quote_column(destination_table))
        details = self.namespace.db.query(command)
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
            with self.namespace.db.transaction() as t:
                t.execute(command)
                self.add_table(new_path)

        # TEST IF THERE IS ANY DATA IN THE NEW NESTED ARRAY
        if not moving_columns:
            return

        column.es_index = destination_table
        with self.namespace.db.transaction() as t:
            t.execute(
                "ALTER TABLE " + quote_column(destination_table) +
                " ADD COLUMN " + quote_column(column.es_column) + " " + column.es_type
            )

            # Deleting parent columns
            for col in moving_columns:
                column = col.es_column
                tmp_table = "tmp_" + existing_table
                columns = list(map(text_type, t.query(SQL_SELECT + SQL_STAR + SQL_FROM + quote_column(existing_table) + SQL_LIMIT + SQL_ZERO).header))
                t.execute(
                    "ALTER TABLE " + quote_column(existing_table) +
                    " RENAME TO " + quote_column(tmp_table)
                )
                t.execute(
                    "CREATE TABLE " + quote_column(existing_table) + " AS " +
                    SQL_SELECT + sql_list([quote_column(c) for c in columns if c != column]) +
                    SQL_FROM + quote_column(tmp_table)
                )
                t.execute("DROP TABLE " + quote_column(tmp_table))

    def add_table(self, nested_path):
        query_paths = self.namespace._snowflakes[self.fact_name]
        if nested_path in query_paths:
            Log.error("table exists")
        query_paths.append(nested_path)
        return Table(nested_path, self)

    @property
    def tables(self):
        """
        :return:  LIST OF (nested_path, full_name) PAIRS
        """
        return [(path, concat_field(self.fact_name, path)) for path in self.query_paths]

    def get_schema(self, nested_path):
        return Schema(nested_path, self)

    @property
    def columns(self):
        return self.namespace.columns.find(self.fact_name)

    @property
    def query_paths(self):
        return self.namespace._snowflakes[self.fact_name]

