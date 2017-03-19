# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from copy import copy

from mo_dots import Data, wrap, split_field, join_field, startswith_field, literal_field, unwrap, concat_field
from mo_logs import Log

from jx_sqlite import quote_table, quoted_UID, _quote_column, sql_types, ORDER, quoted_PARENT
from jx_sqlite.base_table import BaseTable
from pyLibrary.queries.containers import STRUCT


class AlterTable(BaseTable):
    def __del__(self):
        self.db.execute("DROP TABLE " + quote_table(self.name))


    def add_column(self, column):
        """
        ADD COLUMN, IF IT DOES NOT EXIST ALREADY
        """
        if column.name not in self.columns:
            self.columns[column.name] = {column}
        elif column.type not in [c.type for c in self.columns[column.name]]:
            self.columns[column.name].add(column)

        if column.type == "nested":
            nested_table_name = concat_field(self.name, column.name)
            # MAKE THE TABLE
            from jx_sqlite.query_table import QueryTable

            table = QueryTable(nested_table_name, self.db, exists=False)
            self.nested_tables[column.name] = table
        else:
            self.db.execute(
                "ALTER TABLE " + quote_table(self.name) + " ADD COLUMN " + _quote_column(column) + " " + column.type
            )

    def get_leaves(self, table_name=None):
        output = []
        for columns_by_type in self.columns.values():
            for c in columns_by_type:
                if c.type in STRUCT:
                    continue
                c = c.__copy__()
                c.type = "value"  # MULTI-VALUED, SO HIDE THE TYPE IN THIS GENERIC NAME
                output.append(c)
                break
        return output

    def _get_sql_schema(self, frum):
        """
        :param nest: the path to the nested sub-table
        :return: relative schema for the sub-table; change `es_index` to sql alias
        """
        # WE MUST HAVE THE ALIAS NAMES FOR THE TABLES
        nest_to_alias = {
            nested_path: "__" + unichr(ord('a') + i) + "__"
            for i, (nested_path, sub_table) in enumerate(self.nested_tables.items())
            }

        def paths(field):
            path = split_field(field)
            for i in range(len(path) + 1):
                yield join_field(path[0:i])

        columns = Data()
        for k in set(kk for k in self.columns.keys() for kk in paths(k)):
            for j, c in ((j, cc) for j, c in self.columns.items() for cc in c):
                if startswith_field(j, k):
                    if c.type in STRUCT:
                        continue
                    c = copy(c)
                    c.es_index = nest_to_alias[c.nested_path[0]]
                    columns[literal_field(k)] += [c]
        columns._db = self.db
        return unwrap(columns)

    def change_schema(self, required_changes):
        required_changes = wrap(required_changes)
        for required_change in required_changes:
            if required_change.add:
                column = required_change.add
                if column.type == "nested":
                    # WE ARE ALSO NESTING
                    self._nest_column(column, column.names[self.name])

                table = join_field([self.name] + split_field(column.nested_path[0]))

                self.db.execute(
                    "ALTER TABLE " + quote_table(table) + " ADD COLUMN " + _quote_column(column) + " " + sql_types[
                        column.type]
                )

                self.columns.add(column)

            elif required_change.nest:
                column = required_change.nest
                new_path = required_change.new_path
                self._nest_column(column, new_path)
                # REMOVE KNOWLEDGE OF PARENT COLUMNS (DONE AUTOMATICALLY)
                # TODO: DELETE PARENT COLUMNS?

    def _nest_column(self, column, new_path):
        destination_table = join_field([self.name] + split_field(new_path))
        existing_table = join_field([self.name] + split_field(column.nested_path[0]))

        # FIND THE INNER COLUMNS WE WILL BE MOVING
        new_columns = {}
        for cname, cols in self.columns.items():
            if startswith_field(cname, column.names[self.name]):
                new_columns[cname] = set()
                for col in cols:
                    new_columns[cname].add(col)
                    col.nested_path = [new_path] + col.nested_path

        # TODO: IF THERE ARE CHILD TABLES, WE MUST UPDATE THEIR RELATIONS TOO?

        # DEFINE A NEW TABLE?
        # LOAD THE COLUMNS
        command = "PRAGMA table_info(" + quote_table(destination_table) + ")"
        details = self.db.query(command)
        if details.data:
            raise Log.error("not expected, new nesting!")
        from jx_sqlite.query_table import QueryTable
        self.nested_tables[new_path] = sub_table = QueryTable(destination_table, self.db, exists=False)

        self.db.execute(
            "ALTER TABLE " + quote_table(sub_table.name) + " ADD COLUMN " + quoted_PARENT + " INTEGER"
        )
        self.db.execute(
            "ALTER TABLE " + quote_table(sub_table.name) + " ADD COLUMN " + quote_table(ORDER) + " INTEGER"
        )
        for cname, cols in new_columns.items():
            for c in cols:
                sub_table.add_column(c)

        # TEST IF THERE IS ANY DATA IN THE NEW NESTED ARRAY
        all_cols = [c for _, cols in sub_table.columns.items() for c in cols]
        if not all_cols:
            has_nested_data = "0"
        elif len(all_cols) == 1:
            has_nested_data = _quote_column(all_cols[0]) + " is NOT NULL"
        else:
            has_nested_data = "COALESCE(" + \
                              ",".join(_quote_column(c) for c in all_cols) + \
                              ") IS NOT NULL"

        # FILL TABLE WITH EXISTING COLUMN DATA
        command = "INSERT INTO " + quote_table(destination_table) + "(\n" + \
                  ",\n".join(
                      [quoted_UID, quoted_PARENT, quote_table(ORDER)] +
                      [_quote_column(c) for _, cols in sub_table.columns.items() for c in cols]
                  ) + \
                  "\n)\n" + \
                  "\nSELECT\n" + ",".join(
            [quoted_UID, quoted_UID, "0"] +
            [_quote_column(c) for _, cols in sub_table.columns.items() for c in cols]
        ) + \
                  "\nFROM\n" + quote_table(existing_table) + \
                  "\nWHERE\n" + has_nested_data
        self.db.execute(command)


