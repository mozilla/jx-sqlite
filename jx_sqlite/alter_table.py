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

from mo_dots import Data, split_field, join_field, startswith_field, literal_field, unwrap, concat_field, relative_field

from jx_sqlite import quote_table
from jx_sqlite.base_table import BaseTable
from pyLibrary.queries.containers import STRUCT
from pyLibrary.sql.sqlite import quote_column


class AlterTable(BaseTable):
    def __del__(self):
        self.db.execute("DROP TABLE " + quote_table(self.sf.fact))


    def add_column(self, column):
        """
        ADD COLUMN, IF IT DOES NOT EXIST ALREADY
        """
        if column.name not in self.columns:
            self.columns[column.name] = {column}
        elif column.type not in [c.type for c in self.columns[column.name]]:
            self.columns[column.name].add(column)

        if column.type == "nested":
            nested_table_name = concat_field(self.sf.fact, column.name)
            # MAKE THE TABLE
            from jx_sqlite.query_table import QueryTable

            table = QueryTable(nested_table_name, self.db, exists=False)
            self.nested_tables[column.name] = table
        else:
            self.db.execute(
                "ALTER TABLE " + quote_table(self.sf.fact) + " ADD COLUMN " + quote_column(column) + " " + column.type
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
            for i, (nested_path, sub_table) in enumerate(self.sf.tables.items())
            }

        def paths(field):
            path = split_field(field)
            for i in range(len(path) + 1):
                yield join_field(path[0:i])

        columns = Data()
        frum_columns = self.sf.tables[relative_field(frum, self.sf.fact)].columns
        for k in set(kk for k in frum_columns.keys() for kk in paths(k)):
            for j, c in ((j, cc) for j, c in frum_columns.items() for cc in c):
                if startswith_field(j, k):
                    if c.type in STRUCT:
                        continue
                    c = copy(c)
                    c.es_index = nest_to_alias[c.nested_path[0]]
                    columns[literal_field(k)] += [c]
        columns._db = self.db
        return unwrap(columns)

