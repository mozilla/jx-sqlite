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

from mo_future import unichr

from jx_python import jx
from jx_sqlite import UID, quote_table, get_column, _make_column_name, sql_aggs, PARENT, ColumnMapping
from jx_sqlite.edges_table import EdgesTable
from jx_sqlite.expressions import sql_type_to_json_type
from mo_dots import listwrap, split_field, join_field, startswith_field, concat_field
from mo_logs import Log
from pyLibrary.sql import SQL_COMMA, SQL_LEFT_JOIN, SQL_WHERE, SQL_GROUPBY, SQL_SELECT, SQL_FROM, SQL_ORDERBY, SQL_ON, SQL_NULL, sql_list, SQL_IS_NULL, sql_iso, sql_count, SQL_ONE


class GroupbyTable(EdgesTable):
    def _groupby_op(self, query, frum):
        schema = self.sf.tables[join_field(split_field(frum)[1:])].schema
        index_to_column = {}
        nest_to_alias = {
            nested_path: "__" + unichr(ord('a') + i) + "__"
            for i, (nested_path, sub_table) in enumerate(self.sf.tables.items())
        }
        frum_path = split_field(frum)
        base_table = join_field(frum_path[0:1])
        path = join_field(frum_path[1:])
        tables = []
        for n, a in nest_to_alias.items():
            if startswith_field(path, n):
                tables.append({"nest": n, "alias": a})
        tables = jx.sort(tables, {"value": {"length": "nest"}})

        from_sql = join_field([base_table] + split_field(tables[0].nest)) + " " + tables[0].alias
        previous = tables[0]
        for t in tables[1::]:
            from_sql += (
                SQL_LEFT_JOIN + quote_table(concat_field(base_table, t.nest)) + " " + t.alias +
                SQL_ON + t.alias + "." + PARENT + " = " + previous.alias + "." + UID
            )

        selects = []
        groupby = []
        for i, e in enumerate(query.groupby):
            for s in e.value.to_sql(schema):
                column_number = len(selects)
                sql_type, sql = s.sql.items()[0]
                if sql == 'NULL' and not e.value.var in schema.keys():
                    Log.error("No such column {{var}}", var=e.value.var)

                column_alias = _make_column_name(column_number)
                groupby.append(sql)
                selects.append(sql + " AS " + column_alias)
                if s.nested_path == ".":
                    select_name = s.name
                else:
                    select_name = "."
                index_to_column[column_number] = ColumnMapping(
                    is_edge=True,
                    push_name=e.name,
                    push_column_name=e.name.replace("\\.", "."),
                    push_column=i,
                    push_child=select_name,
                    pull=get_column(column_number),
                    sql=sql,
                    column_alias=column_alias,
                    type=sql_type_to_json_type[sql_type]
                )

        for i, s in enumerate(listwrap(query.select)):
            column_number = len(selects)
            sql_type, sql = s.value.to_sql(schema)[0].sql.items()[0]
            if sql == 'NULL' and not s.value.var in schema.keys():
                Log.error("No such column {{var}}", var=s.value.var)

            if s.value == "." and s.aggregate == "count":
                selects.append(sql_count(SQL_ONE) + " AS " + quote_table(s.name))
            else:
                selects.append(sql_aggs[s.aggregate] + sql_iso(sql)+" AS " + quote_table(s.name))

            index_to_column[column_number] = ColumnMapping(
                push_name=s.name,
                push_column_name=s.name,
                push_column=i + len(query.groupby),
                push_child=".",
                pull=get_column(column_number),
                sql=sql,
                column_alias=quote_table(s.name),
                type=sql_type_to_json_type[sql_type]
            )

        for w in query.window:
            selects.append(self._window_op(self, query, w))

        where = query.where.to_sql(schema)[0].sql.b

        command = (
            SQL_SELECT + (sql_list(selects)) +
            SQL_FROM + from_sql +
            SQL_WHERE + where +
            SQL_GROUPBY + sql_list(groupby)
        )

        if query.sort:
            command += SQL_ORDERBY + sql_list(
                sql_iso(sql[t]) + SQL_IS_NULL + "," +
                sql[t] + (" DESC" if s.sort == -1 else "")
                for s, sql in [(s, s.value.to_sql(schema)[0].sql) for s in query.sort]
                for t in "bns" if sql[t]
            )

        return command, index_to_column
