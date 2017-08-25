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

from future.utils import text_type
from jx_python import jx
from jx_sqlite import UID, quote_table, get_column, _make_column_name, sql_text_array_to_set, STATS, sql_aggs, PARENT, ColumnMapping, untyped_column
from mo_dots import listwrap, coalesce, split_field, join_field, startswith_field, relative_field, concat_field, Data, wrap
from mo_logs import Log
from mo_math import Math

from jx_base.domains import DefaultDomain, TimeDomain, DurationDomain, UnitDomain
from jx_sqlite.expressions import Variable, sql_type_to_json_type, TupleOp
from jx_sqlite.setop_table import SetOpTable
from pyLibrary.sql.sqlite import quote_value

class EdgesTable(SetOpTable):
    def _edges_op(self, query, frum):
        query = query.copy()  # WE WILL BE MARKING UP THE QUERY
        index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        outer_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
        frum_path = split_field(frum)
        base_table = join_field(frum_path[0:1])
        path = join_field(frum_path[1:])
        nest_to_alias = {
            nested_path: "__" + unichr(ord('a') + i) + "__"
            for i, (nested_path, sub_table) in enumerate(self.sf.tables.items())
        }

        schema = self.sf.tables[relative_field(frum, self.sf.fact)].schema

        tables = []
        for n, a in nest_to_alias.items():
            if startswith_field(path, n):
                tables.append({"nest": n, "alias": a})
        tables = jx.sort(tables, {"value": {"length": "nest"}})

        from_sql = join_field([base_table] + split_field(tables[0].nest)) + " " + tables[0].alias
        previous = tables[0]
        for t in tables[1::]:
            from_sql += (
                "\nLEFT JOIN\n" + quote_table(concat_field(base_table, t.nest)) + " " + t.alias +
                " ON " + t.alias + "." + PARENT + " = " + previous.alias + "." + UID
            )

        main_filter = query.where.to_sql(schema, boolean=True)[0].sql.b

        # SHIFT THE COLUMN DEFINITIONS BASED ON THE NESTED QUERY DEPTH
        ons = []
        join_types = []
        wheres = []
        not_ons = ["__exists__ IS NULL"]
        groupby = []
        not_groupby = []
        orderby = []
        domains = []
        select_clause = [
            "1 __exists__"  # USED TO DISTINGUISH BETWEEN NULL-BECAUSE-LEFT-JOIN OR NULL-BECAUSE-NULL-VALUE
        ]

        for edge_index, query_edge in enumerate(query.edges):
            edge_alias = "e" + text_type(edge_index)

            if query_edge.value:
                edge_values = [p for c in query_edge.value.to_sql(schema).sql for p in c.items()]

            elif not query_edge.value and any(query_edge.domain.partitions.where):
                case = "CASE "
                for pp, p in enumerate(query_edge.domain.partitions):
                    w = p.where.to_sql(schema)[0].sql.b
                    t = quote_value(pp)
                    case += " WHEN " + w + " THEN " + t
                case += " ELSE NULL END "   # quote value with length of partitions
                edge_values = [("n", case)]

            elif query_edge.range:
                edge_values = query_edge.range.min.to_sql(schema)[0].sql.items() + query_edge.range.max.to_sql(schema)[
                    0].sql.items()

            else:
                Log.error("Do not know how to handle")

            edge_names = []
            for column_index, (json_type, sql) in enumerate(edge_values):
                sql_name = "e" + text_type(edge_index) + "c" + text_type(column_index)
                edge_names.append(sql_name)

                num_sql_columns = len(index_to_column)
                if not query_edge.value and any(query_edge.domain.partitions.where):
                    def __(parts, num_sql_columns):
                        def _get(row):
                            return parts[row[num_sql_columns]].name

                        return _get

                    pull = __(query_edge.domain.partitions, num_sql_columns)
                else:
                    pull = get_column(num_sql_columns)

                if isinstance(query_edge.value, TupleOp):
                    query_edge.allowNulls = False
                    push_child = column_index
                    num_push_columns = len(query_edge.value.terms)
                else:
                    push_child = "."
                    num_push_columns = None

                index_to_column[num_sql_columns] = ColumnMapping(
                    is_edge=True,
                    push_name=query_edge.name,
                    push_column_name=query_edge.name,
                    push_column=edge_index,
                    num_push_columns=num_push_columns,
                    push_child=push_child,  # CAN NOT HANDLE TUPLES IN COLUMN
                    pull=pull,
                    sql=sql,
                    type=sql_type_to_json_type[json_type],
                    column_alias=sql_name
                )

            vals = [v for t, v in edge_values]
            if query_edge.domain.type == "set":
                domain_name = "d" + text_type(edge_index) + "c" + text_type(column_index)
                domain_names = [domain_name]
                if len(edge_names) > 1:
                    Log.error("Do not know how to handle")
                if query_edge.value:
                    domain = "\nUNION ALL\n".join(
                        "SELECT " + quote_value(coalesce(p.dataIndex, i)) + " AS rownum, " + quote_value(
                            p.value) + " AS " + domain_name
                        for i, p in enumerate(query_edge.domain.partitions)
                    )
                    if query_edge.allowNulls:
                        domain += "\nUNION ALL\nSELECT " + quote_value(
                            len(query_edge.domain.partitions)) + " AS rownum, NULL AS " + domain_name
                    where = None
                    join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                    on_clause = (
                    " OR ".join(
                        edge_alias + "." + k + " = " + v
                        for k, v in zip(domain_names, vals)
                        ) +
                    " OR (" +
                    edge_alias + "." + domain_names[0] + " IS NULL AND " +
                    " AND ".join(v + " IS NULL" for v in vals) +
                    ")"
                    )
                    not_on_clause = None
                else:
                    domain = "\nUNION ALL\n".join(
                        "SELECT " + quote_value(pp) + " AS " + domain_name for pp, p in
                        enumerate(query_edge.domain.partitions)
                    )
                    where = None
                    join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                    on_clause = " AND ".join(
                        edge_alias + "." + k + " = " + sql
                        for k, (t, sql) in zip(domain_names, edge_values)
                    )
                    not_on_clause = None
            elif query_edge.domain.type == "range":
                domain_name = "d" + text_type(edge_index) + "c0"
                domain_names = [domain_name]  # ONLY EVER SEEN ONE DOMAIN VALUE, DOMAIN TUPLES CERTAINLY EXIST
                d = query_edge.domain
                if d.max == None or d.min == None or d.min == d.max:
                    Log.error("Invalid range: {{range|json}}", range=d)
                if len(edge_names) == 1:
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    limit = Math.min(query.limit, query_edge.domain.limit)
                    domain += "\nORDER BY \n" + ",\n".join(vals) + \
                              "\nLIMIT " + text_type(limit)

                    where = None
                    join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                    on_clause = " AND ".join(
                        edge_alias + "." + k + " <= " + v + " AND " + v + " < (" + edge_alias + "." + k + " + " + text_type(
                            d.interval) + ")"
                        for k, (t, v) in zip(domain_names, edge_values)
                    )
                    not_on_clause = None
                elif query_edge.range:
                    query_edge.allowNulls = False
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    limit = Math.min(query.limit, query_edge.domain.limit)
                    domain += "\nORDER BY \n" + ",\n".join(vals) + \
                              "\nLIMIT " + text_type(limit)
                    where = None
                    join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                    on_clause = edge_alias + "." + domain_name + " < " + edge_values[1][1] + " AND " + \
                                edge_values[0][1] + " < (" + edge_alias + "." + domain_name + " + " + text_type(
                        d.interval) + ")"
                    not_on_clause = None
                else:
                    Log.error("do not know how to handle")
                    # select_clause.extend(v[0] + " " + k for k, v in zip(domain_names, edge_values))
            elif len(edge_names) > 1:
                domain_names = ["d" + text_type(edge_index) + "c" + text_type(i) for i, _ in enumerate(edge_names)]
                query_edge.allowNulls = False
                domain_columns = [c for c in self.sf.columns if quote_table(c.es_column) in vals]
                if not domain_columns:
                    domain_nested_path = "."
                    Log.note("expecting a known column")
                else:
                    domain_nested_path = domain_columns[0].nested_path
                domain_table = quote_table(concat_field(self.sf.fact, domain_nested_path[0]))
                domain = (
                    "\nSELECT " + ",\n".join(g + " AS " + n for n, g in zip(domain_names, vals)) +
                    "\nFROM\n" + domain_table + " " + nest_to_alias["."] +
                    "\nGROUP BY\n" + ",\n".join(vals)
                )
                limit = Math.min(query.limit, query_edge.domain.limit)
                domain += (
                    "\nORDER BY COUNT(1) DESC" +
                    "\nLIMIT " + text_type(limit)
                )
                where = None
                join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                on_clause = " AND ".join(
                    "((" + edge_alias + "." + k + " IS NULL AND " + v + " IS NULL) OR " + edge_alias + "." + k + " = " + v + ")"
                    for k, v in zip(domain_names, vals)
                )
                not_on_clause = None
            elif isinstance(query_edge.domain, DefaultDomain):
                domain_names = ["d" + text_type(edge_index) + "c" + text_type(i) for i, _ in enumerate(edge_names)]
                domain_columns = [c for c in self.sf.columns if quote_table(c.es_column) in vals]
                if not domain_columns:
                    domain_nested_path = "."
                    Log.note("expecting a known column")
                else:
                    domain_nested_path = domain_columns[0].nested_path
                domain_table = quote_table(concat_field(self.sf.fact, domain_nested_path[0]))
                domain = (
                    "\nSELECT " + ",".join(domain_names) + " FROM ("
                                                           "\nSELECT " + ",\n".join(
                        g + " AS " + n for n, g in zip(domain_names, vals)) +
                    "\nFROM\n" + domain_table + " " + nest_to_alias["."]
                )
                if not query_edge.allowNulls:
                    domain +=  "\nWHERE\n" + " AND ".join(g + " IS NOT NULL" for g in vals)

                domain += "\nGROUP BY\n" + ",\n".join(g for g in vals)

                limit = Math.min(query.limit, query_edge.domain.limit)
                domain += (
                    "\nORDER BY \n" + ",\n".join("COUNT(1) DESC" for g in vals) +
                    "\nLIMIT " + text_type(limit)
                )
                domain += ")"

                where = None
                join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                on_clause = (
                    # "__exists__ AND " +
                    " OR ".join(  # "OR" IS FOR MATCHING DIFFERENT TYPES OF SAME NAME
                        edge_alias + "." + k + " = " + v
                        for k, v in zip(domain_names, vals)
                    ) +
                    " OR (" +
                    edge_alias + "." + domain_names[0] + " IS NULL AND " +
                    " AND ".join(v + " IS NULL" for v in vals) +
                    ")"
                )
                not_on_clause = None

            elif isinstance(query_edge.domain, (DurationDomain, TimeDomain)):
                domain_name = "d" + text_type(edge_index) + "c0"
                domain_names = [domain_name]
                d = query_edge.domain
                if d.max == None or d.min == None or d.min == d.max:
                    Log.error("Invalid time domain: {{range|json}}", range=d)
                if len(edge_names) == 1:
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    if query_edge.allowNulls:
                        domain += "\nUNION ALL SELECT NULL AS " + domain_name + "\n"
                    on_clause = (
                        " AND ".join(
                            edge_alias + "." + k + " <= " + v + " AND " +
                            v + " < (" + edge_alias + "." + k + " + " + quote_value(d.interval) + ")"
                            for k, (t, v) in zip(domain_names, edge_values)
                        ) +
                        " OR (" + " AND ".join(
                            edge_alias + "." + k + " IS NULL AND " + v + " IS NULL"
                            for k, v in zip(domain_names, vals)
                        ) + ")"
                    )
                    where = None
                    join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                    if query_edge.allowNulls:
                        not_on_clause = None
                    else:
                        not_on_clause = " AND ".join(edge_alias + "." + k + " IS NOT NULL" for k in domain_names)
                elif query_edge.range:
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    on_clause = edge_alias + "." + domain_name + " < " + edge_values[1][1] + " AND " + \
                                edge_values[0][1] + " < (" + edge_alias + "." + domain_name + " + " + quote_value(
                        d.interval) + ")"
                    # not_on_clause = "__exists__ IS NULL"
                else:
                    Log.error("do not know how to handle")
            else:
                Log.error("not handled")

            domains.append(domain)
            ons.append(on_clause)
            wheres.append(where)
            join_types.append(join_type)
            if not_on_clause:
                not_ons.append(not_on_clause)

            # groupby.append(",\n".join(nest_to_alias["."] + "." + g for g in vals))
            groupby.append(",\n".join(edge_alias + "." + d for d in domain_names))
            not_groupby.append(",\n".join(edge_alias + "." + d for d in domain_names))

            for n, k in enumerate(domain_names):
                outer_selects.append(edge_alias + "." + k + " AS " + k)

                orderby.append(k + " IS NULL")
                if query.sort[n].sort == -1:
                    orderby.append(k + " DESC ")
                else:
                    orderby.append(k)

        offset = len(query.edges)
        for ssi, s in enumerate(listwrap(query.select)):
            si = ssi + offset
            if isinstance(s.value, Variable) and s.value.var == "." and s.aggregate == "count":
                # COUNT RECORDS, NOT ANY ONE VALUE
                sql = "COUNT(__exists__) AS " + quote_table(s.name)

                column_number = len(outer_selects)
                outer_selects.append(sql)
                index_to_column[column_number] = ColumnMapping(
                    push_name=s.name,
                    push_column_name=s.name,
                    push_column=si,
                    push_child=".",
                    pull=get_column(column_number),
                    sql=sql,
                    column_alias=quote_table(s.name),
                    type=sql_type_to_json_type["n"]
                )
            elif s.aggregate == "count" and (not query.edges and not query.groupby):
                value = s.value.var
                columns = [c.es_column for c in self.sf.columns if untyped_column(c.es_column)[0] == value]
                sql = " + ".join("COUNT(" + quote_table(col) + ")" for col in columns)
                column_number = len(outer_selects)
                outer_selects.append(sql + " AS " + _make_column_name(column_number))
                index_to_column[column_number] = ColumnMapping(
                    push_name=s.name,
                    push_column_name=s.name,
                    push_column=si,
                    push_child=".",
                    pull=get_column(column_number),
                    sql=sql,
                    column_alias=_make_column_name(column_number),
                    type=sql_type_to_json_type["n"]
                )
            elif s.aggregate == "percentile":
                if not isinstance(s.percentile, (int, float)):
                    Log.error("Expecting percentile to be a float between 0 and 1")

                Log.error("not implemented")
            elif s.aggregate == "cardinality":
                for details in s.value.to_sql(schema):
                    for json_type, sql in details.sql.items():
                        column_number = len(outer_selects)
                        count_sql = "COUNT(DISTINCT(" + sql + ")) AS " + _make_column_name(column_number)
                        outer_selects.append(count_sql)
                        index_to_column[column_number] = ColumnMapping(
                            push_name=s.name,
                            push_column_name=s.name,
                            push_column=si,
                            push_child=".",
                            pull=get_column(column_number),
                            sql=count_sql,
                            column_alias=_make_column_name(column_number),
                            type=sql_type_to_json_type[json_type]
                        )
            elif s.aggregate == "union":
                for ei, details in enumerate(s.value.to_sql(schema)):
                    column_number = len(outer_selects)
                    union_table_alias = "t" + text_type(column_number)

                    union_values = []

                    for ci, (json_type, sql) in enumerate(details.sql.items()):
                        cname = _make_column_name(column_number) + "d" + text_type(ci) + "c" + text_type(ci)
                        agg = "JSON_GROUP_ARRAY(DISTINCT(" + sql + "))  AS " + cname
                        union_values.append(cname)
                        union_join_sql = "SELECT " + ", ".join([agg] + groupby)
                        union_prev_table = None

                        # TODO: FIX WHATEVER IS SETTING THE nested_path WRONG
                        nested_path = listwrap(details.nested_path)
                        if nested_path.last() != ".":
                            Log.warning("bad nested path")
                            nested_path.append(".")

                        for p in jx.reverse(nested_path):
                            union_next_table = quote_table(concat_field(base_table, p))
                            if p == ".":
                                union_join_sql += " FROM " + union_next_table + " AS " + union_table_alias
                            else:
                                union_join_sql += (
                                    "\nLEFT JOIN " + union_next_table +
                                    "\nON " + union_next_table + "." + PARENT + " = " + union_prev_table + "." + UID
                                )
                            union_prev_table = union_next_table
                        union_join_sql += (
                            "\nWHERE " + main_filter + " AND (" + sql + ") IS NOT NULL" +
                            ("\nGROUP BY " + ",\n".join(groupby) if groupby else "")
                        )

                        domains.append("(" + union_join_sql + ") AS " + union_table_alias)
                        if not groupby:
                            ons.append("1=1")
                        else:
                            ons.append(" AND ".join(union_table_alias + "." + g + "=" + tables[0].alias + "." + g for g in groupby))
                            # Log.error("do not know how to handle yet")
                        join_types.append("LEFT JOIN")
                        if not query.edges:
                            query.edges = []
                        query.edges.append(Data(allowNulls=False, domain=UnitDomain()))

                    if len(union_values) > 1:
                        union_select_sql = "CONCAT('[', " + ", ',', ".join([
                            "LTRIM(RTRIM(MAX("+cname+"), ']'), '[')"
                            for cname in union_values
                        ])+") AS "+_make_column_name(column_number)
                    else:
                        union_select_sql = "MAX("+union_table_alias + "." + _make_column_name(column_number) + "d0c0) AS " + _make_column_name(column_number)

                    outer_selects.append(union_select_sql)
                    index_to_column[column_number] = ColumnMapping(
                        push_name=s.name,
                        push_column_name=s.name,
                        push_column=si,
                        push_child=".",
                        pull=sql_text_array_to_set(column_number),
                        sql=union_select_sql,
                        column_alias=_make_column_name(column_number),
                        type=sql_type_to_json_type["j"]
                    )

            elif s.aggregate == "stats":  # THE STATS OBJECT
                for details in s.value.to_sql(schema):
                    sql = details.sql["n"]
                    for name, code in STATS.items():
                        full_sql = code.replace("{{value}}", sql)
                        column_number = len(outer_selects)
                        outer_selects.append(full_sql + " AS " + _make_column_name(column_number))
                        index_to_column[column_number] = ColumnMapping(
                            push_name=s.name,
                            push_column_name=s.name,
                            push_column=si,
                            push_child=name,
                            pull=get_column(column_number),
                            sql=full_sql,
                            column_alias=_make_column_name(column_number),
                            type="number"
                        )
            else:  # STANDARD AGGREGATES
                for details in s.value.to_sql(schema):
                    for sql_type, sql in details.sql.items():
                        column_number = len(outer_selects)
                        sql = sql_aggs[s.aggregate] + "(" + sql + ")"
                        if s.default != None:
                            sql = "COALESCE(" + sql + ", " + quote_value(s.default) + ")"
                        outer_selects.append(sql + " AS " + _make_column_name(column_number))
                        index_to_column[column_number] = ColumnMapping(
                            push_name=s.name,
                            push_column_name=s.name,
                            push_column=si,
                            push_child=".",  # join_field(split_field(details.name)[1::]),
                            pull=get_column(column_number),
                            sql=sql,
                            column_alias=_make_column_name(column_number),
                            type=sql_type_to_json_type[sql_type]
                        )

        for w in query.window:
            outer_selects.append(self._window_op(self, query, w))

        all_parts = []

        primary = (
            "(" +
            "\nSELECT\n" + ",\n".join(select_clause) + ",\n" + "*" +
            "\nFROM " + from_sql +
            "\nWHERE " + main_filter +
            ") " + nest_to_alias["."]
        )
        edge_sources = []
        for edge_index, query_edge in enumerate(query.edges):
            edge_alias = "e" + text_type(edge_index)
            domain = domains[edge_index]
            edge_sources.append(Data(
                isNull = query_edge.allowNulls,
                sql="(" + domain + ") AS " + edge_alias
            ))

        # COORDINATES OF ALL primary DATA
        part = (
            "SELECT " + (",\n".join(outer_selects)) +
            "\nFROM\n" + primary
        )
        for t, s, j in zip(join_types, edge_sources, ons):
            part += " " + t + "\n" + s.sql + " ON " + j
        if any(wheres):
            part += "\nWHERE " + " AND ".join("(" + w + ")" for w in wheres if w)
        if groupby:
            part += "\nGROUP BY\n" + ",\n".join(groupby)
        all_parts.append(part)

        missing_domain = [es.sql for es in edge_sources if es.isNull]
        if missing_domain:
            # ALL COORDINATES MISSED BY primary DATA
            part = "SELECT " + (",\n".join(outer_selects)) + "\nFROM\n" + missing_domain[0]
            for s in missing_domain[1:]:
                part += "\nLEFT JOIN\n" + s + "\nON 1=1\n"
            part += "\nLEFT JOIN\n" + primary + "\nON (" + ") AND (".join(ons) + ")"
            part += "\nWHERE " + " AND ".join("(" + w + ")" for w in not_ons if w)
            if groupby:
                part += "\nGROUP BY\n" + ",\n".join(groupby)
            all_parts.append(part)

        command = "SELECT * FROM (\n" + "\nUNION ALL\n".join(all_parts) + "\n)"

        if orderby:
            command += "\nORDER BY\n" + ",\n".join(orderby)

        return command, index_to_column


    def _make_range_domain(self, domain, column_name):
        width = (domain.max - domain.min) / domain.interval
        digits = Math.floor(Math.log10(width - 1))
        if digits == 0:
            value = "a.value"
        else:
            value = "+".join("1" + ("0" * j) + "*" + text_type(chr(ord(b'a') + j)) + ".value" for j in range(digits + 1))

        if domain.interval == 1:
            if domain.min == 0:
                domain = "SELECT " + value + " " + column_name + \
                         "\nFROM __digits__ a"
            else:
                domain = "SELECT (" + value + ") + " + quote_value(domain.min) + " " + column_name + \
                         "\nFROM __digits__ a"
        else:
            if domain.min == 0:
                domain = "SELECT " + value + " * " + quote_value(domain.interval) + " " + column_name + \
                         "\nFROM __digits__ a"
            else:
                domain = "SELECT (" + value + " * " + quote_value(domain.interval) + ") + " + quote_value(
                    domain.min) + " " + column_name + \
                         "\nFROM __digits__ a"

        for j in range(digits):
            domain += "\nJOIN __digits__ " + text_type(chr(ord(b'a') + j + 1)) + " ON 1=1"
        domain += "\nWHERE " + value + " < " + quote_value(width)
        return domain
