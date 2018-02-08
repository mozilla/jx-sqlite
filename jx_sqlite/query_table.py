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

import mo_json
from jx_base import STRUCT
from jx_base.domains import SimpleSetDomain
from jx_base.expressions import jx_expression, Variable, TupleOp
from jx_base.query import QueryOp
from jx_python import jx
from jx_python.meta import Column
from jx_sqlite import quote_table, sql_aggs, unique_name, untyped_column
from jx_sqlite.groupby_table import GroupbyTable
from mo_collections.matrix import Matrix, index_to_coordinate
from mo_dots import listwrap, coalesce, Data, wrap, startswith_field, unwrap, unwraplist, concat_field, relative_field, Null
from mo_future import text_type
from mo_logs import Log
from pyLibrary.sql import SQL_COMMA, SQL, SQL_WHERE, SQL_FROM, SQL_SELECT


class QueryTable(GroupbyTable):
    def get_column_name(self, column):
        return column.names[self.sf.fact]

    def __len__(self):
        counter = self.db.query(SQL_SELECT+"COUNT(*)"+SQL_FROM + quote_table(self.sf.fact))[0][0]
        return counter

    def __nonzero__(self):
        counter = self.db.query(SQL_SELECT+"COUNT(*)"+SQL_FROM + quote_table(self.sf.fact))[0][0]
        return bool(counter)

    def delete(self, where):
        filter = where.to_sql()
        self.db.execute("DELETE"+SQL_FROM + quote_table(self.sf.fact) + SQL_WHERE + filter)

    def vars(self):
        return set(self.columns.keys())

    def map(self, map_):
        return self

    def where(self, filter):
        """
        WILL NOT PULL WHOLE OBJECT, JUST TOP-LEVEL PROPERTIES
        :param filter:  jx_expression filter
        :return: list of objects that match
        """
        select = []
        column_names = []
        for cname, cs in self.columns.items():
            cs = [c for c in cs if c.type not in STRUCT and len(c.nested_path) == 1]
            if len(cs) == 0:
                continue
            column_names.append(cname)
            if len(cs) == 1:
                select.append(quote_table(c.es_column) + " " + quote_table(c.name))
            else:
                select.append(
                    "coalesce(" +
                    SQL_COMMA.join(quote_table(c.es_column) for c in cs) +
                    ") " + quote_table(c.name)
                )

        result = self.db.query(
            SQL_SELECT + SQL("\n,").join(select) +
            SQL_FROM + quote_table(self.sf.fact) +
            SQL_WHERE + jx_expression(filter).to_sql()
        )
        return wrap([{c: v for c, v in zip(column_names, r)} for r in result.data])

    def query(self, query):
        """
        :param query:  JSON Query Expression, SET `format="container"` TO MAKE NEW TABLE OF RESULT
        :return:
        """
        if not startswith_field(query['from'], self.sf.fact):
            Log.error("Expecting table, or some nested table")
        frum, query['from'] = query['from'], self
        table = self.sf.tables[relative_field(frum, self.sf.fact)]
        schema = table.schema
        query = QueryOp.wrap(query, table=table, schema=schema)
        new_table = "temp_" + unique_name()

        if query.format == "container":
            create_table = "CREATE TABLE " + quote_table(new_table) + " AS "
        else:
            create_table = ""

        if query.groupby and query.format != "cube":
            op, index_to_columns = self._groupby_op(query, frum)
            command = create_table + op
        elif query.groupby:
            query.edges, query.groupby = query.groupby, query.edges
            op, index_to_columns = self._edges_op(query, frum)
            command = create_table + op
            query.edges, query.groupby = query.groupby, query.edges
        elif query.edges or any(a != "none" for a in listwrap(query.select).aggregate):
            op, index_to_columns = self._edges_op(query, frum)
            command = create_table + op
        else:
            op = self._set_op(query, frum)
            return op

        result = self.db.query(command)

        if query.format == "container":
            output = QueryTable(new_table, db=self.db, uid=self.uid, exists=True)
        elif query.format == "cube" or (not query.format and query.edges):
            column_names= [None]*(max(c.push_column for c in index_to_columns.values()) + 1)
            for c in index_to_columns.values():
                column_names[c.push_column] = c.push_column_name

            if len(query.edges) == 0 and len(query.groupby) == 0:
                data = {n: Data() for n in column_names}
                for s in index_to_columns.values():
                    data[s.push_name][s.push_child] = unwrap(s.pull(result.data[0]))
                if isinstance(query.select, list):
                    select = [{"name": s.name} for s in query.select]
                else:
                    select = {"name": query.select.name}

                return Data(
                    data=unwrap(data),
                    select=select,
                    meta={"format": "cube"}
                )

            if not result.data:
                edges = []
                dims = []
                for i, e in enumerate(query.edges + query.groupby):
                    allowNulls = coalesce(e.allowNulls, True)

                    if e.domain.type == "set" and e.domain.partitions:
                        domain = SimpleSetDomain(partitions=e.domain.partitions.name)
                    elif e.domain.type == "range":
                        domain = e.domain
                    elif isinstance(e.value, TupleOp):
                        pulls = jx.sort([c for c in index_to_columns.values() if c.push_name == e.name],
                                        "push_child").pull
                        parts = [tuple(p(d) for p in pulls) for d in result.data]
                        domain = SimpleSetDomain(partitions=jx.sort(set(parts)))
                    else:
                        domain = SimpleSetDomain(partitions=[])

                    dims.append(1 if allowNulls else 0)
                    edges.append(Data(
                        name=e.name,
                        allowNulls=allowNulls,
                        domain=domain
                    ))

                data = {}
                for si, s in enumerate(listwrap(query.select)):
                    if s.aggregate == "count":
                        data[s.name] = Matrix(dims=dims, zeros=0)
                    else:
                        data[s.name] = Matrix(dims=dims)

                if isinstance(query.select, list):
                    select = [{"name": s.name} for s in query.select]
                else:
                    select = {"name": query.select.name}

                return Data(
                    meta={"format": "cube"},
                    edges=edges,
                    select=select,
                    data={k: v.cube for k, v in data.items()}
                )

            columns = None

            edges = []
            dims = []
            for g in query.groupby:
                g.is_groupby = True

            for i, e in enumerate(query.edges + query.groupby):
                allowNulls = coalesce(e.allowNulls, True)

                if e.domain.type == "set" and e.domain.partitions:
                    domain = SimpleSetDomain(partitions=e.domain.partitions.name)
                elif e.domain.type == "range":
                    domain = e.domain
                elif e.domain.type == "time":
                    domain = wrap(mo_json.scrub(e.domain))
                elif e.domain.type == "duration":
                    domain = wrap(mo_json.scrub(e.domain))
                elif isinstance(e.value, TupleOp):
                    pulls = jx.sort([c for c in index_to_columns.values() if c.push_name == e.name], "push_child").pull
                    parts = [tuple(p(d) for p in pulls) for d in result.data]
                    domain = SimpleSetDomain(partitions=jx.sort(set(parts)))
                else:
                    if not columns:
                        columns = zip(*result.data)
                    parts = set(columns[i])
                    if e.is_groupby and None in parts:
                        allowNulls = True
                    parts -= {None}

                    if query.sort[i].sort==-1:
                        domain = SimpleSetDomain(partitions=wrap(sorted(parts,reverse=True)))
                    else:
                        domain = SimpleSetDomain(partitions=jx.sort(parts))

                dims.append(len(domain.partitions) + (1 if allowNulls else 0))
                edges.append(Data(
                    name=e.name,
                    allowNulls=allowNulls,
                    domain=domain
                ))

            data_cubes = {}
            for si, s in enumerate(listwrap(query.select)):
                if s.aggregate == "count":
                    data_cubes[s.name] = Matrix(dims=dims, zeros=0)
                else:
                    data_cubes[s.name] = Matrix(dims=dims)

            r2c = index_to_coordinate(dims)  # WORKS BECAUSE THE DATABASE SORTED THE EDGES TO CONFORM
            for rownum, row in enumerate(result.data):
                coord = r2c(rownum)

                for i, s in enumerate(index_to_columns.values()):
                    if s.is_edge:
                        continue
                    if s.push_child == ".":
                        data_cubes[s.push_name][coord] = s.pull(row)
                    else:
                        data_cubes[s.push_name][coord][s.push_child] = s.pull(row)

            if query.select == None:
                select = Null
            elif isinstance(query.select, list):
                select = [{"name": s.name} for s in query.select]
            else:
                select = {"name": query.select.name}

            return Data(
                meta={"format": "cube"},
                edges=edges,
                select=select,
                data={k: v.cube for k, v in data_cubes.items()}
            )
        elif query.format == "table" or (not query.format and query.groupby):
            column_names= [None]*(max(c.push_column for c in index_to_columns.values()) + 1)
            for c in index_to_columns.values():
                    column_names[c.push_column] = c.push_column_name
            data = []
            for d in result.data:
                row = [None for _ in column_names]
                for s in index_to_columns.values():
                    if s.push_child == ".":
                        row[s.push_column] = s.pull(d)
                    elif s.num_push_columns:
                        tuple_value = row[s.push_column]
                        if tuple_value == None:
                            tuple_value = row[s.push_column] = [None] * s.num_push_columns
                        tuple_value[s.push_child] = s.pull(d)
                    elif row[s.push_column] == None:
                        row[s.push_column] = Data()
                        row[s.push_column][s.push_child] = s.pull(d)
                    else:
                        row[s.push_column][s.push_child] = s.pull(d)
                data.append(tuple(unwrap(r) for r in row))

            output = Data(
                meta={"format": "table"},
                header=column_names,
                data=data
            )
        elif query.format == "list" or (not query.edges and not query.groupby):
            if not query.edges and not query.groupby and any(listwrap(query.select).aggregate):
                if isinstance(query.select, list):
                    data = Data()
                    for c in index_to_columns.values():
                        if c.push_child == ".":
                            if data[c.push_name] == None:
                                data[c.push_name] = c.pull(result.data[0])
                            elif isinstance(data[c.push_name], list):
                                data[c.push_name].append(c.pull(result.data[0]))
                            else:
                                data[c.push_name] = [data[c.push_name], c.pull(result.data[0])]
                        else:
                            data[c.push_name][c.push_child] = c.pull(result.data[0])

                    output = Data(
                        meta={"format": "value"},
                        data=data
                    )
                else:
                    data = Data()
                    for s in index_to_columns.values():
                        if not data[s.push_child]:
                            data[s.push_child] = s.pull(result.data[0])
                        else:
                            data[s.push_child] += [s.pull(result.data[0])]
                    output = Data(
                        meta={"format": "value"},
                        data=unwrap(data)
                    )
            else:
                data = []
                for rownum in result.data:
                    row = Data()
                    for c in index_to_columns.values():
                        if c.push_child == ".":
                            row[c.push_name] = c.pull(rownum)
                        elif c.num_push_columns:
                            tuple_value = row[c.push_name]
                            if not tuple_value:
                                tuple_value = row[c.push_name] = [None] * c.num_push_columns
                            tuple_value[c.push_child] = c.pull(rownum)
                        else:
                            row[c.push_name][c.push_child] = c.pull(rownum)

                    data.append(row)

                output = Data(
                    meta={"format": "list"},
                    data=data
                )
        else:
            Log.error("unknown format {{format}}", format=query.format)

        return output

    def query_metadata(self, query):
        frum, query['from'] = query['from'], self
        schema = self.sf.tables["."].schema
        query = QueryOp.wrap(query, schema)
        columns = self.sf.columns
        where = query.where
        tableName = None
        columnName = None

        if query.edges or query.groupby:
            Log.error("Aggregates(groupby or edge) are not supported")

        if where.op == "eq" and where.lhs.var == "table":
            tableName = mo_json.json2value(where.rhs.json)
        elif where.op =="eq" and where.lhs.var =="name":
            columnName = mo_json.json2value(where.rhs.json)
        else:
            Log.error("Only simple filters are expected like: \"eq\" on table and column name")

        t = [i for i in columns[0].names.keys()]
        tables = [concat_field(self.sf.fact, i) for i in t]

        metadata = []
        if columns[-1].es_column!="_id":
            columns.append(Column(
                names={i: relative_field("_id", i) for i in t},
                type="string",
                es_column="_id",
                es_index=self.sf.fact,
                nested_path=["."]
            ))

        for tname, table in zip(t, tables):
            if tableName != None and tableName != table:
                continue

            for col in columns:
                cname, ctype = untyped_column(col.es_column)
                if columnName != None and columnName != cname:
                    continue

                metadata.append((table, col.names[tname], col.type, unwraplist(col.nested_path)))

        if query.format == "cube":
            num_rows = len(metadata)
            header = ["table", "name", "type", "nested_path"]
            temp_data = dict(zip(header, zip(*metadata)))
            return Data(
                meta={"format": "cube"},
                data=temp_data,
                edges=[{
                    "name": "rownum",
                    "domain": {
                        "type": "rownum",
                        "min": 0,
                        "max": num_rows,
                        "interval": 1
                    }
                }]
            )
        elif query.format == "table":
            header = ["table", "name", "type", "nested_path"]
            return Data(
                meta={"format": "table"},
                header=header,
                data=metadata
            )
        else:
            header = ["table", "name", "type", "nested_path"]
            return Data(
                    meta={"format": "list"},
                    data=[dict(zip(header, r)) for r in metadata]
                )

    def _window_op(self, query, window):
        # http://www2.sqlite.org/cvstrac/wiki?p=UnsupportedSqlAnalyticalFunctions
        if window.value == "rownum":
            return (
                "ROW_NUMBER()-1 OVER (" +
                " PARTITION BY " + (SQL_COMMA.join(window.edges.values)) +
                SQL_ORDERBY + (SQL_COMMA.join(window.edges.sort)) +
                ") AS " + quote_table(window.name)
            )

        range_min = text_type(coalesce(window.range.min, "UNBOUNDED"))
        range_max = text_type(coalesce(window.range.max, "UNBOUNDED"))

        return (
            sql_aggs[window.aggregate] + "(" + window.value.to_sql() + ") OVER (" +
            " PARTITION BY " + (SQL_COMMA.join(window.edges.values)) +
            SQL_ORDERBY + (SQL_COMMA.join(window.edges.sort)) +
            " ROWS BETWEEN " + range_min + " PRECEDING AND " + range_max + " FOLLOWING " +
            ") AS " + quote_table(window.name)
        )

    def _normalize_select(self, select):
        output = []
        if select.value == ".":
            for cname, cs in self.columns.items():
                for c in cs:
                    if c.type in STRUCT:
                        continue

                    new_select = select.copy()
                    new_select.name = cname
                    new_select.value = Variable(cname)
                    output.append(new_select)
                    break
        elif select.value.endswith(".*"):
            Log.error("not done")
        else:
            Log.error("not done")
        return output


from jx_base.container import type2container

type2container["sqlite"] = QueryTable
