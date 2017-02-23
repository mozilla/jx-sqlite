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

import re
from collections import Mapping, OrderedDict
from copy import copy

import mo_json
from mo_collections.matrix import Matrix, index_to_coordinate
from mo_dots import listwrap, coalesce, Data, wrap, Null, unwraplist, split_field, join_field, startswith_field, literal_field, unwrap, \
    relative_field, concat_field, unliteral_field
from mo_logs import Log
from mo_math import Math
from mo_math import UNION, MAX
from mo_math.randoms import Random
from mo_times import Date, Duration
from pyLibrary import convert
from mo_kwargs import override
from pyLibrary.queries import jx, Index
from pyLibrary.queries.containers import Container, STRUCT
from pyLibrary.queries.domains import SimpleSetDomain, DefaultDomain, TimeDomain, DurationDomain
from pyLibrary.queries.expressions import jx_expression, Variable, sql_type_to_json_type, TupleOp, LeavesOp
from pyLibrary.queries.meta import Column
from pyLibrary.queries.query import QueryOp
from pyLibrary.sql.sqlite import Sqlite

_containers = None

UID = "__id__"  # will not be quoted
GUID = "__guid__"
ORDER = "__order__"
PARENT = "__parent__"
COLUMN = "__column"

ALL_TYPES = "bns"


def late_import():
    global _containers

    from pyLibrary.queries import containers as _containers

    _ = _containers


class Table_usingSQLite(Container):
    @override
    def __init__(self, name, db=None, uid=GUID, exists=False, kwargs=None):
        """
        :param name: NAME FOR THIS TABLE
        :param db: THE DB TO USE
        :param uid: THE UNIQUE INDEX FOR THIS TABLE
        :return: HANDLE FOR TABLE IN db
        """
        global _containers

        Container.__init__(self, frum=None)
        if db:
            self.db = db
        else:
            self.db = db = Sqlite()

        self.name = name
        self.uid = listwrap(uid)
        self._next_uid = 1
        self._make_digits_table()

        late_import()
        if not _containers.config.default:
            _containers.config.default = {
                "type": "sqlite",
                "settings": {"db": db}
            }

        self.uid_accessor = jx.get(self.uid)
        self.nested_tables = OrderedDict()  # MAP FROM NESTED PATH TO Table OBJECT, PARENTS PROCEED CHILDREN
        self.nested_tables["."] = self
        self.columns = Index(keys=[join_field(["names", self.name])])  # MAP FROM DOCUMENT ABS PROPERTY NAME TO THE SET OF SQL COLUMNS IT REPRESENTS (ONE FOR EACH REALIZED DATATYPE)

        if not exists:
            for u in self.uid:
                if u == GUID:
                    pass
                else:
                    c = Column(
                        names={name: u},
                        type="string",
                        es_column=typed_column(u, "string"),
                        es_index=name
                    )
                    self.add_column_to_schema(self.nested_tables, c)

            command = (
                "CREATE TABLE " + quote_table(name) + "(" +
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
        else:
            # LOAD THE COLUMNS
            command = "PRAGMA table_info(" + quote_table(name) + ")"
            details = self.db.query(command)

            for r in details:
                cname = untyped_column(r[1])
                ctype = r[2].lower()
                column = Column(
                    names={name: cname},
                    type=ctype,
                    es_column=typed_column(cname, ctype),
                    es_index=name
                )

                self.add_column_to_schema(self.columns, column)
                # TODO: FOR ALL TABLES, FIND THE MAX ID

    def quote_column(self, column, table=None):
        return self.db.quote_column(column, table)

    def _make_digits_table(self):
        existence = self.db.query("PRAGMA table_info(__digits__)")
        if not existence.data:
            self.db.execute("CREATE TABLE __digits__(value INTEGER)")
            self.db.execute("INSERT INTO __digits__ " + "\nUNION ALL ".join("SELECT " + unicode(i) for i in range(10)))

    def next_uid(self):
        try:
            return self._next_uid
        finally:
            self._next_uid += 1

    def __del__(self):
        self.db.execute("DROP TABLE " + quote_table(self.name))

    def add(self, doc):
        self.insert([doc])

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

    def insert(self, docs):
        doc_collection = self.flatten_many(docs)
        self._insert(doc_collection)

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
            table = Table_usingSQLite(nested_table_name, self.db, exists=False)
            self.nested_tables[column.name] = table
        else:
            self.db.execute(
                "ALTER TABLE " + quote_table(self.name) + " ADD COLUMN " + _quote_column(column) + " " + column.type
            )

    def get_column_name(self, column):
        return column.names[self.name]

    def __len__(self):
        counter = self.db.query("SELECT COUNT(*) FROM " + quote_table(self.name))[0][0]
        return counter

    def __nonzero__(self):
        counter = self.db.query("SELECT COUNT(*) FROM " + quote_table(self.name))[0][0]
        return bool(counter)

    def __getattr__(self, item):
        return self.__getitem__(item)

    def __getitem__(self, item):
        cs = self.columns.get(item, None)
        if not cs:
            return [Null]

        command = " UNION ALL ".join(
            "SELECT " + _quote_column(c) + " FROM " + quote_table(c.es_index)
            for c in cs
        )

        output = self.db.query(command)
        return [o[0] for o in output]

    def __iter__(self):
        columns = [c for c, cs in self.columns.items() for c in cs if c.type not in STRUCT]
        command = "SELECT " + \
                  ",\n".join(_quote_column(c) for c in columns) + \
                  " FROM " + quote_table(self.name)
        rows = self.db.query(command)
        for r in rows:
            output = Data()
            for (k, t), v in zip(columns, r):
                output[k] = v
            yield output

    def delete(self, where):
        filter = where.to_sql()
        self.db.execute("DELETE FROM " + quote_table(self.name) + " WHERE " + filter)

    def vars(self):
        return set(self.columns.keys())

    def map(self, map_):
        return self

    def update(self, command):
        """
        :param command:  EXPECTING dict WITH {"set": s, "clear": c, "where": w} FORMAT
        """
        command = wrap(command)

        # REJECT DEEP UPDATES
        touched_columns = command.set.keys() | set(listwrap(command['clear']))
        for c in self.get_leaves():
            if c.name in touched_columns and c.nested_path and len(c.name) > len(c.nested_path[0]):
                Log.error("Deep update not supported")

        # ADD NEW COLUMNS
        where = jx_expression(command.where)
        _vars = where.vars()
        _map = {
            v: c.es_column
            for v in _vars
            for c in self.columns.get(v, Null)
            if c.type not in STRUCT
            }
        where_sql = where.map(_map).to_sql()
        new_columns = set(command.set.keys()) - set(self.columns.keys())
        for new_column_name in new_columns:
            nested_value = command.set[new_column_name]
            ctype = get_type(nested_value)
            column = Column(
                names={self.name: new_column_name},
                type=ctype,
                es_index=self.name,
                es_column=typed_column(new_column_name, ctype)
            )
            self.add_column(column)

        # UPDATE THE NESTED VALUES
        for nested_column_name, nested_value in command.set.items():
            if get_type(nested_value) == "nested":
                nested_table_name = concat_field(self.name, nested_column_name)
                nested_table = nested_tables[nested_column_name]
                self_primary_key = ",".join(quote_table(c.es_column) for u in self.uid for c in self.columns[u])
                extra_key_name = UID_PREFIX + "id" + unicode(len(self.uid))
                extra_key = [e for e in nested_table.columns[extra_key_name]][0]

                sql_command = "DELETE FROM " + quote_table(nested_table.name) + \
                              "\nWHERE EXISTS (" + \
                              "\nSELECT 1 " + \
                              "\nFROM " + quote_table(nested_table.name) + " n" + \
                              "\nJOIN (" + \
                              "\nSELECT " + self_primary_key + \
                              "\nFROM " + quote_table(self.name) + \
                              "\nWHERE " + where_sql + \
                              "\n) t ON " + \
                              " AND ".join(
                                  "t." + quote_table(c.es_column) + " = n." + quote_table(c.es_column)
                                  for u in self.uid
                                  for c in self.columns[u]
                              ) + \
                              ")"
                self.db.execute(sql_command)

                # INSERT NEW RECORDS
                if not nested_value:
                    continue

                doc_collection = {}
                for d in listwrap(nested_value):
                    nested_table.flatten(d, Data(), doc_collection, path=nested_column_name)

                prefix = "INSERT INTO " + quote_table(nested_table.name) + \
                         "(" + \
                         self_primary_key + "," + \
                         _quote_column(extra_key) + "," + \
                         ",".join(
                             quote_table(c.es_column)
                             for c in doc_collection.get(".", Null).active_columns
                         ) + ")"

                # BUILD THE PARENT TABLES
                parent = "\nSELECT " + \
                         self_primary_key + \
                         "\nFROM " + quote_table(self.name) + \
                         "\nWHERE " + jx_expression(command.where).to_sql()

                # BUILD THE RECORDS
                children = " UNION ALL ".join(
                    "\nSELECT " +
                    quote_value(i) + " " + quote_table(extra_key.es_column) + "," +
                    ",".join(
                        quote_value(row[c.name]) + " " + quote_table(c.es_column)
                        for c in doc_collection.get(".", Null).active_columns
                    )
                    for i, row in enumerate(doc_collection.get(".", Null).rows)
                )

                sql_command = prefix + \
                              "\nSELECT " + \
                              ",".join(
                                  "p." + quote_table(c.es_column)
                                  for u in self.uid for c in self.columns[u]
                              ) + "," + \
                              "c." + _quote_column(extra_key) + "," + \
                              ",".join(
                                  "c." + quote_table(c.es_column)
                                  for c in doc_collection.get(".", Null).active_columns
                              ) + \
                              "\nFROM (" + parent + ") p " + \
                              "\nJOIN (" + children + \
                              "\n) c on 1=1"

                self.db.execute(sql_command)

                # THE CHILD COLUMNS COULD HAVE EXPANDED
                # ADD COLUMNS TO SELF
                for n, cs in nested_table.columns.items():
                    for c in cs:
                        column = Column(
                            names={self.name: c.name},
                            type=c.type,
                            es_index=c.es_index,
                            es_column=c.es_column,
                            nested_path=[nested_column_name] + c.nested_path
                        )
                        if c.name not in self.columns:
                            self.columns[column.name] = {column}
                        elif c.type not in [c.type for c in self.columns[c.name]]:
                            self.columns[column.name].add(column)

        command = "UPDATE " + quote_table(self.name) + " SET " + \
                  ",\n".join(
                      [
                          _quote_column(c) + "=" + quote_value(get_if_type(v, c.type))
                          for k, v in command.set.items()
                          if get_type(v) != "nested"
                          for c in self.columns[k]
                          if c.type != "nested" and len(c.nested_path) == 1
                          ] +
                      [
                          _quote_column(c) + "=NULL"
                          for k in listwrap(command['clear'])
                          if k in self.columns
                          for c in self.columns[k]
                          if c.type != "nested" and len(c.nested_path) == 1
                          ]
                  ) + \
                  " WHERE " + where_sql

        self.db.execute(command)

    def upsert(self, doc, where):
        old_docs = self.filter(where)
        if len(old_docs) == 0:
            self.insert(doc)
        else:
            self.delete(where)
            self.insert(doc)

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
                    ",".join(quote_table(c.es_column) for c in cs) +
                    ") " + quote_table(c.name)
                )

        result = self.db.query(
            " SELECT " + "\n,".join(select) +
            " FROM " + quote_table(self.name) +
            " WHERE " + jx_expression(filter).to_sql()
        )
        return wrap([{c: v for c, v in zip(column_names, r)} for r in result.data])

    def query(self, query):
        """
        :param query:  JSON Query Expression, SET `format="container"` TO MAKE NEW TABLE OF RESULT
        :return:
        """
        if not startswith_field(query['from'], self.name):
            Log.error("Expecting table, or some nested table")
        frum, query['from'] = query['from'], self
        query = QueryOp.wrap(query, self.columns)

        # TYPE CONFLICTS MUST NOW BE RESOLVED DURING
        # TYPE-SPECIFIC QUERY NORMALIZATION
        # vars_ = query.vars(exclude_select=True)
        # type_map = {
        #     v: c.es_column
        #     for v in vars_
        #     if v in self.columns and len([c for c in self.columns[v] if c.type != "nested"]) == 1
        #     for c in self.columns[v]
        #     if c.type != "nested"
        # }
        #
        # sql_query = query.map(type_map)
        query = query

        new_table = "temp_" + unique_name()

        if query.format == "container":
            create_table = "CREATE TABLE " + quote_table(new_table) + " AS "
        else:
            create_table = ""

        if query.groupby:
            op, index_to_columns = self._groupby_op(query, frum)
            command = create_table + op
        elif query.edges or any(a != "none" for a in listwrap(query.select).aggregate):
            op, index_to_columns = self._edges_op(query, frum)
            command = create_table + op
        else:
            op = self._set_op(query, frum)
            return op

        if query.sort:
            command += "\nORDER BY " + ",\n".join(
                "(" + sql[t] + ") IS NULL" + (" DESC" if s.sort == -1 else "") + ",\n" +
                sql[t] + (" DESC" if s.sort == -1 else "")
                for s, sql in [(s, s.value.to_sql(self)[0].sql) for s in query.sort]
                for t in "bns" if sql[t]
            )

        result = self.db.query(command)

        column_names = query.edges.name + query.groupby.name + listwrap(query.select).name
        if query.format == "container":
            output = Table_usingSQLite(new_table, db=self.db, uid=self.uid, exists=True)
        elif query.format == "cube" or (not query.format and query.edges):
            if len(query.edges) == 0 and len(query.groupby) == 0:
                data = {n: Data() for n in column_names}
                for s in index_to_columns.values():
                    data[s.push_name][s.push_child] = unwrap(s.pull(result.data[0]))
                return Data(
                    data=unwrap(data),
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

                zeros = [
                    0 if s.aggregate == "count" and index_to_columns[si].push_child == "." else Data
                    for si, s in enumerate(listwrap(query.select))
                    ]
                data = {s.name: Matrix(dims=dims, zeros=zeros[si]) for si, s in enumerate(listwrap(query.select))}

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
                    domain = SimpleSetDomain(partitions=jx.sort(parts))

                dims.append(len(domain.partitions) + (1 if allowNulls else 0))
                edges.append(Data(
                    name=e.name,
                    allowNulls=allowNulls,
                    domain=domain
                ))

            zeros = [
                0 if s.aggregate == "count" and index_to_columns[si].push_child == "." else Data
                for si, s in enumerate(listwrap(query.select))
                ]
            data_cubes = {s.name: Matrix(dims=dims, zeros=zeros[si]) for si, s in enumerate(listwrap(query.select))}
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

            if isinstance(query.select, list):
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
                            data[c.push_name] = c.pull(result.data[0])
                        else:
                            data[c.push_name][c.push_child] = c.pull(result.data[0])

                    output = Data(
                        meta={"format": "value"},
                        data=data
                    )
                else:
                    data = Data()
                    for s in index_to_columns.values():
                        data[s.push_child] = s.pull(result.data[0])

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

    def _edges_op(self, query, frum):
        index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        outer_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
        tables = []
        base_table = split_field(frum)[0]
        path = join_field(split_field(frum)[1:])
        nest_to_alias = {nested_path: "__" + unichr(ord('a') + i) + "__" for i, (nested_path, sub_table) in
                         enumerate(self.nested_tables.items())}

        columns = self._get_sql_schema(frum)

        tables = []
        for n, a in nest_to_alias.items():
            if startswith_field(path, n):
                tables.append({"nest": n, "alias": a})
        tables = jx.sort(tables, {"value": {"length": "nest"}})

        from_sql = join_field([base_table] + split_field(tables[0].nest)) + " " + tables[0].alias
        previous = tables[0]
        for t in tables[1::]:
            from_sql += "\nLEFT JOIN\n" + join_field([base_table] + split_field(
                t.nest)) + " " + t.alias + " ON " + t.alias + "." + PARENT + " = " + previous.alias + "." + GUID

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
            edge_alias = "e" + unicode(edge_index)

            if query_edge.value:
                edge_values = [p for c in query_edge.value.to_sql(self).sql for p in c.items()]
            elif not query_edge.value and any(query_edge.domain.partitions.where):
                case = "CASE "
                for pp, p in enumerate(query_edge.domain.partitions):
                    w = p.where.to_sql(self)[0].sql.b
                    t = quote_value(pp)
                    case += " WHEN " + w + " THEN " + t
                case += " ELSE NULL END "
                edge_values = [("n", case)]
            elif query_edge.range:
                edge_values = query_edge.range.min.to_sql(self)[0].sql.items() + query_edge.range.max.to_sql(self)[
                    0].sql.items()
            else:
                Log.error("Do not know how to handle")

            edge_names = []
            for column_index, (json_type, sql) in enumerate(edge_values):
                sql_name = "e" + unicode(edge_index) + "c" + unicode(column_index)
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

                index_to_column[num_sql_columns] = Data(
                    is_edge=True,
                    push_name=query_edge.name,
                    push_column=edge_index,
                    num_push_columns=num_push_columns,
                    push_child=push_child,  # CAN NOT HANDLE TUPLES IN COLUMN
                    pull=pull,
                    sql=sql,
                    type=sql_type_to_json_type[json_type]
                )

            vals = [v for t, v in edge_values]
            if query_edge.domain.type == "set":
                domain_name = "d" + unicode(edge_index) + "c" + unicode(column_index)
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
                    on_clause = " OR ".join(
                        edge_alias + "." + k + " = " + v
                        for k, (t, v) in zip(domain_names, edge_values)
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
                domain_name = "d" + unicode(edge_index) + "c0"
                domain_names = [domain_name]  # ONLY EVER SEEN ONE DOMAIN VALUE, DOMAIN TUPLES CERTAINLY EXIST
                d = query_edge.domain
                if d.max == None or d.min == None or d.min == d.max:
                    Log.error("Invalid range: {{range|json}}", range=d)
                if len(edge_names) == 1:
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    limit = Math.min(query.limit, query_edge.domain.limit)
                    domain += "\nORDER BY \n" + ",\n".join(vals) + \
                              "\nLIMIT " + unicode(limit)

                    where = None
                    join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                    on_clause = " AND ".join(
                        edge_alias + "." + k + " <= " + v + " AND " + v + " < (" + edge_alias + "." + k + " + " + unicode(
                            d.interval) + ")"
                        for k, (t, v) in zip(domain_names, edge_values)
                    )
                    not_on_clause = None
                elif query_edge.range:
                    query_edge.allowNulls = False
                    domain = self._make_range_domain(domain=d, column_name=domain_name)
                    limit = Math.min(query.limit, query_edge.domain.limit)
                    domain += "\nORDER BY \n" + ",\n".join(vals) + \
                              "\nLIMIT " + unicode(limit)
                    where = None
                    join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                    on_clause = edge_alias + "." + domain_name + " < " + edge_values[1][1] + " AND " + \
                                edge_values[0][1] + " < (" + edge_alias + "." + domain_name + " + " + unicode(
                        d.interval) + ")"
                    not_on_clause = None
                else:
                    Log.error("do not know how to handle")
                    # select_clause.extend(v[0] + " " + k for k, v in zip(domain_names, edge_values))
            elif len(edge_names) > 1:
                domain_names = ["d" + unicode(edge_index) + "c" + unicode(i) for i, _ in enumerate(edge_names)]
                query_edge.allowNulls = False
                domain = (
                    "\nSELECT " + ",\n".join(g + " AS " + n for n, g in zip(domain_names, vals)) +
                    "\nFROM\n" + quote_table(self.name) + " " + nest_to_alias["."] +
                    "\nGROUP BY\n" + ",\n".join(vals)
                )
                limit = Math.min(query.limit, query_edge.domain.limit)
                domain += (
                    "\nORDER BY COUNT(1) DESC" +
                    "\nLIMIT " + unicode(limit)
                )
                where = None
                join_type = "LEFT JOIN" if query_edge.allowNulls else "JOIN"
                on_clause = " AND ".join(
                    "((" + edge_alias + "." + k + " IS NULL AND " + v + " IS NULL) OR " + edge_alias + "." + k + " = " + v + ")"
                    for k, v in zip(domain_names, vals)
                )
                not_on_clause = None
            elif isinstance(query_edge.domain, DefaultDomain):
                domain_names = ["d" + unicode(edge_index) + "c" + unicode(i) for i, _ in enumerate(edge_names)]
                domain = (
                    "\nSELECT " + ",".join(domain_names) + " FROM ("
                                                           "\nSELECT " + ",\n".join(
                        g + " AS " + n for n, g in zip(domain_names, vals)) +
                    "\nFROM\n" + quote_table(self.name) + " " + nest_to_alias["."] +
                    "\nWHERE\n" + " AND ".join(g + " IS NOT NULL" for g in vals) +
                    "\nGROUP BY\n" + ",\n".join(g for g in vals)
                )
                limit = Math.min(query.limit, query_edge.domain.limit)
                domain += (
                    "\nORDER BY \n" + ",\n".join("COUNT(1) DESC" for g in vals) +
                    "\nLIMIT " + unicode(limit)
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
                domain_name = "d" + unicode(edge_index) + "c0"
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

            for k in domain_names:
                outer_selects.append(edge_alias + "." + k + " AS " + k)

                orderby.append(k + " IS NULL")
                orderby.append(k)

        offset = len(query.edges)
        for ssi, s in enumerate(listwrap(query.select)):
            si = ssi + offset
            if isinstance(s.value, Variable) and s.value.var == "." and s.aggregate == "count":
                # COUNT RECORDS, NOT ANY ONE VALUE
                sql = "COUNT(__exists__) AS " + quote_table(s.name)

                column_number = len(outer_selects)
                outer_selects.append(sql)
                index_to_column[column_number] = Data(
                    push_name=s.name,
                    push_column=si,
                    push_child=".",
                    pull=get_column(column_number),
                    sql=sql,
                    type=sql_type_to_json_type["n"]
                )
            elif s.aggregate == "percentile":
                if not isinstance(s.percentile, (int, float)):
                    Log.error("Expecting percentile to be a float between 0 and 1")

                Log.error("not implemented")
            elif s.aggregate == "cardinality":
                for details in s.value.to_sql(self):
                    for json_type, sql in details.sql.items():
                        column_number = len(outer_selects)
                        count_sql = "COUNT(DISTINCT(" + sql + ")) AS " + _make_column_name(column_number)
                        outer_selects.append(count_sql)
                        index_to_column[column_number] = Data(
                            push_name=s.name,
                            push_column=si,
                            push_child=".",
                            pull=get_column(column_number),
                            sql=count_sql,
                            type=sql_type_to_json_type[json_type]
                        )
            elif s.aggregate == "union":
                for details in s.value.to_sql(self):
                    concat_sql = []
                    column_number = len(outer_selects)

                    for json_type, sql in details.sql.items():
                        concat_sql.append("GROUP_CONCAT(QUOTE(DISTINCT(" + sql + ")))")

                    if len(concat_sql) > 1:
                        concat_sql = "CONCAT(" + ",".join(concat_sql) + ") AS " + _make_column_name(column_number)
                    else:
                        concat_sql = concat_sql[0] + " AS " + _make_column_name(column_number)

                    outer_selects.append(concat_sql)
                    index_to_column[column_number] = Data(
                        push_name=s.name,
                        push_column=si,
                        push_child=".",
                        pull=sql_text_array_to_set(column_number),
                        sql=concat_sql,
                        type=sql_type_to_json_type[json_type]
                    )

            elif s.aggregate == "stats":  # THE STATS OBJECT
                for details in s.value.to_sql(self):
                    sql = details.sql["n"]
                    for name, code in STATS.items():
                        full_sql = code.replace("{{value}}", sql)
                        column_number = len(outer_selects)
                        outer_selects.append(full_sql + " AS " + _make_column_name(column_number))
                        index_to_column[column_number] = Data(
                            push_name=s.name,
                            push_column=si,
                            push_child=name,
                            pull=get_column(column_number),
                            sql=full_sql,
                            type="number"
                        )
            else:  # STANDARD AGGREGATES
                for details in s.value.to_sql(self):
                    for sql_type, sql in details.sql.items():
                        column_number = len(outer_selects)
                        sql = sql_aggs[s.aggregate] + "(" + sql + ")"
                        if s.default != None:
                            sql = "COALESCE(" + sql + ", " + quote_value(s.default) + ")"
                        outer_selects.append(sql + " AS " + _make_column_name(column_number))
                        index_to_column[column_number] = Data(
                            push_name=s.name,
                            push_column=si,
                            push_child=".",  # join_field(split_field(details.name)[1::]),
                            pull=get_column(column_number),
                            sql=sql,
                            type=sql_type_to_json_type[sql_type]
                        )

        for w in query.window:
            outer_selects.append(self._window_op(self, query, w))

        main_filter = query.where.to_sql(self)[0].sql.b

        all_parts = []

        primary = (
            "(" +
            "\nSELECT\n" + ",\n".join(select_clause) + ",\n" + "*" +
            "\nFROM " + from_sql +
            "\nWHERE " + main_filter +
            ") " + nest_to_alias["."]
        )
        sources = []
        for edge_index, query_edge in enumerate(query.edges):
            edge_alias = "e" + unicode(edge_index)
            domain = domains[edge_index]
            sources.append("(" + domain + ") " + edge_alias)

        # COORDINATES OF ALL primary DATA
        part = "SELECT " + (",\n".join(outer_selects)) + "\nFROM\n" + primary
        for t, s, j in zip(join_types, sources, ons):
            part += " " + t + "\n" + s + " ON " + j
        if any(wheres):
            part += "\nWHERE " + " AND ".join("(" + w + ")" for w in wheres if w)
        if groupby:
            part += "\nGROUP BY\n" + ",\n".join(groupby)
        all_parts.append(part)

        # ALL COORINATES MISSED BY primary DATA
        part = "SELECT " + (",\n".join(outer_selects)) + "\nFROM\n" + sources[0]
        for s in sources[1:]:
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
            value = "+".join("1" + ("0" * j) + "*" + unicode(chr(ord(b'a') + j)) + ".value" for j in range(digits + 1))

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
            domain += "\nJOIN __digits__ " + unicode(chr(ord(b'a') + j + 1)) + " ON 1=1"
        domain += "\nWHERE " + value + " < " + quote_value(width)
        return domain

    def _groupby_op(self, query, frum):
        columns = self._get_sql_schema(frum)
        index_to_column = {}
        nest_to_alias = {nested_path: "__" + unichr(ord('a') + i) + "__" for i, (nested_path, sub_table) in
                         enumerate(self.nested_tables.items())}

        selects = []
        groupby = []
        for i, e in enumerate(query.groupby):
            column_number = len(selects)
            sql_type, sql = e.value.to_sql(self)[0].sql.items()[0]
            groupby.append(sql)
            selects.append(sql + " AS " + e.name)

            index_to_column[column_number] = Data(
                is_edge=True,
                push_name=e.name,
                push_column=column_number,
                push_child=".",
                pull=get_column(column_number),
                sql=sql,
                type=sql_type_to_json_type[sql_type]
            )

        for s in listwrap(query.select):
            column_number = len(selects)
            sql_type, sql = s.value.to_sql(self)[0].sql.items()[0]

            if s.value == "." and s.aggregate == "count":
                selects.append("COUNT(1) AS " + quote_table(s.name))
            else:
                selects.append(sql_aggs[s.aggregate] + "(" + sql + ") AS " + quote_table(s.name))

            index_to_column[column_number] = Data(
                push_name=s.name,
                push_column=column_number,
                push_child=".",
                pull=get_column(column_number),
                sql=sql,
                type=sql_type_to_json_type[sql_type]
            )

        for w in query.window:
            selects.append(self._window_op(self, query, w))

        where = query.where.to_sql(self)[0].sql.b

        command = "SELECT\n" + (",\n".join(selects)) + \
                  "\nFROM\n" + quote_table(self.name) + " " + nest_to_alias["."] + \
                  "\nWHERE\n" + where + \
                  "\nGROUP BY\n" + ",\n".join(groupby)

        return command, index_to_column

    def _set_op(self, query, frum):
        # GET LIST OF COLUMNS
        primary_nested_path = join_field(split_field(frum)[1:])
        vars_ = UNION([s.value.vars() for s in listwrap(query.select)])

        nest_to_alias = {
            nested_path: "__" + unichr(ord('a') + i) + "__"
            for i, (nested_path, sub_table) in enumerate(self.nested_tables.items())
        }

        active_columns = {".": []}
        for cname, cols in self.columns.items():
            if any(startswith_field(cname, v) for v in vars_):
                for c in cols:
                    if c.type in STRUCT:
                        continue
                    nest = c.nested_path[0]
                    active = active_columns.get(nest)
                    if not active:
                        active = active_columns[nest] = []
                    active.append(c)
        # ANY VARS MENTIONED WITH NO COLUMNS?
        for v in vars_:
            if not any(startswith_field(cname, v) for cname in self.columns.keys()):
                active_columns["."].append(Column(
                    names={self.name: v},
                    type="null",
                    es_column=".",
                    es_index=".",
                    nested_path=["."]
                ))

        # EVERY COLUMN, AND THE INDEX IT TAKES UP
        index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
        index_to_uid = {}  # FROM NESTED PATH TO THE INDEX OF UID
        sql_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
        nest_to_alias = {
            nested_path: "__" + unichr(ord('a') + i) + "__"
            for i, (nested_path, sub_table) in enumerate(self.nested_tables.items())
            }

        sorts = []
        if query.sort:
            for s in query.sort:
                col = s.value.to_sql(self)[0]
                for t, sql in col.sql.items():
                    json_type = sql_type_to_json_type[t]
                    if json_type in STRUCT:
                        continue
                    column_number = len(sql_selects)
                    # SQL HAS ABS TABLE REFERENCE
                    column_alias = _make_column_name(column_number)
                    sql_selects.append(sql + " AS " + column_alias)
                    if s.sort == -1:
                        sorts.append(column_alias + " IS NOT NULL")
                        sorts.append(column_alias + " DESC")
                    else:
                        sorts.append(column_alias + " IS NULL")
                        sorts.append(column_alias)

        primary_doc_details = Data()
        # EVERY SELECT STATEMENT THAT WILL BE REQUIRED, NO MATTER THE DEPTH
        # WE WILL CREATE THEM ACCORDING TO THE DEPTH REQUIRED
        for nested_path, sub_table in self.nested_tables.items():
            nested_doc_details = {
                "sub_table": sub_table,
                "children": [],
                "index_to_column": {},
                "nested_path": [nested_path]  # fake the real nested path, we only look at [0] anyway
            }

            # INSERT INTO TREE
            if not primary_doc_details:
                primary_doc_details = nested_doc_details
            else:
                def place(parent_doc_details):
                    if startswith_field(nested_path, parent_doc_details['nested_path'][0]):
                        for c in parent_doc_details['children']:
                            if place(c):
                                return True
                        parent_doc_details['children'].append(nested_doc_details)

                place(primary_doc_details)

            alias = nested_doc_details['alias'] = nest_to_alias[nested_path]

            # WE ALWAYS ADD THE UID AND ORDER
            column_number = index_to_uid[nested_path] = nested_doc_details['id_coord'] = len(sql_selects)
            sql_select = alias + "." + quoted_UID
            sql_selects.append(sql_select + " AS " + _make_column_name(column_number))
            if nested_path != ".":
                sql_select = alias + "." + quote_table(ORDER)
                sql_selects.append(sql_select + " AS " + _make_column_name(column_number))

            # WE DO NOT NEED DATA FROM TABLES WE REQUEST NOTHING FROM
            if nested_path not in active_columns:
                continue

            if primary_nested_path == nested_path:
                # ADD SQL SELECT COLUMNS FOR EACH jx SELECT CLAUSE
                si = 0
                for s in listwrap(query.select):
                    try:
                        column_number = len(sql_selects)
                        s.pull = get_column(column_number)
                        db_columns = s.value.to_sql(self)

                        if isinstance(s.value, LeavesOp):
                            for column in db_columns:
                                for t, unsorted_sql in column.sql.items():
                                    json_type = sql_type_to_json_type[t]
                                    if json_type in STRUCT:
                                        continue
                                    column_number = len(sql_selects)
                                    # SQL HAS ABS TABLE REFERENCE
                                    column_alias = _make_column_name(column_number)
                                    sql_selects.append(unsorted_sql + " AS " + column_alias)
                                    index_to_column[column_number] = nested_doc_details['index_to_column'][column_number] = Data(
                                        push_name=concat_field(s.name, column.name),
                                        push_column=si,
                                        push_child=".",
                                        pull=get_column(column_number),
                                        sql=unsorted_sql,
                                        type=json_type,
                                        nested_path=[nested_path]
                                        # fake the real nested path, we only look at [0] anyway
                                    )
                                    si += 1
                        else:
                            for column in db_columns:
                                for t, unsorted_sql in column.sql.items():
                                    json_type = sql_type_to_json_type[t]
                                    if json_type in STRUCT:
                                        continue
                                    column_number = len(sql_selects)
                                    # SQL HAS ABS TABLE REFERENCE
                                    column_alias = _make_column_name(column_number)
                                    sql_selects.append(unsorted_sql + " AS " + column_alias)
                                    index_to_column[column_number] = nested_doc_details['index_to_column'][column_number] = Data(
                                        push_name=s.name,
                                        push_column=si,
                                        push_child=column.name,
                                        pull=get_column(column_number),
                                        sql=unsorted_sql,
                                        type=json_type,
                                        nested_path=[nested_path]
                                        # fake the real nested path, we only look at [0] anyway
                                    )
                    finally:
                        si += 1
            elif startswith_field(nested_path, primary_nested_path):
                # ADD REQUIRED COLUMNS, FOR DEEP STUFF
                for ci, c in enumerate(active_columns[nested_path]):
                    if c.type in STRUCT:
                        continue

                    column_number = len(sql_selects)
                    nested_path = c.nested_path
                    unsorted_sql = nest_to_alias[nested_path[0]] + "." + quote_table(c.es_column)
                    column_alias = _make_column_name(column_number)
                    sql_selects.append(unsorted_sql + " AS " + column_alias)
                    index_to_column[column_number] = nested_doc_details['index_to_column'][column_number] = Data(
                        push_name=s.name,
                        push_column=si,
                        push_child=relative_field(c.name, s.name),
                        pull=get_column(column_number),
                        sql=unsorted_sql,
                        type=c.type,
                        nested_path=nested_path
                    )

        where_clause = query.where.to_sql(self, boolean=True)[0].sql.b

        unsorted_sql = self._make_sql_for_one_nest_in_set_op(
            ".",
            sql_selects,
            where_clause,
            active_columns,
            index_to_column
        )

        for n, _ in self.nested_tables.items():
            sorts.append(COLUMN + unicode(index_to_uid[n]))

        ordered_sql = (
            "SELECT * FROM (\n" +
            unsorted_sql +
            "\n)" +
            "\nORDER BY\n" + ",\n".join(sorts) +
            "\nLIMIT " + quote_value(query.limit)
        )
        result = self.db.query(ordered_sql)

        def _accumulate_nested(rows, row, nested_doc_details, parent_doc_id, parent_id_coord):
            """
            :param rows: REVERSED STACK OF ROWS (WITH push() AND pop())
            :param row: CURRENT ROW BEING EXTRACTED
            :param nested_doc_details: {
                    "nested_path": wrap_nested_path(nested_path),
                    "index_to_column": map from column number to column details
                    "children": all possible direct decedents' nested_doc_details
                 }
            :param parent_doc_id: the id of the parent doc (for detecting when to step out of loop)
            :param parent_id_coord: the column number for the parent id (so we ca extract from each row)
            :return: the nested property (usually an array)
            """
            previous_doc_id = None
            doc = Data()
            output = []
            id_coord = nested_doc_details['id_coord']

            while True:
                doc_id = row[id_coord]

                if doc_id == None or (parent_id_coord is not None and row[parent_id_coord] != parent_doc_id):
                    rows.append(row)  # UNDO
                    output = unwraplist(output)
                    return output if output else None

                if doc_id != previous_doc_id:
                    previous_doc_id = doc_id
                    doc = Data()
                    curr_nested_path = nested_doc_details['nested_path'][0]
                    if isinstance(query.select, list) or isinstance(query.select.value, LeavesOp):
                        # ASSIGN INNER PROPERTIES
                        for i, c in nested_doc_details['index_to_column'].items():
                            value = row[i]
                            if value == None:
                                continue
                            if value == '':
                                continue

                            relative_path = relative_field(concat_field(c.push_name, c.push_child), curr_nested_path)
                            if relative_path == ".":
                                doc = value
                            else:
                                doc[relative_path] = value
                    else:
                        # ASSIGN INNER PROPERTIES
                        for i, c in nested_doc_details['index_to_column'].items():
                            value = row[i]
                            if value is not None:
                                relative_path = relative_field(c.push_child, curr_nested_path)
                                if relative_path == ".":
                                    doc = value
                                else:
                                    doc[relative_path] = value
                    output.append(doc)

                # ASSIGN NESTED ARRAYS
                for child_details in nested_doc_details['children']:
                    child_id = row[child_details['id_coord']]
                    if child_id is not None:
                        nested_value = _accumulate_nested(rows, row, child_details, doc_id, id_coord)
                        if nested_value is not None:
                            path = child_details['nested_path'][0]
                            doc[path] = nested_value

                try:
                    row = rows.pop()
                except IndexError:
                    output = unwraplist(output)
                    return output if output else None

        cols = tuple(index_to_column.values())

        if query.format == "cube":
            num_rows = len(result.data)
            num_cols = MAX([c.push_column for c in cols]) + 1 if len(cols) else 0
            map_index_to_name = {c.push_column: c.push_name for c in cols}
            temp_data = [[None]*num_rows for _ in range(num_cols)]
            for rownum, d in enumerate(result.data):
                for c in cols:
                    if c.push_child == ".":
                        temp_data[c.push_column][rownum] = c.pull(d)
                    else:
                        column = temp_data[c.push_column][rownum]
                        if column is None:
                            column = temp_data[c.push_column][rownum] = {}
                        column[c.push_child] = c.pull(d)

            output = Data(
                meta={"format": "cube"},
                data={n: temp_data[c] for c, n in map_index_to_name.items()},
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
            return output
        elif query.format == "table":
            num_column = MAX([c.push_column for c in cols])+1
            header = [None]*num_column
            for c in cols:
                # header[c.push_column] = c.push_name
                sf = split_field(c.push_name)
                if len(sf) == 0:
                    header[c.push_column] = "."
                elif len(sf) == 1:
                    header[c.push_column] = sf[0]
                else:
                    # TABLES ONLY USE THE FIRST-LEVEL PROPERTY NAMES
                    # PUSH ALL DEEPER NAMES TO CHILD
                    header[c.push_column] = sf[0]
                    c.push_child = join_field(sf[1:] + split_field(c.push_child))

            output_data = []
            for d in result.data:
                row = [None] * num_column
                for c in cols:
                    set_column(row, c.push_column, c.push_child, c.pull(d))
                output_data.append(row)
            return Data(
                meta={"format": "table"},
                header=header,
                data=output_data
            )
        else:
            rows = list(reversed(unwrap(result.data)))
            row = rows.pop()
            output = Data(
                meta={"format": "list"},
                data=listwrap(_accumulate_nested(rows, row, primary_doc_details, None, None))
            )
            return output

    def _make_sql_for_one_nest_in_set_op(
        self,
        primary_nested_path,
        selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
        where_clause,
        active_columns,
        index_to_sql_select  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
    ):
        """
        FOR EACH NESTED LEVEL, WE MAKE A QUERY THAT PULLS THE VALUES/COLUMNS REQUIRED
        WE `UNION ALL` THEM WHEN DONE
        :param primary_nested_path:
        :param selects:
        :param where_clause:
        :param active_columns:
        :param index_to_sql_select:
        :return: SQL FOR ONE NESTED LEVEL
        """

        parent_alias = "a"
        from_clause = ""
        select_clause = []
        children_sql = []
        done = []

        # STATEMENT FOR EACH NESTED PATH
        for i, (nested_path, sub_table) in enumerate(self.nested_tables.items()):
            if any(startswith_field(nested_path, d) for d in done):
                continue

            alias = "__" + unichr(ord('a') + i) + "__"

            if primary_nested_path == nested_path:
                select_clause = []
                # ADD SELECT CLAUSE HERE
                for select_index, s in enumerate(selects):
                    sql_select = index_to_sql_select.get(select_index)
                    if not sql_select:
                        select_clause.append(selects[select_index])
                        continue

                    if startswith_field(sql_select.nested_path[0], nested_path):
                        select_clause.append(sql_select.sql + " AS " + _make_column_name(select_index))
                    else:
                        # DO NOT INCLUDE DEEP STUFF AT THIS LEVEL
                        select_clause.append("NULL AS " + _make_column_name(select_index))

                if nested_path == ".":
                    from_clause += "\nFROM " + quote_table(self.name) + " " + alias + "\n"
                else:
                    from_clause += "\nLEFT JOIN " + quote_table(sub_table.name) + " " + alias + "\n" \
                                                                                                " ON " + alias + "." + quoted_PARENT + " = " + parent_alias + "." + quoted_UID + "\n"
                    where_clause = "(" + where_clause + ") AND " + alias + "." + quote_table(ORDER) + " > 0\n"

            elif startswith_field(primary_nested_path, nested_path):
                # PARENT TABLE
                # NO NEED TO INCLUDE COLUMNS, BUT WILL INCLUDE ID AND ORDER
                if nested_path == ".":
                    from_clause += "\nFROM " + quote_table(self.name) + " " + alias + "\n"
                else:
                    parent_alias = alias = unichr(ord('a') + i - 1)
                    from_clause += "\nLEFT JOIN " + quote_table(sub_table.name) + " " + alias + \
                                   " ON " + alias + "." + quoted_PARENT + " = " + parent_alias + "." + quoted_UID
                    where_clause = "(" + where_clause + ") AND " + parent_alias + "." + quote_table(ORDER) + " > 0\n"

            elif startswith_field(nested_path, primary_nested_path):
                # CHILD TABLE
                # GET FIRST ROW FOR EACH NESTED TABLE
                from_clause += "\nLEFT JOIN " + quote_table(sub_table.name) + " " + alias + \
                               " ON " + alias + "." + quoted_PARENT + " = " + parent_alias + "." + quoted_UID + \
                               " AND " + alias + "." + quote_table(ORDER) + " = 0\n"

                # IMMEDIATE CHILDREN ONLY
                done.append(nested_path)
                # NESTED TABLES WILL USE RECURSION
                children_sql.append(self._make_sql_for_one_nest_in_set_op(
                    nested_path,
                    selects,  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE
                    where_clause,
                    active_columns,
                    index_to_sql_select  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
                ))
            else:
                # SIBLING PATHS ARE IGNORED
                continue

            parent_alias = alias

        sql = "\nUNION ALL\n".join(
            ["SELECT\n" + ",\n".join(select_clause) + from_clause + "\nWHERE\n" + where_clause] +
            children_sql
        )

        return sql

    def _window_op(self, query, window):
        # http://www2.sqlite.org/cvstrac/wiki?p=UnsupportedSqlAnalyticalFunctions
        if window.value == "rownum":
            return "ROW_NUMBER()-1 OVER (" + \
                   " PARTITION BY " + (", ".join(window.edges.values)) + \
                   " ORDER BY " + (", ".join(window.edges.sort)) + \
                   ") AS " + quote_table(window.name)

        range_min = unicode(coalesce(window.range.min, "UNBOUNDED"))
        range_max = unicode(coalesce(window.range.max, "UNBOUNDED"))

        return sql_aggs[window.aggregate] + "(" + window.value.to_sql() + ") OVER (" + \
               " PARTITION BY " + (", ".join(window.edges.values)) + \
               " ORDER BY " + (", ".join(window.edges.sort)) + \
               " ROWS BETWEEN " + range_min + " PRECEDING AND " + range_max + " FOLLOWING " + \
               ") AS " + quote_table(window.name)

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
        self.nested_tables[new_path] = sub_table = Table_usingSQLite(destination_table, self.db, exists=False)

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

    def flatten_many(self, docs, path="."):
        """
        :param doc: THE JSON DOCUMENT
        :param path: FULL PATH TO THIS (INNER/NESTED) DOCUMENT
        :return: TUPLE (success, command, doc_collection) WHERE
                 success: BOOLEAN INDICATING PROPER PARSING
                 command: SCHEMA CHANGES REQUIRED TO BE SUCCESSFUL NEXT TIME
                 doc_collection: MAP FROM NESTED PATH TO INSERTION PARAMETERS:
                 {"active_columns": list, "rows": list of objects}
        """

        # TODO: COMMAND TO ADD COLUMNS
        # TODO: COMMAND TO NEST EXISTING COLUMNS
        # COLLECT AS MANY doc THAT DO NOT REQUIRE SCHEMA CHANGE

        required_changes = []
        _insertion = Data(
            active_columns=set(),
            rows=[]
        )
        doc_collection = {".": _insertion}
        nested_tables = copy(self.nested_tables)  # KEEP TRACK OF WHAT TABLE WILL BE MADE (SHORTLY)
        columns = copy(self.columns)

        def _flatten(data, uid, parent_id, order, full_path, nested_path, row=None):
            """
            :param data: the data we are pulling apart
            :param uid: the uid we are giving this doc
            :param parent_id: the parent id of this (sub)doc
            :param order: the number of siblings before this one
            :param full_path: path to this (sub)doc
            :param nested_path: list of paths, deepest first
            :param row: we will be filling this
            :return:
            """
            insertion = doc_collection[nested_path[0]]
            if not row:
                row = {UID: uid, PARENT: parent_id, ORDER: order}
                insertion.rows.append(row)

            if isinstance(data, Mapping):
                for k, v in data.items():
                    cname = concat_field(full_path, k)
                    value_type = get_type(v)
                    if value_type is None:
                        continue

                    if value_type in STRUCT:
                        c = unwraplist(
                            [cc for cc in columns[cname] if cc.type in STRUCT]
                        )
                    else:
                        c = unwraplist(
                            [cc for cc in columns[cname] if cc.type == value_type]
                        )

                    if not c:
                        # WHAT IS THE NESTING LEVEL FOR THIS PATH?
                        deeper_nested_path = "."
                        for path, _ in nested_tables.items():
                            if startswith_field(cname, path) and len(deeper_nested_path) < len(path):
                                deeper_nested_path = path
                        if deeper_nested_path != nested_path[0]:
                            # I HIGHLY SUSPECT, THROUGH CALLING _flatten() AGAIN THE REST OF THIS BLOCK IS NOT NEEDED
                            nested_column = unwraplist(
                                [cc for cc in columns.get(deeper_nested_path, Null) if cc.type in STRUCT]
                            )
                            insertion.active_columns.add(nested_column)
                            row[nested_column.es_column] = "."

                            nested_path = [deeper_nested_path] + nested_path
                            insertion = doc_collection.get(nested_path[0], None)
                            if not insertion:
                                insertion = doc_collection[nested_path[0]] = Data(
                                    active_columns=set(),
                                    rows=[]
                                )
                            uid, parent_id, order = self.next_uid(), uid, 0
                            row = {UID: uid, PARENT: parent_id, ORDER: order}
                            insertion.rows.append(row)

                        c = Column(
                            names={self.name: cname},
                            type=value_type,
                            es_column=typed_column(cname, value_type),
                            es_index=self.name,  # THIS MAY BE THE WRONG TABLE, IF THIS PATH IS A NESTED DOC
                            nested_path=nested_path
                        )
                        self.add_column_to_schema(self.nested_tables, c)
                        if value_type == "nested":
                            nested_tables[cname] = "fake table"

                        required_changes.append({"add": c})

                        # INSIDE IF BLOCK BECAUSE WE DO NOT WANT IT TO ADD WHAT WE columns.get() ALREADY
                        insertion.active_columns.add(c)

                    # BE SURE TO NEST VALUES, IF NEEDED
                    if value_type == "nested":
                        row[c.es_column] = "."
                        deeper_nested_path = [cname] + nested_path
                        insertion = doc_collection.get(cname, None)
                        if not insertion:
                            insertion = doc_collection[cname] = Data(
                                active_columns=set(),
                                rows=[]
                            )
                        for i, r in enumerate(v):
                            child_uid = self.next_uid()
                            _flatten(r, child_uid, uid, i, cname, deeper_nested_path)
                    elif value_type == "object":
                        row[c.es_column] = "."
                        _flatten(v, uid, parent_id, order, cname, nested_path, row=row)
                    elif c.type:
                        row[c.es_column] = v
            else:
                k = "."
                v = data
                cname = concat_field(full_path, k)
                value_type = get_type(v)
                if value_type is None:
                    return

                if value_type in STRUCT:
                    c = unwraplist([c for c in self.columns if c.type in STRUCT])
                else:
                    try:
                        c = unwraplist([c for c in self.columns if c.type == value_type])
                    except Exception, e:
                        Log.error("not expected", cause=e)

                if not c:
                    c = Column(
                        names={self.name: cname},
                        type=value_type,
                        es_column=typed_column(cname, value_type),
                        es_index=self.name,
                        nested_path=nested_path
                    )
                    self.add_column_to_schema(columns, c)
                    if value_type == "nested":
                        nested_tables[cname] = "fake table"
                    required_changes.append({"add": c})

                insertion.active_columns.add(c)

                if value_type == "nested":
                    if c.type == "object":
                        # WE CAN FIX THIS,
                        Log.error("fix this")

                    row[c.es_column] = "."
                    deeper_nested_path = [cname] + nested_path
                    insertion = doc_collection.get(cname, None)
                    if not insertion:
                        doc_collection[cname] = Data(
                            active_columns=set(),
                            rows=[]
                        )
                    for i, r in enumerate(v):
                        child_uid = self.next_uid()
                        _flatten(r, child_uid, uid, i, cname, deeper_nested_path)
                elif value_type == "object":
                    if c.type == "nested":
                        # MOVE TO SINGLE-VALUED LIST
                        child_uid = self.next_uid()
                        row[c.es_column] = "."
                        deeper_nested_path = [cname] + nested_path
                        _flatten(v, child_uid, uid, 0, cname, deeper_nested_path)
                    else:
                        row[c.es_column] = "."
                        _flatten(v, uid, parent_id, order, nested_path, row=row)
                elif c.type:
                    row[c.es_column] = v

        for doc in docs:
            _flatten(doc, self.next_uid(), 0, 0, full_path=path, nested_path=["."])
            if required_changes:
                self.change_schema(required_changes)
            required_changes = []

        return doc_collection

    def _insert(self, collection):
        for nested_path, details in collection.items():
            active_columns = wrap(list(details.active_columns))
            rows = details.rows
            table_name = concat_field(self.name, nested_path)

            if table_name == self.name:
                # DO NOT REQUIRE PARENT OR ORDER COLUMNS
                meta_columns = [UID]
            else:
                meta_columns = [UID, PARENT, ORDER]

            all_columns = meta_columns + active_columns.es_column

            prefix = "INSERT INTO " + quote_table(table_name) + \
                     "(" + ",".join(map(quote_table, all_columns)) + ")"

            # BUILD THE RECORDS
            records = " UNION ALL ".join(
                "\nSELECT " + ",".join(quote_value(row.get(c)) for c in all_columns)
                for row in unwrap(rows)
            )

            self.db.execute(prefix + records)

    def add_column_to_schema(self, nest_to_schema, column):
        abs_table = literal_field(self.name)
        abs_name = column.names[abs_table]

        for nest, schema in nest_to_schema.items():
            rel_table = literal_field(join_field([self.name] + split_field(nest)))
            rel_name = relative_field(abs_name, nest)

            column.names[rel_table] = rel_name


_do_not_quote = re.compile(r"^\w+$", re.UNICODE)


def quote_table(column):
    if _do_not_quote.match(column):
        return column
    return convert.string2quote(column)


def _quote_column(column):
    return convert.string2quote(column.es_column)


def quote_value(value):
    if isinstance(value, (Mapping, list)):
        return "."
    elif isinstance(value, Date):
        return unicode(value.unix)
    elif isinstance(value, Duration):
        return unicode(value.seconds)
    elif isinstance(value, basestring):
        return "'" + value.replace("'", "''") + "'"
    elif value == None:
        return "NULL"
    elif value is True:
        return "1"
    elif value is False:
        return "0"
    else:
        return unicode(value)


def unique_name():
    return Random.string(20)


def column_key(k, v):
    if v == None:
        return None
    elif isinstance(v, basestring):
        return k, "string"
    elif isinstance(v, list):
        return k, None
    elif isinstance(v, Mapping):
        return k, "object"
    elif isinstance(v, Date):
        return k, "number"
    else:
        return k, "number"


def get_type(v):
    if v == None:
        return None
    elif isinstance(v, basestring):
        return "string"
    elif isinstance(v, Mapping):
        return "object"
    elif isinstance(v, (int, float, Date)):
        return "number"
    elif isinstance(v, list):
        return "nested"
    return None


def get_document_value(document, column):
    """
    RETURN DOCUMENT VALUE IF MATCHES THE column (name, type)

    :param document: THE DOCUMENT
    :param column: A (name, type) PAIR
    :return: VALUE, IF IT IS THE SAME NAME AND TYPE
    """
    v = document.get(split_field(column.name)[0], None)
    return get_if_type(v, column.type)


def get_if_type(value, type):
    if is_type(value, type):
        if type == "object":
            return "."
        if isinstance(value, Date):
            return value.unix
        return value
    return None


def is_type(value, type):
    if value == None:
        return False
    elif isinstance(value, basestring) and type == "string":
        return value
    elif isinstance(value, list):
        return False
    elif isinstance(value, Mapping) and type == "object":
        return True
    elif isinstance(value, (int, float, Date)) and type == "number":
        return True
    return False


def typed_column(name, type_):
    if type_ == "nested":
        type_ = "object"
    return concat_field(name, "$" + type_)


def untyped_column(column_name):
    if "$" in column_name:
        return join_field(split_field(column_name)[:-1])
    else:
        return column_name
        # return column_name.split(".$")[0]


def _make_column_name(number):
    return COLUMN + unicode(number)


sql_aggs = {
    "avg": "AVG",
    "average": "AVG",
    "count": "COUNT",
    "first": "FIRST_VALUE",
    "last": "LAST_VALUE",
    "max": "MAX",
    "maximum": "MAX",
    "median": "MEDIAN",
    "min": "MIN",
    "minimum": "MIN",
    "sum": "SUM"
}

sql_types = {
    "string": "TEXT",
    "integer": "INTEGER",
    "number": "REAL",
    "boolean": "INTEGER",
    "object": "TEXT",
    "nested": "TEXT"
}

STATS = {
    "count": "COUNT({{value}})",
    "std": "SQRT((1-1.0/COUNT({{value}}))*VARIANCE({{value}}))",
    "min": "MIN({{value}})",
    "max": "MAX({{value}})",
    "sum": "SUM({{value}})",
    "median": "MEDIAN({{value}})",
    "sos": "SUM({{value}}*{{value}})",
    "var": "(1-1.0/COUNT({{value}}))*VARIANCE({{value}})",
    "avg": "AVG({{value}})"
}

quoted_UID = quote_table(UID)
quoted_ORDER = quote_table(ORDER)
quoted_PARENT = quote_table(PARENT)


def sql_text_array_to_set(column):
    def _convert(row):
        text = row[column]
        return set(eval('[' + text.replace("''", "\'") + ']'))

    return _convert


def get_column(column):
    """
    :param column: The column you want extracted
    :return: a function that can pull the given column out of sql resultset
    """

    def _get(row):
        return row[column]

    return _get


def set_column(row, col, child, value):
    if child == ".":
        row[col] = value
    else:
        column = row[col]
        if column is None:
            column = row[col] = {}
        Data(column)[child] = value


def copy_cols(cols, nest_to_alias):
    """
    MAKE ALIAS FOR EACH COLUMN
    :param cols:
    :param nest_to_alias:  map from nesting level to subquery alias
    :return:
    """
    output = set()
    for c in cols:
        c = copy(c)
        c.es_index = nest_to_alias[c.nested_path[0]]
        output.add(c)
    return output



