# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import, division, unicode_literals

from collections import Mapping

from jx_base import Column, generateGuid
from jx_base.expressions import jx_expression
from jx_sqlite import GUID, ORDER, PARENT, UID, get_if_type, get_type, typed_column, untyped_column
from jx_sqlite.base_table import BaseTable
from jx_sqlite.expressions import json_type_to_sql_type
from mo_dots import Data, Null, concat_field, listwrap, startswith_field, unwrap, unwraplist, wrap, \
    is_many
from mo_future import text
from mo_json import STRUCT, NESTED
from mo_logs import Log
from mo_times import Date
from pyLibrary.sql import SQL_AND, SQL_FROM, SQL_INNER_JOIN, SQL_NULL, SQL_SELECT, SQL_TRUE, SQL_UNION_ALL, SQL_WHERE, \
    sql_iso, sql_list, SQL_VALUES, SQL_INSERT, ConcatSQL, SQL_EQ, SQL_UPDATE, SQL_SET, SQL_ONE, SQL_DELETE
from pyLibrary.sql.sqlite import json_type_to_sqlite_type, quote_column, quote_value


class InsertTable(BaseTable):

    def add(self, doc):
        self.insert([doc])

    def insert(self, docs):
        if not is_many(docs):
            Log.error("Expecting a list of documents")
        doc_collection = self.flatten_many(docs)
        self._insert(doc_collection)

    def update(self, command):
        """
        :param command:  EXPECTING dict WITH {"set": s, "clear": c, "where": w} FORMAT
        """
        command = wrap(command)

        # REJECT DEEP UPDATES
        touched_columns = command.set.keys() | set(listwrap(command['clear']))
        for c in self.schema.columns:
            if c.name in touched_columns and len(c.nested_path) > 1:
                Log.error("Deep update not supported")

        # ADD NEW COLUMNS
        where = jx_expression(command.where)
        _vars = where.vars()
        _map = {
            v: c.es_column
            for v in _vars
            for c in self.columns.get(v, Null)
            if c.jx_type not in STRUCT
        }
        where_sql = where.map(_map).to_sql(self.schema)
        new_columns = set(command.set.keys()) - set(self.columns.keys())
        for new_column_name in new_columns:
            nested_value = command.set[new_column_name]
            ctype = get_type(nested_value)
            column = Column(
                name=new_column_name,
                jx_type=ctype,
                es_index=self.name,
                es_type=json_type_to_sqlite_type(ctype),
                es_column=typed_column(new_column_name, ctype),
                last_updated=Date.now()
            )
            self.add_column(column)

        # UPDATE THE NESTED VALUES
        for nested_column_name, nested_value in command.set.items():
            if get_type(nested_value) == "nested":
                nested_table_name = concat_field(self.name, nested_column_name)
                nested_table = nested_tables[nested_column_name]
                self_primary_key = sql_list(quote_column(c.es_column) for u in self.uid for c in self.columns[u])
                extra_key_name = UID + text(len(self.uid))
                extra_key = [e for e in nested_table.columns[extra_key_name]][0]

                sql_command = (
                    SQL_DELETE + SQL_FROM + quote_column(nested_table.name) +
                    SQL_WHERE + "EXISTS" +
                    sql_iso(
                        SQL_SELECT + SQL_ONE +
                        SQL_FROM + sql_alias(quote_column(nested_table.name), "n") +
                        SQL_INNER_JOIN + sql_iso(
                            SQL_SELECT + self_primary_key +
                            SQL_FROM + quote_column(abs_schema.fact) +
                            SQL_WHERE + where_sql
                    ) +
                    " t ON " +
                    SQL_AND.join(
                        quote_column("t", c.es_column) + SQL_EQ + quote_column("n", c.es_column)
                        for u in self.uid
                        for c in self.columns[u]
                    )
                )
                )
                self.db.execute(sql_command)

                # INSERT NEW RECORDS
                if not nested_value:
                    continue

                doc_collection = {}
                for d in listwrap(nested_value):
                    nested_table.flatten(d, Data(), doc_collection, path=nested_column_name)

                prefix = SQL_INSERT + quote_column(nested_table.name) + sql_iso(sql_list(
                    [self_primary_key] +
                    [quote_column(extra_key)] +
                    [
                        quote_column(c.es_column)
                        for c in doc_collection.get(".", Null).active_columns
                    ]
                ))

                # BUILD THE PARENT TABLES
                parent = (
                    SQL_SELECT + self_primary_key +
                    SQL_FROM + quote_column(abs_schema.fact) +
                    SQL_WHERE + jx_expression(command.where).to_sql(schema)
                )

                # BUILD THE RECORDS
                children = SQL_UNION_ALL.join(
                    SQL_SELECT +
                    quote_value(i) + " " + quote_column(extra_key.es_column) + "," +
                    sql_list(
                        quote_value(row[c.name]) + " " + quote_column(c.es_column)
                        for c in doc_collection.get(".", Null).active_columns
                    )
                    for i, row in enumerate(doc_collection.get(".", Null).rows)
                )

                sql_command = (
                    prefix +
                    SQL_SELECT +
                    sql_list(
                        [quote_column("p", c.es_column) for u in self.uid for c in self.columns[u]] +
                        [quote_column("c", extra_key)] +
                        [quote_column("c", c.es_column) for c in doc_collection.get(".", Null).active_columns]
                    ) +
                    SQL_FROM + sql_iso(parent) + " p" +
                    SQL_INNER_JOIN + sql_iso(children) + " c" + " ON " + SQL_TRUE
                )

                self.db.execute(sql_command)

                # THE CHILD COLUMNS COULD HAVE EXPANDED
                # ADD COLUMNS TO SELF
                for n, cs in nested_table.columns.items():
                    for c in cs:
                        column = Column(
                            name=c.name,
                            jx_type=c.jx_type,
                            es_type=c.es_type,
                            es_index=c.es_index,
                            es_column=c.es_column,
                            nested_path=[nested_column_name] + c.nested_path,
                            last_updated=Date.now()
                        )
                        if c.name not in self.columns:
                            self.columns[column.name] = {column}
                        elif c.jx_type not in [c.jx_type for c in self.columns[c.name]]:
                            self.columns[column.name].add(column)

        command = (
            SQL_UPDATE + quote_column(abs_schema.fact) + SQL_SET +
            sql_list(
                [
                    quote_column(c) + SQL_EQ + quote_value(get_if_type(v, c.jx_type))
                    for k, v in command.set.items()
                    if get_type(v) != "nested"
                    for c in self.columns[k]
                    if c.jx_type != "nested" and len(c.nested_path) == 1
                ] +
                [
                    quote_column(c) + SQL_EQ + SQL_NULL
                    for k in listwrap(command['clear'])
                    if k in self.columns
                    for c in self.columns[k]
                    if c.jx_type != "nested" and len(c.nested_path) == 1
                ]
            ) +
            SQL_WHERE + where_sql
        )

        self.db.execute(command)

    def upsert(self, doc, where):
        old_docs = self.filter(where)
        if len(old_docs) == 0:
            self.insert(doc)
        else:
            self.delete(where)
            self.insert(doc)

    def flatten_many(self, docs, path="."):
        """
        :param docs: THE JSON DOCUMENT
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

        _insertion = Data(
            active_columns=set(),
            rows=[]
        )
        doc_collection = {".": _insertion}
        # KEEP TRACK OF WHAT TABLE WILL BE MADE (SHORTLY)
        required_changes = []
        facts = self.container.get_or_create_facts(self.name)
        snowflake = facts.snowflake

        def _flatten(data, uid, parent_id, order, full_path, nested_path, row=None, guid=None):
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
            table = concat_field(self.name, nested_path[0])
            insertion = doc_collection[nested_path[0]]
            if not row:
                row = {GUID: guid, UID: uid, PARENT: parent_id, ORDER: order}
                insertion.rows.append(row)

            if isinstance(data, Mapping):
                items = ((concat_field(full_path, k), v ) for k, v in wrap(data).leaves())
            else:
                # PRIMITIVE VALUES
                items = [(full_path, data)]

            for cname, v in items:
                value_type = get_type(v)
                if value_type is None:
                    continue

                if value_type == NESTED:
                    c = unwraplist([
                        cc
                        for cc in snowflake.columns
                        if cc.jx_type in STRUCT and untyped_column(cc.name) == cname
                    ])
                else:
                    c = unwraplist([
                        cc
                        for cc in snowflake.columns
                        if cc.jx_type == value_type and cc.name == cname
                    ])

                insertion = doc_collection[nested_path[0]]
                if not c:
                    # WHAT IS THE NESTING LEVEL FOR THIS PATH?
                    deeper_nested_path = "."
                    for path in snowflake.query_paths:
                        if startswith_field(cname, path) and len(deeper_nested_path) < len(path):
                            deeper_nested_path = path

                    c = Column(
                        name=cname,
                        jx_type=value_type,
                        es_type=json_type_to_sqlite_type.get(value_type, value_type),
                        es_column=typed_column(cname, json_type_to_sql_type.get(value_type)),
                        es_index=table,
                        nested_path=nested_path,
                        last_updated=Date.now()
                    )
                    if value_type == "nested":
                        snowflake.query_paths.append(c.es_column)
                        required_changes.append({'nest': (c, nested_path)})
                    else:
                        snowflake.columns.append(c)
                        required_changes.append({"add": c})

                        # INSIDE IF BLOCK BECAUSE WE DO NOT WANT IT TO ADD WHAT WE columns.get() ALREADY
                        insertion.active_columns.add(c)
                elif c.jx_type == "nested" and value_type == "object":
                    value_type = "nested"
                    v = [v]
                elif len(c.nested_path) < len(nested_path):
                    from_doc = doc_collection.get(c.nested_path[0], None)
                    column = c.es_column
                    from_doc.active_columns.remove(c)
                    snowflake._remove_column(c)
                    required_changes.append({"nest": (c, nested_path)})
                    deep_c = Column(
                        name=cname,
                        jx_type=value_type,
                        es_type=json_type_to_sqlite_type.get(value_type, value_type),
                        es_column=typed_column(cname, json_type_to_sql_type.get(value_type)),
                        es_index=table,
                        nested_path=nested_path,
                        last_updated=Date.now()
                    )
                    snowflake._add_column(deep_c)
                    snowflake._drop_column(c)
                    from_doc.active_columns.remove(c)

                    for r in from_doc.rows:
                        r1 = unwrap(r)
                        if column in r1:
                            row1 = {UID: self.container.next_uid(), PARENT: r1["__id__"], ORDER: 0, column: r1[column]}
                            insertion.rows.append(row1)
                elif len(c.nested_path) > len(nested_path):
                    insertion = doc_collection[c.nested_path[0]]
                    row = {UID: self.container.next_uid(), PARENT: uid, ORDER: order}
                    insertion.rows.append(row)

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
                        child_uid = self.container.next_uid()
                        _flatten(r, child_uid, uid, i, cname, deeper_nested_path)
                elif value_type == "object":
                    row[c.es_column] = "."
                    _flatten(v, uid, parent_id, order, cname, nested_path, row=row)
                elif c.jx_type:
                    insertion.active_columns.add(c)
                    row[c.es_column] = v

        for doc in docs:
            _flatten(doc, self.container.next_uid(), 0, 0, full_path=path, nested_path=["."], guid=generateGuid())
            if required_changes:
                snowflake.change_schema(required_changes)
            required_changes = []

        return doc_collection

    def _insert(self, collection):
        for nested_path, details in collection.items():
            active_columns = wrap(list(details.active_columns))
            rows = details.rows
            num_rows = len(rows)
            table_name = concat_field(self.name, nested_path)

            if table_name == self.name:
                # DO NOT REQUIRE PARENT OR ORDER COLUMNS
                meta_columns = [GUID, UID]
            else:
                meta_columns = [UID, PARENT, ORDER]

            all_columns = meta_columns + active_columns.es_column  # ONLY THE PRIMITIVE VALUE COLUMNS
            command = ConcatSQL([
                SQL_INSERT,
                quote_column(table_name),
                sql_iso(sql_list(map(quote_column, all_columns))),
                SQL_VALUES,
                sql_list(
                    sql_iso(sql_list(quote_value(row.get(c)) for c in all_columns))
                    for row in unwrap(rows)
                )
            ])

            with self.db.transaction() as t:
                t.execute(command)
