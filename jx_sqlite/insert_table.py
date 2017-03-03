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

from collections import Mapping
from copy import copy

from mo_dots import listwrap, Data, wrap, Null, unwraplist, startswith_field, unwrap, \
    concat_field
from mo_logs import Log

from jx_sqlite import typed_column, quote_table, _quote_column, get_type, quote_value, ORDER, UID, PARENT, get_if_type
from jx_sqlite.alter_table import AlterTable
from pyLibrary.queries.containers import STRUCT
from pyLibrary.queries.expressions import jx_expression
from pyLibrary.queries.meta import Column


class InsertTable(AlterTable):

    def add(self, doc):
        self.insert([doc])

    def insert(self, docs):
        doc_collection = self.flatten_many(docs)
        self._insert(doc_collection)

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
