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
from datetime import date
from datetime import datetime

import jx_base
from jx_base import python_type_to_json_type
from jx_base import STRUCT, Column, Table
from jx_base.schema import Schema
from jx_python import jx
from mo_collections import UniqueIndex
from mo_dots import Data, concat_field, get_attr, listwrap, unwraplist, NullType, FlatList, set_default, split_field, join_field, ROOT_PATH, wrap
from mo_future import none_type, text_type, long, PY2
from mo_json.typed_encoder import untype_path, unnest_path
from mo_logs import Log
from mo_threads import Lock
from mo_times.dates import Date

singlton = None


class ColumnList(Table):
    """
    OPTIMIZED FOR THE PARTICULAR ACCESS PATTERNS USED
    """

    def __init__(self):
        Table.__init__(self, "meta.columns")
        self.data = {}  # MAP FROM ES_INDEX TO (abs_column_name to COLUMNS)
        self.locker = Lock()
        self._schema = None
        self.extend(METADATA_COLUMNS)

    def find(self, es_index, abs_column_name):
        with self.locker:
            if es_index.startswith("meta."):
                self._update_meta()

            if not abs_column_name:
                return [c for cs in self.data.get(es_index, {}).values() for c in cs]
            else:
                return self.data.get(es_index, {}).get(abs_column_name, [])

    def extend(self, columns):
        self.dirty = True
        with self.locker:
            for column in columns:
                self._add(column)

    def add(self, column):
        self.dirty = True
        with self.locker:
            return self._add(column)

    def _add(self, column):
        columns_for_table = self.data.setdefault(column.es_index, {})
        existing_columns = columns_for_table.setdefault(column.names["."], [])

        for canonical in existing_columns:
            if canonical is column:
                return canonical
            if canonical.es_type == column.es_type:
                set_default(column.names, canonical.names)
                for key in Column.__slots__:
                    canonical[key] = column[key]
                return canonical
        existing_columns.append(column)
        return column

    def _update_meta(self):
        if not self.dirty:
            return

        for mcl in self.data.get("meta.columns").values():
            for mc in mcl:
                count = 0
                values = set()
                objects = 0
                multi = 1
                for t, cs in self.data.items():
                    for c, css in cs.items():
                        for column in css:
                            value = column[mc.names["."]]
                            if value == None:
                                pass
                            else:
                                count += 1
                                if isinstance(value, list):
                                    multi = max(multi, len(value))
                                    try:
                                        values |= set(value)
                                    except Exception:
                                        objects += len(value)
                                elif isinstance(value, Mapping):
                                    objects += 1
                                else:
                                    values.add(value)
                mc.count = count
                mc.cardinality = len(values) + objects
                mc.partitions = jx.sort(values)
                mc.multi = multi
                mc.last_updated = Date.now()
        self.dirty = False

    def __iter__(self):
        self._update_meta()
        for t, cs in self.data.items():
            for c, css in cs.items():
                for column in css:
                    yield column

    def __len__(self):
        return self.data['meta.columns']['es_index'].count

    def update(self, command):
        self.dirty = True
        try:
            command = wrap(command)
            eq = command.where.eq
            if eq.es_index:
                columns = self.find(eq.es_index, eq.name)
                columns = [c for c in columns if all(get_attr(c, k) == v for k, v in eq.items())]
            else:
                with self.locker:
                    columns = list(self)
                    columns = jx.filter(columns, command.where)

            with self.locker:
                for col in list(columns):
                    for k in command["clear"]:
                        if k == ".":
                            columns.remove(col)
                        else:
                            col[k] = None

                    for k, v in command.set.items():
                        col[k] = v
        except Exception as e:
            Log.error("should not happen", cause=e)

    def query(self, query):
        with self.locker:
            self._update_meta()
            query.frum = self.__iter__()
            output = jx.run(query)

        return output

    def groupby(self, keys):
        with self.locker:
            self._update_meta()
            return jx.groupby(self.__iter__(), keys)

    @property
    def schema(self):
        if not self._schema:
            with self.locker:
                self._update_meta()
                self._schema = Schema(".", [c for cs in self.data["meta.columns"].values() for c in cs])
        return self._schema

    @property
    def namespace(self):
        return self

    def denormalized(self):
        """
        THE INTERNAL STRUCTURE FOR THE COLUMN METADATA IS VERY DIFFERENT FROM
        THE DENORMALIZED PERSPECITVE. THIS PROVIDES THAT PERSPECTIVE FOR QUERIES
        """
        with self.locker:
            self._update_meta()
            output = [
                {
                    "table": concat_field(c.es_index, untype_path(table)),
                    "name": untype_path(name),
                    "cardinality": c.cardinality,
                    "es_column": c.es_column,
                    "es_index": c.es_index,
                    "last_updated": c.last_updated,
                    "count": c.count,
                    "nested_path": [unnest_path(n) for n in c.nested_path],
                    "es_type": c.es_type,
                    "type": c.jx_type
                }
                for tname, css in self.data.items()
                for cname, cs in css.items()
                for c in cs
                if c.jx_type not in STRUCT  # and c.es_column != "_id"
                for table, name in c.names.items()
            ]

        from jx_python.containers.list_usingPythonList import ListContainer
        return ListContainer(
            self.name,
            data=output,
            schema=jx_base.Schema(
                "meta.columns",
                SIMPLE_METADATA_COLUMNS
            )
        )


def get_schema_from_list(table_name, frum):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    columns = UniqueIndex(keys=("names.\\.",))
    _get_schema_from_list(frum, ".", prefix_path=[], nested_path=ROOT_PATH, columns=columns)
    return Schema(table_name=table_name, columns=list(columns))


def _get_schema_from_list(frum, table_name, prefix_path, nested_path, columns):
    """
    :param frum: The list
    :param table_name: Name of the table this list holds records for
    :param prefix_path: parent path
    :param nested_path: each nested array, in reverse order
    :param columns: map from full name to column definition
    :return:
    """

    for d in frum:
        row_type = _type_to_name[d.__class__]
        if row_type != "object":
            full_name = join_field(prefix_path)
            column = columns[full_name]
            if not column:
                column = Column(
                    names={table_name: full_name},
                    es_column=full_name,
                    es_index=".",
                    jx_type=python_type_to_json_type[d.__class__],
                    es_type=row_type,
                    nested_path=nested_path
                )
                columns.add(column)
            column.es_type = _merge_type[column.es_type][row_type]
            column.jx_type = _merge_type[column.jx_type][row_type]
        else:
            for name, value in d.items():
                full_name = join_field(prefix_path + [name])
                column = columns[full_name]
                if not column:
                    column = Column(
                        names={table_name: full_name},
                        es_column=full_name,
                        es_index=".",
                        es_type="undefined",
                        nested_path=nested_path
                    )
                    columns.add(column)
                if isinstance(value, list):
                    if len(value) == 0:
                        this_type = "undefined"
                    elif len(value) == 1:
                        this_type = _type_to_name[value[0].__class__]
                    else:
                        this_type = _type_to_name[value[0].__class__]
                        if this_type == "object":
                            this_type = "nested"
                else:
                    this_type = _type_to_name[value.__class__]
                new_type = _merge_type[column.es_type][this_type]
                column.es_type = new_type

                if this_type == "object":
                    _get_schema_from_list([value], table_name, prefix_path + [name], nested_path, columns)
                elif this_type == "nested":
                    np = listwrap(nested_path)
                    newpath = unwraplist([join_field(split_field(np[0]) + [name])] + np)
                    _get_schema_from_list(value, table_name, prefix_path + [name], newpath, columns)


METADATA_COLUMNS = (
    [
        Column(
            names={".": c},
            es_index="meta.columns",
            es_column=c,
            es_type="string",
            nested_path=ROOT_PATH
        )
        for c in ["es_type", "jx_type", "nested_path", "es_column", "es_index"]
    ] + [
        Column(
            es_index="meta.columns",
            names={".": c},
            es_column=c,
            es_type="object",
            nested_path=ROOT_PATH
        )
        for c in ["names", "partitions"]
    ] + [
        Column(
            names={".": c},
            es_index="meta.columns",
            es_column=c,
            es_type="long",
            nested_path=ROOT_PATH
        )
        for c in ["count", "cardinality", "multi"]
    ] + [
        Column(
            names={".": "last_updated"},
            es_index="meta.columns",
            es_column="last_updated",
            es_type="time",
            nested_path=ROOT_PATH
        )
    ]
)

SIMPLE_METADATA_COLUMNS = (
    [
        Column(
            names={".": c},
            es_index="meta.columns",
            es_column=c,
            es_type="string",
            nested_path=ROOT_PATH
        )
        for c in ["table", "name", "type", "nested_path"]
    ] + [
        Column(
            names={".": c},
            es_index="meta.columns",
            es_column=c,
            es_type="long",
            nested_path=ROOT_PATH
        )
        for c in ["count", "cardinality", "multi"]
    ] + [
        Column(
            names={".": "last_updated"},
            es_index="meta.columns",
            es_column="last_updated",
            es_type="time",
            nested_path=ROOT_PATH
        )
    ]
)


_type_to_name = {
    none_type: "undefined",
    NullType: "undefined",
    bool: "boolean",
    str: "string",
    text_type: "string",
    int: "integer",
    float: "double",
    Data: "object",
    dict: "object",
    set: "nested",
    list: "nested",
    FlatList: "nested",
    Date: "double",
    datetime: "double",
    date: "double"
}

if PY2:
    _type_to_name[long] = "integer"

_merge_type = {
    "undefined": {
        "undefined": "undefined",
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": "object",
        "nested": "nested"
    },
    "boolean": {
        "undefined": "boolean",
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "integer": {
        "undefined": "integer",
        "boolean": "integer",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "long": {
        "undefined": "long",
        "boolean": "long",
        "integer": "long",
        "long": "long",
        "float": "double",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "float": {
        "undefined": "float",
        "boolean": "float",
        "integer": "float",
        "long": "double",
        "float": "float",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "double": {
        "undefined": "double",
        "boolean": "double",
        "integer": "double",
        "long": "double",
        "float": "double",
        "double": "double",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "number": {
        "undefined": "number",
        "boolean": "number",
        "integer": "number",
        "long": "number",
        "float": "number",
        "double": "number",
        "number": "number",
        "string": "string",
        "object": None,
        "nested": None
    },
    "string": {
        "undefined": "string",
        "boolean": "string",
        "integer": "string",
        "long": "string",
        "float": "string",
        "double": "string",
        "number": "string",
        "string": "string",
        "object": None,
        "nested": None
    },
    "object": {
        "undefined": "object",
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "number": None,
        "string": None,
        "object": "object",
        "nested": "nested"
    },
    "nested": {
        "undefined": "nested",
        "boolean": None,
        "integer": None,
        "long": None,
        "float": None,
        "double": None,
        "number": None,
        "string": None,
        "object": "nested",
        "nested": "nested"
    }
}
