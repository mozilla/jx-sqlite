# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import itertools
from collections import Mapping
from datetime import date
from datetime import datetime
from types import NoneType

from mo_collections import UniqueIndex
from mo_dots import Data, wrap, listwrap, unwraplist, FlatList, unwrap, join_field, split_field, NullType, Null
from mo_logs import Log
from mo_threads import Lock
from mo_times.dates import Date
from pyLibrary import convert
from pyLibrary.queries import jx, Schema
from pyLibrary.queries.containers import Container
from pyLibrary.queries.expression_compiler import compile_expression
from pyLibrary.queries.expressions import TRUE_FILTER, jx_expression, Expression, TrueOp, jx_expression_to_function, Variable
from pyLibrary.queries.lists.aggs import is_aggs, list_aggs
from pyLibrary.queries.meta import Column, ROOT_PATH

_get = object.__getattribute__


class ListContainer(Container):
    def __init__(self, name, data, schema=None):
        #TODO: STORE THIS LIKE A CUBE FOR FASTER ACCESS AND TRANSFORMATION
        data = list(unwrap(data))
        Container.__init__(self, data, schema)
        if schema == None:
            self._schema = get_schema_from_list(name, data)
        else:
            self._schema = schema
        self.name = name
        self.data = data
        self.locker = Lock()  # JUST IN CASE YOU WANT TO DO MORE THAN ONE THING

    @property
    def query_path(self):
        return None

    @property
    def schema(self):
        return self._schema

    def last(self):
        """
        :return:  Last element in the list, or Null
        """
        if self.data:
            return self.data[-1]
        else:
            return Null

    def query(self, q):
        q = wrap(q)
        frum = self
        if is_aggs(q):
            frum = list_aggs(frum.data, q)
        else:  # SETOP
            try:
                if q.filter != None or q.esfilter != None:
                    Log.error("use 'where' clause")
            except AttributeError, e:
                pass

            if q.where is not TRUE_FILTER and not isinstance(q.where, TrueOp):
                frum = frum.filter(q.where)

            if q.sort:
                frum = frum.sort(q.sort)

            if q.select:
                frum = frum.select(q.select)
        #TODO: ADD EXTRA COLUMN DESCRIPTIONS TO RESULTING SCHEMA
        for param in q.window:
            frum.window(param)

        return frum

    def update(self, command):
        """
        EXPECTING command == {"set":term, "clear":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS A JSON EXPRESSION FILTER
        """
        command = wrap(command)
        command_clear = listwrap(command["clear"])
        command_set = command.set.items()
        command_where = jx.get(command.where)

        for c in self.data:
            if command_where(c):
                for k in command_clear:
                    c[k] = None
                for k, v in command_set:
                    c[k] = v

    def filter(self, where):
        return self.where(where)

    def where(self, where):
        temp = None
        if isinstance(where, Mapping):
            exec("def temp(row):\n    return "+jx_expression(where).to_python())
        elif isinstance(where, Expression):
            temp = compile_expression(where.to_python())
        else:
            temp = where

        return ListContainer("from "+self.name, filter(temp, self.data), self.schema)

    def sort(self, sort):
        return ListContainer("from "+self.name, jx.sort(self.data, sort, already_normalized=True), self.schema)

    def get(self, select):
        """
        :param select: the variable to extract from list
        :return:  a simple list of the extraction
        """
        if isinstance(select, list):
            return [(d[s] for s in select) for d in self.data]
        else:
            return [d[select] for d in self.data]

    def select(self, select):
        selects = listwrap(select)

        if not all(isinstance(s.value, Variable) for s in selects):
            Log.error("selecting on structure, or expressions, not supported yet")
        if len(selects) == 1 and isinstance(selects[0].value, Variable) and selects[0].value.var == ".":
            new_schema = self.schema
            if selects[0].name == ".":
                return self
        else:
            new_schema = None

        if isinstance(select, list):
            push_and_pull = [(s.name, jx_expression_to_function(s.value)) for s in selects]
            def selector(d):
                output = Data()
                for n, p in push_and_pull:
                    output[n] = p(wrap(d))
                return unwrap(output)

            new_data = map(selector, self.data)
        else:
            select_value = jx_expression_to_function(select.value)
            new_data = map(select_value, self.data)

        return ListContainer("from "+self.name, data=new_data, schema=new_schema)

    def window(self, window):
        _ = window
        jx.window(self.data, window)
        return self

    def having(self, having):
        _ = having
        Log.error("not implemented")

    def format(self, format):
        if format == "table":
            frum = convert.list2table(self.data, self.schema.keys())
        elif format == "cube":
            frum = convert.list2cube(self.data, self.schema.keys())
        else:
            frum = self.__data__()

        return frum

    def groupby(self, keys, contiguous=False):
        try:
            keys = listwrap(keys)
            get_key = jx_expression_to_function(keys)
            if not contiguous:
                data = sorted(self.data, key=get_key)

            def _output():
                for g, v in itertools.groupby(data, get_key):
                    group = Data()
                    for k, gg in zip(keys, g):
                        group[k] = gg
                    yield (group, wrap(list(v)))

            return _output()
        except Exception, e:
            Log.error("Problem grouping", e)

    def insert(self, documents):
        self.data.extend(documents)

    def extend(self, documents):
        self.data.extend(documents)

    def __data__(self):
        return wrap({
            "meta": {"format": "list"},
            "data": [{k: unwraplist(v) for k, v in row.items()} for row in self.data]
        })

    def get_columns(self, table_name=None):
        return self.schema.values()

    def add(self, value):
        self.data.append(value)

    def __getitem__(self, item):
        if item < 0 or len(self.data) <= item:
            return Null
        return self.data[item]

    def __iter__(self):
        return (wrap(d) for d in self.data)

    def __len__(self):
        return len(self.data)


def get_schema_from_list(table_name, frum):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    columns = UniqueIndex(keys=(join_field(["names", table_name]),))
    _get_schema_from_list(frum, table_name, prefix_path=[], nested_path=ROOT_PATH, columns=columns)
    return Schema(table_name=table_name, columns=columns)


def _get_schema_from_list(frum, table_name, prefix_path, nested_path, columns):
    """
    :param frum:  The list
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
                    type="undefined",
                    nested_path=nested_path
                )
                columns.add(column)
            column.type = _merge_type[column.type][row_type]
        else:
            for name, value in d.items():
                full_name = join_field(prefix_path + [name])
                column = columns[full_name]
                if not column:
                    column = Column(
                        names={table_name: full_name},
                        es_column=full_name,
                        es_index=".",
                        type="undefined",
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
                new_type = _merge_type[column.type][this_type]
                column.type = new_type

                if this_type == "object":
                    _get_schema_from_list([value], table_name, prefix_path + [name], nested_path, columns)
                elif this_type == "nested":
                    np = listwrap(nested_path)
                    newpath = unwraplist([join_field(split_field(np[0])+[name])]+np)
                    _get_schema_from_list(value, table_name, prefix_path + [name], newpath, columns)


_type_to_name = {
    NoneType: "undefined",
    NullType: "undefined",
    bool: "boolean",
    str: "string",
    unicode: "string",
    int: "integer",
    long: "long",
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

_merge_type = {
    "undefined": {
        "undefined": "undefined",
        "boolean": "boolean",
        "integer": "integer",
        "long": "long",
        "float": "float",
        "double": "double",
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
        "string": None,
        "object": "nested",
        "nested": "nested"
    }
}



def _exec(code):
    try:
        temp = None
        exec "temp = " + code
        return temp
    except Exception, e:
        Log.error("Could not execute {{code|quote}}", code=code, cause=e)
