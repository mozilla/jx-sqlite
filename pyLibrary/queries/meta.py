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

import itertools
from copy import copy
from itertools import product
from types import NoneType

from datetime import date

from datetime import datetime

from mo_collections import UniqueIndex
from mo_logs import Log
from mo_threads import Lock, THREAD_STOP
from mo_threads import Queue
from mo_threads import Thread
from mo_threads import Till
from mo_times.dates import Date
from mo_times.durations import HOUR, MINUTE
from mo_times.timer import Timer
from mo_dots import Data, relative_field, concat_field, get_attr, listwrap, unwraplist, NullType, FlatList
from mo_dots import coalesce, set_default, Null, literal_field, split_field, join_field, ROOT_PATH
from mo_dots import wrap
from mo_kwargs import override
from pyLibrary.meta import DataClass
from pyLibrary.queries import jx, Schema
from pyLibrary.queries.containers import STRUCT, Container
from pyLibrary.queries.query import QueryOp

_elasticsearch = None

MAX_COLUMN_METADATA_AGE = 12 * HOUR
ENABLE_META_SCAN = False
DEBUG = False
TOO_OLD = 2*HOUR
OLD_METADATA = MINUTE
singlton = None
TEST_TABLE_PREFIX = "testing"  # USED TO TURN OFF COMPLAINING ABOUT TEST INDEXES


class FromESMetadata(Schema):
    """
    QUERY THE METADATA
    """

    def __new__(cls, *args, **kwargs):
        global singlton
        if singlton:
            return singlton
        else:
            singlton = object.__new__(cls)
            return singlton

    @override
    def __init__(self, host, index, alias=None, name=None, port=9200, kwargs=None):
        global _elasticsearch
        if hasattr(self, "settings"):
            return

        from pyLibrary.queries.containers.list_usingPythonList import ListContainer
        from pyLibrary.env import elasticsearch as _elasticsearch

        self.settings = kwargs
        self.default_name = coalesce(name, alias, index)
        self.default_es = _elasticsearch.Cluster(kwargs=kwargs)
        self.todo = Queue("refresh metadata", max=100000, unique=True)

        self.es_metadata = Null
        self.last_es_metadata = Date.now()-OLD_METADATA

        self.meta=Data()
        table_columns = metadata_tables()
        column_columns = metadata_columns()
        self.meta.tables = ListContainer("meta.tables", [], wrap({c.names["."]: c for c in table_columns}))
        self.meta.columns = ColumnList()
        self.meta.columns.insert(column_columns)
        self.meta.columns.insert(table_columns)
        # TODO: fix monitor so it does not bring down ES
        if ENABLE_META_SCAN:
            self.worker = Thread.run("refresh metadata", self.monitor)
        else:
            self.worker = Thread.run("refresh metadata", self.not_monitor)
        return

    @property
    def query_path(self):
        return None

    @property
    def url(self):
        return self.default_es.path + "/" + self.default_name.replace(".", "/")

    def get_table(self, table_name):
        with self.meta.tables.locker:
            return wrap([t for t in self.meta.tables.data if t.name == table_name])

    def _upsert_column(self, c):
        # ASSUMING THE  self.meta.columns.locker IS HAD
        existing_columns = self.meta.columns.find(c.es_index, c.names["."])
        if not existing_columns:
            self.meta.columns.add(c)
            self.todo.add(c)

            if ENABLE_META_SCAN:
                if DEBUG:
                    Log.note("todo: {{table}}::{{column}}", table=c.es_index, column=c.es_column)
                # MARK meta.columns AS DIRTY TOO
                cols = self.meta.columns.find("meta.columns", None)
                for cc in cols:
                    cc.partitions = cc.cardinality = None
                    cc.last_updated = Date.now()
                self.todo.extend(cols)
        else:
            canonical = existing_columns[0]
            if canonical is not c:
                set_default(c.names, canonical.names)
                for key in Column.__slots__:
                    canonical[key] = c[key]
            if DEBUG:
                Log.note("todo: {{table}}::{{column}}", table=canonical.es_index, column=canonical.es_column)
            self.todo.add(canonical)

    def _get_columns(self, table=None):
        # TODO: HANDLE MORE THEN ONE ES, MAP TABLE SHORT_NAME TO ES INSTANCE
        table_path = split_field(table)
        es_index = table_path[0]
        query_path = join_field(table_path[1:])
        meta = self.es_metadata.indices[es_index]
        if not meta or self.last_es_metadata < Date.now() - OLD_METADATA:
            self.es_metadata = self.default_es.get_metadata(force=True)
            meta = self.es_metadata.indices[es_index]

        for _, properties in meta.mappings.items():
            properties.properties["_id"] = {"type": "string", "index": "not_analyzed"}
            self._parse_properties(meta.index, properties, meta)

    def _parse_properties(self, abs_index, properties, meta):
        # IT IS IMPORTANT THAT NESTED PROPERTIES NAME ALL COLUMNS, AND
        # ALL COLUMNS ARE GIVEN NAMES FOR ALL NESTED PROPERTIES
        abs_columns = _elasticsearch.parse_properties(abs_index, None, properties.properties)
        abs_columns = abs_columns.filter(  # TODO: REMOVE WHEN jobs PROPERTY EXPLOSION IS CONTAINED
            lambda r: not r.es_column.startswith("other.") and
                      not r.es_column.startswith("previous_values.cf_") and
                      not r.es_index.startswith("debug") and
                      r.es_column.find("=") == -1 and
                      r.es_column.find(" ") == -1
        )

        def add_column(c, query_path):
            c.last_updated = Date.now()
            if query_path[0] != ".":
                c.names[query_path[0]] = relative_field(c.names["."], query_path[0])

            with self.meta.columns.locker:
                self._upsert_column(c)
                for alias in meta.aliases:
                    c = copy(c)
                    c.es_index = alias
                    self._upsert_column(c)

        with Timer("upserting {{num}} columns", {"num": len(abs_columns)}, debug=DEBUG):
            # LIST OF EVERY NESTED PATH
            query_paths = [[c.es_column] for c in abs_columns if c.type == "nested"]
            for a, b in itertools.product(query_paths, query_paths):
                aa = a[0]
                bb = b[0]
                if aa and bb.startswith(aa):
                    for i, b_prefix in enumerate(b):
                        if len(b_prefix) > len(aa):
                            continue
                        if aa == b_prefix:
                            break  # SPLIT ALREADY FOUND
                        b.insert(i, aa)
                        break
            for q in query_paths:
                q.append(".")
            query_paths.append(ROOT_PATH)

            # ADD RELATIVE COLUMNS
            for abs_column in abs_columns:
                for query_path in query_paths:
                    add_column(abs_column, query_path)

    def query(self, _query):
        return self.meta.columns.query(QueryOp(set_default(
            {
                "from": self.meta.columns,
                "sort": ["table", "name"]
            },
            _query.__data__()
        )))

    def get_columns(self, table_name, column_name=None, force=False):
        """
        RETURN METADATA COLUMNS
        """
        table_path = split_field(table_name)
        es_index_name = table_path[0]
        query_path = join_field(table_path[1:])
        table = self.get_table(es_index_name)[0]
        abs_column_name = None if column_name == None else concat_field(query_path, column_name)

        try:
            # LAST TIME WE GOT INFO FOR THIS TABLE
            if not table:
                table = Table(
                    name=es_index_name,
                    url=None,
                    query_path=None,
                    timestamp=Date.now()
                )
                with self.meta.tables.locker:
                    self.meta.tables.add(table)
                self._get_columns(table=es_index_name)
            elif force or table.timestamp == None or table.timestamp < Date.now() - MAX_COLUMN_METADATA_AGE:
                table.timestamp = Date.now()
                self._get_columns(table=es_index_name)

            with self.meta.columns.locker:
                columns = self.meta.columns.find(es_index_name, column_name)
            if columns:
                columns = jx.sort(columns, "names.\.")
                # AT LEAST WAIT FOR THE COLUMNS TO UPDATE
                while len(self.todo) and not all(columns.get("last_updated")):
                    if DEBUG:
                        Log.note("waiting for columns to update {{columns|json}}", columns=[c.es_index+"."+c.es_column for c in columns if not c.last_updated])
                    Till(seconds=1).wait()
                return columns
        except Exception as e:
            Log.error("Not expected", cause=e)

        if abs_column_name:
            Log.error("no columns matching {{table}}.{{column}}", table=table_name, column=abs_column_name)
        else:
            self._get_columns(table=table_name)  # TO TEST WHAT HAPPENED
            Log.error("no columns for {{table}}?!", table=table_name)

    def _update_cardinality(self, c):
        """
        QUERY ES TO FIND CARDINALITY AND PARTITIONS FOR A SIMPLE COLUMN
        """
        if c.type in STRUCT:
            Log.error("not supported")
        try:
            if c.es_index == "meta.columns":
                with self.meta.columns.locker:
                    partitions = jx.sort([g[c.es_column] for g, _ in jx.groupby(self.meta.columns, c.es_column) if g[c.es_column] != None])
                    self.meta.columns.update({
                        "set": {
                            "partitions": partitions,
                            "count": len(self.meta.columns),
                            "cardinality": len(partitions),
                            "last_updated": Date.now()
                        },
                        "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                    })
                return
            if c.es_index == "meta.tables":
                with self.meta.columns.locker:
                    partitions = jx.sort([g[c.es_column] for g, _ in jx.groupby(self.meta.tables, c.es_column) if g[c.es_column] != None])
                    self.meta.columns.update({
                        "set": {
                            "partitions": partitions,
                            "count": len(self.meta.tables),
                            "cardinality": len(partitions),
                            "last_updated": Date.now()
                        },
                        "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                    })
                return

            es_index = c.es_index.split(".")[0]
            result = self.default_es.post("/" + es_index + "/_search", data={
                "aggs": {c.names["."]: _counting_query(c)},
                "size": 0
            })
            r = result.aggregations.values()[0]
            count = result.hits.total
            cardinality = coalesce(r.value, r._nested.value, 0 if r.doc_count==0 else None)
            if cardinality == None:
                Log.error("logic error")

            query = Data(size=0)
            if cardinality > 1000 or (count >= 30 and cardinality == count) or (count >= 1000 and cardinality / count > 0.99):
                if DEBUG:
                    Log.note("{{table}}.{{field}} has {{num}} parts", table=c.es_index, field=c.es_column, num=cardinality)
                with self.meta.columns.locker:
                    self.meta.columns.update({
                        "set": {
                            "count": count,
                            "cardinality": cardinality,
                            "last_updated": Date.now()
                        },
                        "clear": ["partitions"],
                        "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                    })
                return
            elif c.type in _elasticsearch.ES_NUMERIC_TYPES and cardinality > 30:
                if DEBUG:
                    Log.note("{{field}} has {{num}} parts", field=c.name, num=cardinality)
                with self.meta.columns.locker:
                    self.meta.columns.update({
                        "set": {
                            "count": count,
                            "cardinality": cardinality,
                            "last_updated": Date.now()
                        },
                        "clear": ["partitions"],
                        "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                    })
                return
            elif len(c.nested_path) != 1:
                query.aggs[literal_field(c.names["."])] = {
                    "nested": {"path": c.nested_path[0]},
                    "aggs": {"_nested": {"terms": {"field": c.es_column, "size": 0}}}
                }
            else:
                query.aggs[literal_field(c.names["."])] = {"terms": {"field": c.es_column, "size": 0}}

            result = self.default_es.post("/" + es_index + "/_search", data=query)

            aggs = result.aggregations.values()[0]
            if aggs._nested:
                parts = jx.sort(aggs._nested.buckets.key)
            else:
                parts = jx.sort(aggs.buckets.key)

            if DEBUG:
                Log.note("{{field}} has {{parts}}", field=c.name, parts=parts)
            with self.meta.columns.locker:
                self.meta.columns.update({
                    "set": {
                        "count": count,
                        "cardinality": cardinality,
                        "partitions": parts,
                        "last_updated": Date.now()
                    },
                    "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                })
        except Exception as e:
            if "IndexMissingException" in e and c.es_index.startswith(TEST_TABLE_PREFIX):
                with self.meta.columns.locker:
                    self.meta.columns.update({
                        "set": {
                            "count": 0,
                            "cardinality": 0,
                            "last_updated": Date.now()
                        },
                        "clear":[
                            "partitions"
                        ],
                        "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                    })
            else:
                self.meta.columns.update({
                    "set": {
                        "last_updated": Date.now()
                    },
                    "clear": [
                        "count",
                        "cardinality",
                        "partitions",
                    ],
                    "where": {"eq": {"names.\\.": ".", "es_index": c.es_index, "es_column": c.es_column}}
                })
                Log.warning("Could not get {{col.es_index}}.{{col.es_column}} info", col=c, cause=e)

    def monitor(self, please_stop):
        please_stop.on_go(lambda: self.todo.add(THREAD_STOP))
        while not please_stop:
            try:
                if not self.todo:
                    with self.meta.columns.locker:
                        old_columns = [
                            c
                            for c in self.meta.columns
                            if (c.last_updated == None or c.last_updated < Date.now()-TOO_OLD) and c.type not in STRUCT
                        ]
                        if old_columns:
                            if DEBUG:
                                Log.note("Old columns wth dates {{dates|json}}", dates=wrap(old_columns).last_updated)
                            self.todo.extend(old_columns)
                            # TEST CONSISTENCY
                            for c, d in product(list(self.todo.queue), list(self.todo.queue)):
                                if c.es_column == d.es_column and c.es_index == d.es_index and c != d:
                                    Log.error("")
                        else:
                            if DEBUG:
                                Log.note("no more metatdata to update")

                column = self.todo.pop(Till(seconds=(10*MINUTE).seconds))
                if column:
                    if DEBUG:
                        Log.note("update {{table}}.{{column}}", table=column.es_index, column=column.es_column)
                    if column.type in STRUCT:
                        with self.meta.columns.locker:
                            column.last_updated = Date.now()
                        continue
                    elif column.last_updated >= Date.now()-TOO_OLD:
                        continue
                    try:
                        self._update_cardinality(column)
                        if DEBUG and not column.es_index.startswith(TEST_TABLE_PREFIX):
                            Log.note("updated {{column.name}}", column=column)
                    except Exception as e:
                        Log.warning("problem getting cardinality for {{column.name}}", column=column, cause=e)
            except Exception as e:
                Log.warning("problem in cardinality monitor", cause=e)

    def not_monitor(self, please_stop):
        Log.alert("metadata scan has been disabled")
        please_stop.on_go(lambda: self.todo.add(THREAD_STOP))
        while not please_stop:
            c = self.todo.pop()
            if c == THREAD_STOP:
                break

            if not c.last_updated or c.last_updated >= Date.now()-TOO_OLD:
                continue

            with self.meta.columns.locker:
                self.meta.columns.update({
                    "set": {
                        "last_updated": Date.now()
                    },
                    "clear":[
                        "count",
                        "cardinality",
                        "partitions",
                    ],
                    "where": {"eq": {"es_index": c.es_index, "es_column": c.es_column}}
                })
            if DEBUG:
                Log.note("Could not get {{col.es_index}}.{{col.es_column}} info", col=c)


def _counting_query(c):
    if len(c.nested_path) != 1:
        return {
            "nested": {
                "path": c.nested_path[0]  # FIRST ONE IS LONGEST
            },
            "aggs": {
                "_nested": {"cardinality": {
                    "field": c.es_column,
                    "precision_threshold": 10 if c.type in _elasticsearch.ES_NUMERIC_TYPES else 100
                }}
            }
        }
    else:
        return {"cardinality": {
            "field": c.es_column
        }}


def metadata_columns():
    return wrap(
        [
            Column(
                names={".":c},
                es_index="meta.columns",
                es_column=c,
                type="string",
                nested_path=ROOT_PATH
            )
            for c in [
                "type",
                "nested_path",
                "es_column",
                "es_index"
            ]
        ] + [
            Column(
                es_index="meta.columns",
                names={".":c},
                es_column=c,
                type="object",
                nested_path=ROOT_PATH
            )
            for c in [
                "names",
                "domain",
                "partitions"
            ]
        ] + [
            Column(
                names={".": c},
                es_index="meta.columns",
                es_column=c,
                type="long",
                nested_path=ROOT_PATH
            )
            for c in [
                "count",
                "cardinality"
            ]
        ] + [
            Column(
                names={".": "last_updated"},
                es_index="meta.columns",
                es_column="last_updated",
                type="time",
                nested_path=ROOT_PATH
            )
        ]
    )


def metadata_tables():
    return wrap(
        [
            Column(
                names={".": c},
                es_index="meta.tables",
                es_column=c,
                type="string",
                nested_path=ROOT_PATH
            )
            for c in [
                "name",
                "url",
                "query_path"
            ]
        ]+[
            Column(
                names={".": "timestamp"},
                es_index="meta.tables",
                es_column="timestamp",
                type="integer",
                nested_path=ROOT_PATH
            )
        ]
    )


class Table(DataClass("Table", [
    "name",
    "url",
    "query_path",
    "timestamp"
])):
    @property
    def columns(self):
        return FromESMetadata.singlton.get_columns(table_name=self.name)


Column = DataClass(
    "Column",
    [
        # "table",
        "names",  # MAP FROM TABLE NAME TO COLUMN NAME (ONE COLUMN CAN HAVE MULTIPLE NAMES)
        "es_column",
        "es_index",
        # "es_type",
        "type",
        {"name": "useSource", "default": False},
        {"name": "nested_path", "nulls": True},  # AN ARRAY OF PATHS (FROM DEEPEST TO SHALLOWEST) INDICATING THE JSON SUB-ARRAYS
        {"name": "count", "nulls": True},
        {"name": "cardinality", "nulls": True},
        {"name": "partitions", "nulls": True},
        {"name": "last_updated", "nulls": True}
    ]
)


class ColumnList(Container):
    """
    OPTIMIZED FOR THE PARTICULAR ACCESS PATTERNS USED
    """

    def __init__(self):
        self.data = {}  # MAP FROM ES_INDEX TO (abs_column_name to COLUMNS)
        self.locker = Lock()
        self.count = 0
        self.meta_schema = None

    def find(self, es_index, abs_column_name):
        if "." in es_index and not es_index.startswith("meta."):
            Log.error("unlikely index name")
        if not abs_column_name:
            return [c for cs in self.data.get(es_index, {}).values() for c in cs]
        else:
            return self.data.get(es_index, {}).get(abs_column_name, [])

    def insert(self, columns):
        for column in columns:
            self.add(column)

    def add(self, column):
        columns_for_table = self.data.setdefault(column.es_index, {})
        _columns = columns_for_table.setdefault(column.names["."], [])
        _columns.append(column)
        self.count += 1

    def __iter__(self):
        for t, cs in self.data.items():
            for c, css in cs.items():
                for column in css:
                    yield column

    def __len__(self):
        return self.count

    def update(self, command):
        try:
            command = wrap(command)
            eq = command.where.eq
            if eq.es_index:
                columns = self.find(eq.es_index, eq.name)
                columns = [c for c in columns if all(get_attr(c, k) == v for k, v in eq.items())]
            else:
                columns = list(self)
                columns = jx.filter(columns, command.where)

            for col in columns:
                for k in command["clear"]:
                    col[k] = None

                for k, v in command.set.items():
                    col[k] = v
        except Exception as e:
            Log.error("sould not happen", cause=e)

    def query(self, query):
        query.frum = self.__iter__()
        output = jx.run(query)

        return output

    def groupby(self, keys):
        return jx.groupby(self.__iter__(), keys)

    @property
    def schema(self):
        return wrap({k: set(v) for k, v in self.data["meta.columns"].items()})

    def denormalized(self):
        """
        THE INTERNAL STRUCTURE FOR THE COLUMN METADATA IS VERY DIFFERENT FROM
        THE DENORMALIZED PERSPECITVE. THIS PROVIDES THAT PERSPECTIVE FOR QUERIES
        """
        output = [
            {
                "table": concat_field(c.es_index, table),
                "name": name,
                "cardinality": c.cardinality,
                "es_column": c.es_column,
                "es_index": c.es_index,
                "last_updated": c.last_updated,
                "count": c.count,
                "nested_path": c.nested_path,
                "type": c.type
            }
            for tname, css in self.data.items()
            for cname, cs in css.items()
            for c in cs
            for table, name in c.names.items()
        ]
        #+[
        #     {
        #         "table": tname,
        #         "name": "_id",
        #         "nested_path": ["."],
        #         "type": "string"
        #     }
        #     for tname, _ in self.data.items()
        # ]
        if not self.meta_schema:
            self.meta_schema = get_schema_from_list("meta\\.columns", output)

        from pyLibrary.queries.containers.list_usingPythonList import ListContainer
        return ListContainer("meta\\.columns", data=output, schema=self.meta_schema)


def get_schema_from_list(table_name, frum):
    """
    SCAN THE LIST FOR COLUMN TYPES
    """
    columns = UniqueIndex(keys=("names.\\.",))
    _get_schema_from_list(frum, ".", prefix_path=[], nested_path=ROOT_PATH, columns=columns)
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

