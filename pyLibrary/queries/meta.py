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

from mo_logs import Log
from mo_threads import Lock, THREAD_STOP
from mo_threads import Queue
from mo_threads import Thread
from mo_threads import Till
from mo_times.dates import Date
from mo_times.durations import HOUR, MINUTE
from mo_times.timer import Timer
from mo_dots import Data
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
        self.meta.tables = ListContainer("meta.tables", [], wrap({c.names["meta.tables"]: c for c in table_columns}))
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
        existing_columns = self.meta.columns.find(c.table, c.name)
        if not existing_columns:
            self.meta.columns.add(c)
            self.todo.add(c)

            if ENABLE_META_SCAN:
                if DEBUG:
                    Log.note("todo: {{table}}::{{column}}", table=c.table, column=c.es_column)
                # MARK meta.columns AS DIRTY TOO
                cols = self.meta.columns.find("meta.columns", None)
                for cc in cols:
                    cc.partitions = cc.cardinality = None
                    cc.last_updated = Date.now()
                self.todo.extend(cols)
        else:
            canonical = existing_columns[0]
            if canonical.relative and not c.relative:
                return  # RELATIVE COLUMNS WILL SHADOW ABSOLUTE COLUMNS

            for key in Column.__slots__:
                canonical[key] = c[key]
            if DEBUG:
                Log.note("todo: {{table}}::{{column}}", table=canonical.table, column=canonical.es_column)
            self.todo.add(canonical)

    def _get_columns(self, table=None):
        # TODO: HANDLE MORE THEN ONE ES, MAP TABLE SHORT_NAME TO ES INSTANCE
        meta = self.es_metadata.indices[table]
        if not meta or self.last_es_metadata < Date.now() - OLD_METADATA:
            self.es_metadata = self.default_es.get_metadata(force=True)
            meta = self.es_metadata.indices[table]
        self._parse_properties(meta.index, Data(properties={"_id": {"type": "string", "index": "not_analyzed"}}), meta)
        for _, properties in meta.mappings.items():
            self._parse_properties(meta.index, properties, meta)

    def _parse_properties(self, abs_index, properties, meta):
        abs_columns = _elasticsearch.parse_properties(abs_index, None, properties.properties)
        abs_columns = abs_columns.filter(  # TODO: REMOVE WHEN jobs PROPERTY EXPLOSION IS CONTAINED
            lambda r: not r.es_column.startswith("other.") and
                      not r.es_column.startswith("previous_values.cf_") and
                      not r.es_index.startswith("debug") and
                      r.es_column.find("=")==-1 and
                      r.es_column.find(" ")==-1
        )
        with Timer("upserting {{num}} columns", {"num": len(abs_columns)}, debug=DEBUG):
            def add_column(c, query_path):
                c.last_updated = Date.now()
                c.table = join_field([c.es_index]+split_field(query_path[0]))

                with self.meta.columns.locker:
                    self._upsert_column(c)
                    for alias in meta.aliases:
                        c = copy(c)
                        c.table = join_field([alias]+split_field(query_path[0]))
                        self._upsert_column(c)

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
                full_path = abs_column.nested_path
                abs_depth = len(full_path)-1
                abs_parent = full_path[1] if abs_depth else ""

                for query_path in query_paths:
                    rel_depth = len(query_path)-1
                    rel_parent = query_path[0]
                    rel_column = copy(abs_column)
                    rel_column.relative = True

                    add_column(copy(abs_column), query_path)

                    if rel_parent == ".":
                        add_column(rel_column, query_path)
                    elif abs_column.es_column.startswith(rel_parent+"."):
                        rel_column.name = abs_column.es_column[len(rel_parent)+1:]
                        add_column(rel_column, query_path)
                    elif abs_column.es_column == rel_parent:
                        rel_column.name = "."
                        add_column(rel_column, query_path)
                    elif not abs_parent:
                        # THIS RELATIVE NAME (..o) ALSO NEEDS A RELATIVE NAME (o)
                        # AND THEN REMOVE THE SHADOWED
                        rel_column.name = "." + ("." * (rel_depth - abs_depth)) + abs_column.es_column
                        add_column(rel_column, query_path)
                    elif rel_parent.startswith(abs_parent+"."):
                        rel_column.name = "." + ("." * (rel_depth - abs_depth)) + abs_column.es_column
                        add_column(rel_column, query_path)
                    elif rel_parent != abs_parent:
                        # SIBLING NESTED PATHS ARE INVISIBLE
                        pass
                    else:
                        Log.error("logic error")

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
        try:
            # LAST TIME WE GOT INFO FOR THIS TABLE
            short_name = join_field(split_field(table_name)[0:1])
            table = self.get_table(short_name)[0]

            if not table:
                table = Table(
                    name=short_name,
                    url=None,
                    query_path=None,
                    timestamp=Date.now()
                )
                with self.meta.tables.locker:
                    self.meta.tables.add(table)
                self._get_columns(table=short_name)
            elif force or table.timestamp == None or table.timestamp < Date.now() - MAX_COLUMN_METADATA_AGE:
                table.timestamp = Date.now()
                self._get_columns(table=short_name)

            with self.meta.columns.locker:
                columns = self.meta.columns.find(table_name, column_name)
            if columns:
                columns = jx.sort(columns, "name")
                # AT LEAST WAIT FOR THE COLUMNS TO UPDATE
                while len(self.todo) and not all(columns.get("last_updated")):
                    if DEBUG:
                        Log.note("waiting for columns to update {{columns|json}}", columns=[c.table+"."+c.es_column for c in columns if not c.last_updated])
                    Till(seconds=1).wait()
                return columns
        except Exception as e:
            Log.error("Not expected", cause=e)

        if column_name:
            Log.error("no columns matching {{table}}.{{column}}", table=table_name, column=column_name)
        else:
            self._get_columns(table=table_name)
            Log.error("no columns for {{table}}?!", table=table_name)

    def _update_cardinality(self, c):
        """
        QUERY ES TO FIND CARDINALITY AND PARTITIONS FOR A SIMPLE COLUMN
        """
        if c.type in STRUCT:
            Log.error("not supported")
        try:
            if c.table == "meta.columns":
                with self.meta.columns.locker:
                    partitions = jx.sort([g[c.es_column] for g, _ in jx.groupby(self.meta.columns, c.es_column) if g[c.es_column] != None])
                    self.meta.columns.update({
                        "set": {
                            "partitions": partitions,
                            "count": len(self.meta.columns),
                            "cardinality": len(partitions),
                            "last_updated": Date.now()
                        },
                        "where": {"eq": {"table": c.table, "es_column": c.es_column}}
                    })
                return
            if c.table == "meta.tables":
                with self.meta.columns.locker:
                    partitions = jx.sort([g[c.es_column] for g, _ in jx.groupby(self.meta.tables, c.es_column) if g[c.es_column] != None])
                    self.meta.columns.update({
                        "set": {
                            "partitions": partitions,
                            "count": len(self.meta.tables),
                            "cardinality": len(partitions),
                            "last_updated": Date.now()
                        },
                        "where": {"eq": {"table": c.table, "name": c.name}}
                    })
                return

            es_index = c.table.split(".")[0]
            result = self.default_es.post("/" + es_index + "/_search", data={
                "aggs": {c.name: _counting_query(c)},
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
                    Log.note("{{table}}.{{field}} has {{num}} parts", table=c.table, field=c.es_column, num=cardinality)
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
                query.aggs[literal_field(c.name)] = {
                    "nested": {"path": c.nested_path[0]},
                    "aggs": {"_nested": {"terms": {"field": c.es_column, "size": 0}}}
                }
            else:
                query.aggs[literal_field(c.name)] = {"terms": {"field": c.es_column, "size": 0}}

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
            if "IndexMissingException" in e and c.table.startswith(TEST_TABLE_PREFIX):
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
                    "where": {"eq": {"names.meta\.columns": c.table, "es_column": c.es_column}}
                })
                Log.warning("Could not get {{col.table}}.{{col.es_column}} info", col=c, cause=e)

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
                                if c.es_column == d.es_column and c.table == d.table and c != d:
                                    Log.error("")
                        else:
                            if DEBUG:
                                Log.note("no more metatdata to update")

                column = self.todo.pop(Till(seconds=(10*MINUTE).seconds))
                if column:
                    if DEBUG:
                        Log.note("update {{table}}.{{column}}", table=column.table, column=column.es_column)
                    if column.type in STRUCT:
                        with self.meta.columns.locker:
                            column.last_updated = Date.now()
                        continue
                    elif column.last_updated >= Date.now()-TOO_OLD:
                        continue
                    try:
                        self._update_cardinality(column)
                        if DEBUG and not column.table.startswith(TEST_TABLE_PREFIX):
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
                names={"meta.columns":c},
                es_index=None,
                es_column=c,
                type="string",
                nested_path=ROOT_PATH
            )
            for c in [
                "name",
                "type",
                "nested_path",
                "relative",
                "es_column",
                "table"
            ]
        ] + [
            Column(
                es_index=None,
                names={"meta.columns":c},
                es_column=c,
                type="object",
                nested_path=ROOT_PATH
            )
            for c in [
                "domain",
                "partitions"
            ]
        ] + [
            Column(
                names={"meta.columns": c},
                es_index=None,
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
                names={"meta.columns": "last_updated"},
                es_index=None,
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
                names={"meta.tables": c},
                es_index=None,
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
                names={"meta.tables": "timestamp"},
                es_index=None,
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
        self.data = {}  # MAP FROM TABLE NAME TO COLUMNS
        self.locker = Lock()
        self.count=0

    def find(self, table, column):
        if not column:
            return [c for cs in self.data.get(table, {}).values() for c in cs]
        else:
            return self.data.get(table, {}).get(column, [])

    def insert(self, columns):
        for column in columns:
            self.add(column)

    def add(self, column):
        if len(column.names)!=1:
            Log.error("not expected")
        table, cname = column.names.items()[0]

        columns_for_table = self.data.setdefault(table, {})
        _columns = columns_for_table.setdefault(cname, [])
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
            if eq.table:
                columns = self.find(eq.table, eq.name)
                columns = [c for c in columns if all(c[k] == v for k, v in eq.items())]
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
