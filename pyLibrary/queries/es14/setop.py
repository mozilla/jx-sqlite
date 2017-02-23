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

from mo_collections.matrix import Matrix
from mo_dots import coalesce, split_field, set_default, Data, unwraplist, literal_field, join_field, unwrap, wrap
from mo_dots import listwrap
from mo_dots.lists import FlatList
from mo_logs import Log
from mo_math import AND
from mo_math import MAX
from mo_times.timer import Timer
from pyLibrary import queries
from pyLibrary.queries import es14, es09
from pyLibrary.queries.containers import STRUCT
from pyLibrary.queries.containers.cube import Cube
from pyLibrary.queries.domains import ALGEBRAIC
from pyLibrary.queries.es14.util import jx_sort_to_es_sort
from pyLibrary.queries.expressions import simplify_esfilter, Variable, LeavesOp
from pyLibrary.queries.query import DEFAULT_LIMIT

format_dispatch = {}


def is_setop(es, query):
    if not any(map(es.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])):
        return False

    select = listwrap(query.select)

    if not query.edges:
        isDeep = len(split_field(query.frum.name)) > 1  # LOOKING INTO NESTED WILL REQUIRE A SCRIPT
        simpleAgg = AND([s.aggregate in ("count", "none") for s in select])  # CONVERTING esfilter DEFINED PARTS WILL REQUIRE SCRIPT

        # NO EDGES IMPLIES SIMPLER QUERIES: EITHER A SET OPERATION, OR RETURN SINGLE AGGREGATE
        if simpleAgg or isDeep:
            return True
    else:
        isSmooth = AND((e.domain.type in ALGEBRAIC and e.domain.interval == "none") for e in query.edges)
        if isSmooth:
            return True

    return False


def es_setop(es, query):
    es_query, filters = es14.util.es_query_template(query.frum.name)
    set_default(filters[0], simplify_esfilter(query.where.to_esfilter()))
    es_query.size = coalesce(query.limit, queries.query.DEFAULT_LIMIT)
    es_query.sort = jx_sort_to_es_sort(query.sort)
    es_query.fields = FlatList()

    return extract_rows(es, es_query, query)


def extract_rows(es, es_query, query):
    is_list = isinstance(query.select, list)
    selects = wrap([s.copy() for s in listwrap(query.select)])
    new_select = FlatList()
    columns = query.frum.get_columns()
    leaf_columns = set(c.name for c in columns if c.type not in STRUCT and (c.nested_path[0] == "." or c.es_column == c.nested_path))
    nested_columns = set(c.name for c in columns if len(c.nested_path) != 1)

    i = 0
    source = "fields"
    for select in selects:
        # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
        if isinstance(select.value, LeavesOp):
            term = select.value.term
            if isinstance(term, Variable):

                if term.var == ".":
                    es_query.fields = None
                    source = "_source"

                    net_columns = leaf_columns - set(selects.name) - {"_id"}
                    for n in net_columns:
                        new_select.append({
                            "name": n,
                            "value": Variable(n),
                            "put": {"name": n, "index": i, "child": "."}
                        })
                        i += 1
                else:
                    parent = term.var + "."
                    prefix = len(parent)
                    for c in leaf_columns:
                        if c.startswith(parent):
                            if es_query.fields is not None:
                                es_query.fields.append(c)

                            new_select.append({
                                "name": select.name + "." + c[prefix:],
                                "value": Variable(c),
                                "put": {"name": select.name + "." + c[prefix:], "index": i, "child": "."}
                            })
                            i += 1

        elif isinstance(select.value, Variable):
            if select.value.var == ".":
                es_query.fields = None
                source = "_source"

                new_select.append({
                    "name": select.name,
                    "value": select.value,
                    "put": {"name": select.name, "index": i, "child": "."}
                })
                i += 1
            elif select.value.var == "_id":
                new_select.append({
                    "name": select.name,
                    "value": select.value,
                    "pull": "_id",
                    "put": {"name": select.name, "index": i, "child": "."}
                })
                i += 1
            elif select.value.var in nested_columns or [c for c in nested_columns if c.startswith(select.value.var+".")]:
                es_query.fields = None
                source = "_source"

                new_select.append({
                    "name": select.name,
                    "value": select.value,
                    "put": {"name": select.name, "index": i, "child": "."}
                })
                i += 1
            else:
                parent = select.value.var + "."
                prefix = len(parent)
                net_columns = [c for c in leaf_columns if c.startswith(parent)]
                if not net_columns:
                    # LEAF
                    if es_query.fields is not None:
                        es_query.fields.append(select.value.var)
                    new_select.append({
                        "name": select.name,
                        "value": select.value,
                        "put": {"name": select.name, "index": i, "child": "."}
                    })
                else:
                    # LEAVES OF OBJECT
                    for n in net_columns:
                        if es_query.fields is not None:
                            es_query.fields.append(n)
                        new_select.append({
                            "name": select.name,
                            "value": Variable(n),
                            "put": {"name": select.name, "index": i, "child": n[prefix:]}
                        })
                i += 1
        else:
            es_query.script_fields[literal_field(select.name)] = {"script": select.value.to_ruby()}
            new_select.append({
                "name": select.name,
                "pull": "fields." + literal_field(select.name),
                "put": {"name": select.name, "index": i, "child": "."}
            })
            i += 1

    for n in new_select:
        if n.pull:
            continue
        if source == "_source":
            n.pull = join_field(["_source"] + split_field(n.value.var))
        elif isinstance(n.value, Variable):
            n.pull = "fields." + literal_field(n.value.var)
        else:
            Log.error("Do not know what to do")

    with Timer("call to ES") as call_timer:
        data = es09.util.post(es, es_query, query.limit)

    T = data.hits.hits

    try:
        formatter, groupby_formatter, mime_type = format_dispatch[query.format]

        output = formatter(T, new_select, query)
        output.meta.timing.es = call_timer.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception, e:
        Log.error("problem formatting", e)


def format_list(T, select, query=None):
    data = []
    if isinstance(query.select, list):
        for row in T:
            r = Data()
            for s in select:
                r[s.put.name][s.put.child] = unwraplist(row[s.pull])
            data.append(r if r else None)
    elif isinstance(query.select.value, LeavesOp):
        for row in T:
            r = Data()
            for s in select:
                r[s.put.name][s.put.child] = unwraplist(row[s.pull])
            data.append(r if r else None)
    else:
        for row in T:
            r = Data()
            for s in select:
                r[s.put.child] = unwraplist(row[s.pull])
            data.append(r if r else None)

    return Data(
        meta={"format": "list"},
        data=data
    )


def format_table(T, select, query=None):
    data = []
    num_columns = (MAX(select.put.index) + 1)
    for row in T:
        r = [None] * num_columns
        for s in select:
            value = unwraplist(row[s.pull])

            if value == None:
                continue

            index, child = s.put.index, s.put.child
            if child == ".":
                r[index] = value
            else:
                if r[index] is None:
                    r[index] = Data()
                r[index][child] = value

        data.append(r)

    header = [None] * num_columns
    for s in select:
        if header[s.put.index]:
            continue
        header[s.put.index] = s.name

    return Data(
        meta={"format": "table"},
        header=header,
        data=data
    )


def format_cube(T, select, query=None):
    table = format_table(T, select, query)

    if len(table.data) == 0:
        return Cube(
            select,
            edges=[{"name": "rownum", "domain": {"type": "rownum", "min": 0, "max": 0, "interval": 1}}],
            data={h: Matrix(list=[]) for i, h in enumerate(table.header)}
        )

    cols = zip(*unwrap(table.data))
    return Cube(
        select,
        edges=[{"name": "rownum", "domain": {"type": "rownum", "min": 0, "max": len(table.data), "interval": 1}}],
        data={h: Matrix(list=cols[i]) for i, h in enumerate(table.header)}
    )


set_default(format_dispatch, {
    None: (format_cube, None, "application/json"),
    "cube": (format_cube, None, "application/json"),
    "table": (format_table, None, "application/json"),
    "list": (format_list, None, "application/json")
})
