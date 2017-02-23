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

from mo_dots import split_field, FlatList, listwrap, literal_field, coalesce, Data, unwrap
from mo_logs import Log
from mo_threads import Thread
from mo_times.timer import Timer
from pyLibrary import queries, convert
from mo_collections.unique_index import UniqueIndex
from pyLibrary.queries import es09, es14
from pyLibrary.queries.containers import STRUCT
from pyLibrary.queries.es14.setop import format_dispatch
from pyLibrary.queries.es14.util import jx_sort_to_es_sort
from pyLibrary.queries.expressions import split_expression_by_depth, simplify_esfilter, AndOp, compile_expression, \
    Variable, LeavesOp

EXPRESSION_PREFIX = "_expr."

_ = convert


def is_deepop(es, query):
    if query.edges or query.groupby:
        return False
    if all(s.aggregate not in (None, "none") for s in listwrap(query.select)):
        return False
    if len(split_field(query.frum.name)) > 1:
        return True

    # ASSUME IT IS NESTED IF WE ARE ASKING FOR NESTED COLUMNS
    # vars_ = query_get_all_vars(query)
    # columns = query.frum.get_columns()
    # if any(c for c in columns if len(c.nested_path) != 1 and c.name in vars_):
    #    return True
    return False


def es_deepop(es, query):
    columns = query.frum.get_columns(query.frum.name)
    query_path = query.frum.query_path
    columns = UniqueIndex(keys=["name"], data=sorted(columns, lambda a, b: cmp(len(b.nested_path), len(a.nested_path))), fail_on_dup=False)
    map_to_es_columns = {c.name: c.es_column for c in columns}
    map_to_local = {
        c.name: "_inner" + c.es_column[len(c.nested_path[0]):] if len(c.nested_path) != 1 else "fields." + literal_field(c.es_column)
        for c in columns
    }
    # TODO: FIX THE GREAT SADNESS CAUSED BY EXECUTING post_expressions
    # THE EXPRESSIONS SHOULD BE PUSHED TO THE CONTAINER:  ES ALLOWS
    # {"inner_hit":{"script_fields":[{"script":""}...]}}, BUT THEN YOU
    # LOOSE "_source" BUT GAIN "fields", FORCING ALL FIELDS TO BE EXPLICIT
    post_expressions = {}
    es_query, es_filters = es14.util.es_query_template(query.frum.name)

    # SPLIT WHERE CLAUSE BY DEPTH
    wheres = split_expression_by_depth(query.where, query.frum.schema, map_to_es_columns)
    for i, f in enumerate(es_filters):
        # PROBLEM IS {"match_all": {}} DOES NOT SURVIVE set_default()
        for k, v in unwrap(simplify_esfilter(AndOp("and", wheres[i]).to_esfilter())).items():
            f[k] = v


    if not wheres[1]:
        more_filter = {
            "and": [
                simplify_esfilter(AndOp("and", wheres[0]).to_esfilter()),
                {"not": {
                    "nested": {
                        "path": query_path,
                        "filter": {
                            "match_all": {}
                        }
                    }
                }}
            ]
        }
    else:
        more_filter = None

    es_query.size = coalesce(query.limit, queries.query.DEFAULT_LIMIT)
    es_query.sort = jx_sort_to_es_sort(query.sort)
    es_query.fields = []

    is_list = isinstance(query.select, list)
    new_select = FlatList()

    def get_pull(column):
        if len(column.nested_path) != 1:
            return "_inner" + column.es_column[len(column.nested_path[0]):]
        else:
            return "fields." + literal_field(column.es_column)

    i = 0
    for s in listwrap(query.select):
        if isinstance(s.value, LeavesOp):
            if isinstance(s.value.term, Variable):
                if s.value.term.var==".":
                    # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
                    for c in columns:
                        if c.relative and c.type not in STRUCT:
                            if len(c.nested_path) == 1:
                                es_query.fields += [c.es_column]
                            new_select.append({
                                "name": c.name,
                                "pull": get_pull(c),
                                "nested_path": c.nested_path[0],
                                "put": {"name": literal_field(c.name), "index": i, "child": "."}
                            })
                            i += 1

                    # REMOVE DOTS IN PREFIX IF NAME NOT AMBIGUOUS
                    col_names = [c.name for c in columns if c.relative]
                    for n in new_select:
                        if n.name.startswith("..") and n.name.lstrip(".") not in col_names:
                            n.name = n.put.name = n.name.lstrip(".")
                else:
                    column = s.value.term.var+"."
                    prefix = len(column)
                    for c in columns:
                        if c.name.startswith(column) and c.type not in STRUCT:
                            pull = get_pull(c)
                            if len(c.nested_path) == 0:
                                es_query.fields += [c.es_column]

                            new_select.append({
                                "name": s.name + "." + c.name[prefix:],
                                "pull": pull,
                                "nested_path": c.nested_path[0],
                                "put": {"name": s.name + "." + literal_field(c.name[prefix:]), "index": i, "child": "."}
                            })
                            i += 1
        elif isinstance(s.value, Variable):
            if s.value.var == ".":
                for c in columns:
                    if c.relative and c.type not in STRUCT:
                        if len(c.nested_path) == 1:
                            es_query.fields += [c.es_column]
                        new_select.append({
                            "name": c.name,
                            "pull": get_pull(c),
                            "nested_path": c.nested_path[0],
                            "put": {"name": ".", "index": i, "child": c.es_column}
                        })
                i += 1
            elif s.value.var == "_id":
                new_select.append({
                    "name": s.name,
                    "value": s.value.var,
                    "pull": "_id",
                    "put": {"name": s.name, "index": i, "child": "."}
                })
                i += 1
            else:
                column = columns[(s.value.var,)]
                parent = column.es_column+"."
                prefix = len(parent)
                net_columns = [c for c in columns if c.es_column.startswith(parent) and c.type not in STRUCT]
                if not net_columns:
                    pull = get_pull(column)
                    if len(column.nested_path) == 1:
                        es_query.fields += [column.es_column]
                    new_select.append({
                        "name": s.name,
                        "pull": pull,
                        "nested_path": column.nested_path[0],
                        "put": {"name": s.name, "index": i, "child": "."}
                    })
                else:
                    done = set()
                    for n in net_columns:
                        # THE COLUMNS CAN HAVE DUPLICATE REFERNCES TO THE SAME ES_COLUMN
                        if n.es_column in done:
                            continue
                        done.add(n.es_column)

                        pull = get_pull(n)
                        if len(n.nested_path) == 1:
                            es_query.fields += [n.es_column]
                        new_select.append({
                            "name": s.name,
                            "pull": pull,
                            "nested_path": n.nested_path[0],
                            "put": {"name": s.name, "index": i, "child": n.es_column[prefix:]}
                        })
                i += 1
        else:
            expr = s.value
            for v in expr.vars():
                for n in columns:
                    if n.name == v:
                        if len(n.nested_path) == 1:
                            es_query.fields += [n.es_column]

            pull = EXPRESSION_PREFIX + s.name
            post_expressions[pull] = compile_expression(expr.map(map_to_local).to_python())

            new_select.append({
                "name": s.name if is_list else ".",
                "pull": pull,
                "value": expr.__data__(),
                "put": {"name": s.name, "index": i, "child": "."}
            })
            i += 1

    # <COMPLICATED> ES needs two calls to get all documents
    more = []
    def get_more(please_stop):
        more.append(es09.util.post(
            es,
            Data(
                filter=more_filter,
                fields=es_query.fields
            ),
            query.limit
        ))
    if more_filter:
        need_more = Thread.run("get more", target=get_more)

    with Timer("call to ES") as call_timer:
        data = es09.util.post(es, es_query, query.limit)

    # EACH A HIT IS RETURNED MULTIPLE TIMES FOR EACH INNER HIT, WITH INNER HIT INCLUDED
    def inners():
        for t in data.hits.hits:
            for i in t.inner_hits[literal_field(query_path)].hits.hits:
                t._inner = i._source
                for k, e in post_expressions.items():
                    t[k] = e(t)
                yield t
        if more_filter:
            Thread.join(need_more)
            for t in more[0].hits.hits:
                yield t
    #</COMPLICATED>

    try:
        formatter, groupby_formatter, mime_type = format_dispatch[query.format]

        output = formatter(inners(), new_select, query)
        output.meta.timing.es = call_timer.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception, e:
        Log.error("problem formatting", e)
