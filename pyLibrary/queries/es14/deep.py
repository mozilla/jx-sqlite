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

from mo_dots import split_field, FlatList, listwrap, literal_field, coalesce, Data, unwrap, concat_field, relative_field, \
    unliteral_field, join_field
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
    schema = query.frum.schema
    columns = schema.columns
    query_path = schema.query_path

    map_to_local = {k: get_pull(c[0]) for k, c in schema.lookup.items()}

    # TODO: FIX THE GREAT SADNESS CAUSED BY EXECUTING post_expressions
    # THE EXPRESSIONS SHOULD BE PUSHED TO THE CONTAINER:  ES ALLOWS
    # {"inner_hit":{"script_fields":[{"script":""}...]}}, BUT THEN YOU
    # LOOSE "_source" BUT GAIN "fields", FORCING ALL FIELDS TO BE EXPLICIT
    post_expressions = {}
    es_query, es_filters = es14.util.es_query_template(query.frum.name)

    # SPLIT WHERE CLAUSE BY DEPTH
    wheres = split_expression_by_depth(query.where, schema)
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

    i = 0
    for s in listwrap(query.select):
        if isinstance(s.value, LeavesOp):
            if isinstance(s.value.term, Variable):
                if s.value.term.var == ".":
                    # IF THERE IS A *, THEN INSERT THE EXTRA COLUMNS
                    for c in columns:
                        if c.type not in STRUCT and c.es_column != "_id":
                            if c.nested_path[0] == ".":
                                es_query.fields += [c.es_column]
                            new_select.append({
                                "name": c.names[query_path],
                                "pull": get_pull(c),
                                "nested_path": c.nested_path[0],
                                "put": {"name": literal_field(c.names[query_path]), "index": i, "child": "."}
                            })
                            i += 1

                    # REMOVE DOTS IN PREFIX IF NAME NOT AMBIGUOUS
                    col_names = set(c.names[query_path] for c in columns)
                    for n in new_select:
                        if n.name.startswith("..") and n.name.lstrip(".") not in col_names:
                            n.name = n.name.lstrip(".")
                            n.put.name = literal_field(n.name)
                            col_names.add(n.name)
                else:
                    prefix = schema[s.value.term.var][0].names["."] + "."
                    prefix_length = len(prefix)
                    for c in columns:
                        cname = c.names["."]
                        if cname.startswith(prefix) and c.type not in STRUCT:
                            pull = get_pull(c)
                            if c.nested_path[0] == ".":
                                es_query.fields += [c.es_column]

                            new_select.append({
                                "name": s.name + "." + cname[prefix_length:],
                                "pull": pull,
                                "nested_path": c.nested_path[0],
                                "put": {
                                    "name": s.name + "." + literal_field(cname[prefix_length:]),
                                    "index": i,
                                    "child": "."
                                }
                            })
                            i += 1
        elif isinstance(s.value, Variable):
            if s.value.var == ".":
                for c in columns:
                    if c.type not in STRUCT and c.es_column != "_id":
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
                prefix = schema[s.value.var][0]
                if not prefix:
                    net_columns = []
                else:
                    parent = prefix.es_column+"."
                    prefix_length = len(parent)
                    net_columns = [c for c in columns if c.es_column.startswith(parent) and c.type not in STRUCT]

                if not net_columns:
                    pull = get_pull(prefix)
                    if len(prefix.nested_path) == 1:
                        es_query.fields += [prefix.es_column]
                    new_select.append({
                        "name": s.name,
                        "pull": pull,
                        "nested_path": prefix.nested_path[0],
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
                            "put": {"name": s.name, "index": i, "child": n.es_column[prefix_length:]}
                        })
                i += 1
        else:
            expr = s.value
            for v in expr.vars():
                for c in schema[v]:
                    if c.nested_path[0] == ".":
                        es_query.fields += [c.es_column]
                    # else:
                    #     Log.error("deep field not expected")

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
    except Exception as e:
        Log.error("problem formatting", e)


def get_pull(column):
    if column.nested_path[0] == ".":
        return concat_field("fields", literal_field(column.es_column))
    else:
        depth = len(split_field(column.nested_path[0]))
        rel_name = split_field(column.es_column)[depth:]
        return join_field(["_inner"] + rel_name)
