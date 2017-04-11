# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import

from mo_collections.matrix import Matrix
from mo_math import AND
from mo_dots import listwrap, unwrap, literal_field
from jx_base.queries import is_variable_name
from pyLibrary.queries.es09.util import aggregates, fix_es_stats, build_es_query
from pyLibrary.queries import es09
from pyLibrary.queries.containers.cube import Cube
from pyLibrary.queries.expressions import simplify_esfilter, Variable, jx_expression_to_function


def is_aggop(query):
    if not query.edges:
        return True
    return False


def es_aggop(es, mvel, query):
    select = listwrap(query.select)
    FromES = build_es_query(query)

    isSimple = AND(aggregates[s.aggregate] == "count" for s in select)
    if isSimple:
        return es_countop(es, query)  # SIMPLE, USE TERMS FACET INSTEAD


    value2facet = dict()  # ONLY ONE FACET NEEDED PER
    name2facet = dict()   # MAP name TO FACET WITH STATS

    for s in select:
        if s.value not in value2facet:
            if isinstance(s.value, Variable):
                unwrap(FromES.facets)[s.name] = {
                    "statistical": {
                        "field": s.value.var
                    },
                    "facet_filter": simplify_esfilter(query.where.to_esfilter())
                }
            else:
                unwrap(FromES.facets)[s.name] = {
                    "statistical": {
                        "script": jx_expression_to_function(s.value)
                    },
                    "facet_filter": simplify_esfilter(query.where)
                }
            value2facet[s.value] = s.name
        name2facet[s.name] = value2facet[s.value]

    data = es09.util.post(es, FromES, query.limit)

    matricies = {s.name: Matrix(value=fix_es_stats(data.facets[literal_field(s.name)])[aggregates[s.aggregate]]) for s in select}
    cube = Cube(query.select, [], matricies)
    cube.frum = query
    return cube



def es_countop(es, mvel, query):
    """
    RETURN SINGLE COUNT
    """
    select = listwrap(query.select)
    FromES = build_es_query(query)
    for s in select:

        if is_variable_name(s.value):
            FromES.facets[s.name] = {
                "terms": {
                    "field": s.value,
                    "size": query.limit,
                },
                "facet_filter":{"exists":{"field":s.value}}
            }
        else:
            # COMPLICATED value IS PROBABLY A SCRIPT, USE IT
            FromES.facets[s.name] = {
                "terms": {
                    "script_field": es09.expressions.compile_expression(s.value, query),
                    "size": 200000
                }
            }

    data = es09.util.post(es, FromES, query.limit)

    matricies = {}
    for s in select:
        matricies[s.name] = Matrix(value=data.hits.facets[s.name].total)

    cube = Cube(query.select, query.edges, matricies)
    cube.frum = query
    return cube
