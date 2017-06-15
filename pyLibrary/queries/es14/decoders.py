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

from mo_logs import Log
from mo_dots import set_default, coalesce, literal_field, Data, unwraplist
from mo_dots import wrap
from mo_math import MAX, UNION
from mo_math import Math
from pyLibrary.queries import jx
from pyLibrary.queries.containers import STRUCT
from pyLibrary.queries.dimensions import Dimension
from pyLibrary.queries.domains import SimpleSetDomain, DefaultDomain, PARTITION
from pyLibrary.queries.expressions import simplify_esfilter, Variable, NotOp, InOp, Literal, OrOp, AndOp, \
    InequalityOp, TupleOp, LeavesOp
from pyLibrary.queries.query import MAX_LIMIT, DEFAULT_LIMIT


class AggsDecoder(object):
    def __new__(cls, e=None, query=None, *args, **kwargs):
        e.allowNulls = coalesce(e.allowNulls, True)

        if e.value and e.domain.type == "default":
            # if query.groupby:
            #     return object.__new__(DefaultDecoder, e)

            if isinstance(e.value, basestring):
                Log.error("Expecting Variable or Expression, not plain string")

            if isinstance(e.value, LeavesOp):
                return object.__new__(ObjectDecoder, e)
            elif isinstance(e.value, TupleOp):
                # THIS domain IS FROM A dimension THAT IS A SIMPLE LIST OF fields
                # JUST PULL THE FIELDS
                if not all(isinstance(t, Variable) for t in e.value.terms):
                    Log.error("Can only handle variables in tuples")

                e.domain = Data(
                    dimension={"fields":e.value.terms}
                )
                return object.__new__(DimFieldListDecoder, e)
            elif isinstance(e.value, Variable):
                schema = query.frum.schema
                col = schema[e.value.var][0]

                if col.type in STRUCT:
                    return object.__new__(ObjectDecoder, e)
                if not col:
                    return object.__new__(DefaultDecoder, e)
                limit = coalesce(e.domain.limit, query.limit, DEFAULT_LIMIT)

                if col.partitions != None:
                    e.domain = SimpleSetDomain(partitions=col.partitions[:limit:])
                else:
                    e.domain = set_default(DefaultDomain(limit=limit), e.domain.__data__())
                    return object.__new__(DefaultDecoder, e)

            else:
                return object.__new__(DefaultDecoder, e)

        if e.value and e.domain.type in PARTITION:
            return object.__new__(SetDecoder, e)
        if isinstance(e.domain.dimension, Dimension):
            e.domain = e.domain.dimension.getDomain()
            return object.__new__(SetDecoder, e)
        if e.value and e.domain.type == "time":
            return object.__new__(TimeDecoder, e)
        if e.range:
            return object.__new__(GeneralRangeDecoder, e)
        if e.value and e.domain.type == "duration":
            return object.__new__(DurationDecoder, e)
        elif e.value and e.domain.type == "range":
            return object.__new__(RangeDecoder, e)
        elif not e.value and e.domain.dimension.fields:
            # THIS domain IS FROM A dimension THAT IS A SIMPLE LIST OF fields
            # JUST PULL THE FIELDS
            fields = e.domain.dimension.fields
            if isinstance(fields, Mapping):
                Log.error("No longer allowed: All objects are expressions")
            else:
                return object.__new__(DimFieldListDecoder, e)
        elif not e.value and all(e.domain.partitions.where):
            return object.__new__(GeneralSetDecoder, e)
        else:
            Log.error("domain type of {{type}} is not supported yet", type=e.domain.type)


    def __init__(self, edge, query, limit):
        self.start = None
        self.edge = edge
        self.name = literal_field(self.edge.name)
        self.query = query
        self.limit = limit

    def append_query(self, es_query, start):
        Log.error("Not supported")

    def count(self, row):
        pass

    def done_count(self):
        pass

    def get_value_from_row(self, row):
        Log.error("Not implemented")

    def get_value(self, index):
        Log.error("Not implemented")

    def get_index(self, row):
        Log.error("Not implemented")

    @property
    def num_columns(self):
        return 0


class SetDecoder(AggsDecoder):

    def __init__(self, edge, query, limit):
        AggsDecoder.__init__(self, edge, query, limit)
        self.domain = edge.domain

        # WE ASSUME IF THE VARIABLES MATCH, THEN THE SORT TERM AND EDGE TERM MATCH, AND WE SORT BY TERM
        self.sorted = None
        edge_var = edge.value.vars()
        for s in query.sort:
            if not edge_var - s.value.vars():
                self.sorted = {1: "asc", -1: "desc"}[s.sort]
                domain = self.domain
                key = self.domain.key
                domain.partitions = parts = jx.sort(domain.partitions, {"value": key, "sort": s.sort})
                domain.map = {i: p for i, p in enumerate(parts)}

    def append_query(self, es_query, start):
        self.start = start
        domain = self.domain
        field = self.edge.value

        if isinstance(field, Variable):
            key = domain.key
            if isinstance(key, (tuple, list)) and len(key)==1:
                key = key[0]
            include = [p[key] for p in domain.partitions]

            if self.edge.allowNulls:

                return wrap({"aggs": {
                    "_match": set_default({"terms": {
                        "field": field.var,
                        "size": self.limit,
                        "include": include,
                        "order": {"_term": self.sorted} if self.sorted else None
                    }}, es_query),
                    "_missing": set_default(
                        {"filter": {"or": [
                            field.missing().to_esfilter(),
                            {"not": {"terms": {field.var: include}}}
                        ]}},
                        es_query
                    ),
                }})
            else:
                return wrap({"aggs": {
                    "_match": set_default({"terms": {
                        "field": field.var,
                        "size": self.limit,
                        "include": include,
                        "order": {"_term": self.sorted} if self.sorted else None
                    }}, es_query)
                }})
        else:
            include = [p[domain.key] for p in domain.partitions]
            if self.edge.allowNulls:

                return wrap({"aggs": {
                    "_match": set_default({"terms": {
                        "script_field": field.to_ruby(),
                        "size": self.limit,
                        "include": include
                    }}, es_query),
                    "_missing": set_default(
                        {"filter": {"or": [
                            field.missing().to_esfilter(),
                            NotOp("not", InOp("in", [field, Literal("literal", include)])).to_esfilter()
                        ]}},
                        es_query
                    ),
                }})
            else:
                return wrap({"aggs": {
                    "_match": set_default({"terms": {
                        "script_field": field.to_ruby(),
                        "size": self.limit,
                        "include": include
                    }}, es_query)
                }})

    def get_value(self, index):
        return self.domain.getKeyByIndex(index)

    def get_value_from_row(self, row):
        return row[self.start]["key"]

    def get_index(self, row):
        try:
            part = row[self.start]
            return self.domain.getIndexByKey(part["key"])
        except Exception as e:
            Log.error("problem", cause=e)

    @property
    def num_columns(self):
        return 1


def _range_composer(edge, domain, es_query, to_float):
    # USE RANGES
    _min = coalesce(domain.min, MAX(domain.partitions.min))
    _max = coalesce(domain.max, MAX(domain.partitions.max))

    if isinstance(edge.value, Variable):
        calc = {"field": edge.value.var}
    else:
        calc = {"script_field": edge.value.to_ruby()}

    if edge.allowNulls:    # TODO: Use Expression.missing().esfilter() TO GET OPTIMIZED FILTER
        missing_filter = set_default(
            {"filter": {"or": [
                OrOp("or", [
                    InequalityOp("lt", [edge.value, Literal(None, to_float(_min))]),
                    InequalityOp("gte", [edge.value, Literal(None, to_float(_max))]),
                ]).to_esfilter(),
                edge.value.missing().to_esfilter()
            ]}},
            es_query
        )
    else:
        missing_filter = None

    return wrap({"aggs": {
        "_match": set_default(
            {"range": calc},
            {"range": {"ranges": [{"from": to_float(p.min), "to": to_float(p.max)} for p in domain.partitions]}},
            es_query
        ),
        "_missing": missing_filter
    }})


class TimeDecoder(AggsDecoder):
    def append_query(self, es_query, start):
        self.start = start
        return _range_composer(self.edge, self.edge.domain, es_query, lambda x: x.unix)

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)

        f = coalesce(part["from"], part["key"])
        t = coalesce(part["to"], part["key"])
        if f == None or t == None:
            return len(domain.partitions)
        else:
            for p in domain.partitions:
                if p.min.unix <= f <p.max.unix:
                    return p.dataIndex
        sample = part.copy
        sample.buckets = None
        Log.error("Expecting to find {{part}}",  part=sample)

    @property
    def num_columns(self):
        return 1


class GeneralRangeDecoder(AggsDecoder):
    """
    Accept an algebraic domain, and an edge with a `range` attribute
    This class assumes the `snapshot` version - where we only include
    partitions that have their `min` value in the range.
    """

    def __init__(self, edge, query, limit):
        AggsDecoder.__init__(self, edge, query, limit)
        if edge.domain.type=="time":
            self.to_float = lambda x: x.unix
        elif edge.domain.type=="range":
            self.to_float = lambda x: x
        else:
            Log.error("Unknown domain of type {{type}} for range edge", type=edge.domain.type)

    def append_query(self, es_query, start):
        self.start = start

        edge = self.edge
        range = edge.range
        domain = edge.domain

        aggs = {}
        for i, p in enumerate(domain.partitions):
            filter_ = AndOp("and", [
                InequalityOp("lte", [range.min, Literal("literal", self.to_float(p.min))]),
                InequalityOp("gt", [range.max, Literal("literal", self.to_float(p.min))])
            ])
            aggs["_join_" + unicode(i)] = set_default(
                {"filter": filter_.to_esfilter()},
                es_query
            )

        return wrap({"aggs": aggs})

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)
        return part["_index"]

    @property
    def num_columns(self):
        return 1


class GeneralSetDecoder(AggsDecoder):
    """
    EXPECTING ALL PARTS IN partitions TO HAVE A where CLAUSE
    """

    def append_query(self, es_query, start):
        self.start = start

        parts = self.edge.domain.partitions
        filters = []
        notty = []

        for p in parts:
            filters.append(AndOp("and", [p.where]+notty).to_esfilter())
            notty.append(NotOp("not", p.where))

        missing_filter = None
        if self.edge.allowNulls:    # TODO: Use Expression.missing().esfilter() TO GET OPTIMIZED FILTER
            missing_filter = set_default(
                {"filter": AndOp("and", notty).to_esfilter()},
                es_query
            )

        return wrap({"aggs": {
            "_match": set_default(
                {"filters": {"filters": filters}},
                es_query
            ),
            "_missing": missing_filter
        }})

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)
        return part["_index"]

    @property
    def num_columns(self):
        return 1


class DurationDecoder(AggsDecoder):
    def append_query(self, es_query, start):
        self.start = start
        return _range_composer(self.edge, self.edge.domain, es_query, lambda x: x.seconds)

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)

        f = coalesce(part["from"], part["key"])
        t = coalesce(part["to"], part["key"])
        if f == None or t == None:
            return len(domain.partitions)
        else:
            for p in domain.partitions:
                if p.min.seconds <= f < p.max.seconds:
                    return p.dataIndex
        sample = part.copy
        sample.buckets = None
        Log.error("Expecting to find {{part}}",  part=sample)

    @property
    def num_columns(self):
        return 1


class RangeDecoder(AggsDecoder):
    def append_query(self, es_query, start):
        self.start = start
        return _range_composer(self.edge, self.edge.domain, es_query, lambda x: x)

    def get_value(self, index):
        return self.edge.domain.getKeyByIndex(index)

    def get_index(self, row):
        domain = self.edge.domain
        part = row[self.start]
        if part == None:
            return len(domain.partitions)

        f = coalesce(part["from"], part["key"])
        t = coalesce(part["to"], part["key"])
        if f == None or t == None:
            return len(domain.partitions)
        else:
            for p in domain.partitions:
                if p.min <= f <p.max:
                    return p.dataIndex
        sample = part.copy
        sample.buckets = None
        Log.error("Expecting to find {{part}}",  part=sample)

    @property
    def num_columns(self):
        return 1


class DefaultDecoder(SetDecoder):
    # FOR DECODING THE default DOMAIN TYPE (UNKNOWN-AT-QUERY-TIME SET OF VALUES)

    def __init__(self, edge, query, limit):
        AggsDecoder.__init__(self, edge, query, limit)
        self.domain = edge.domain
        self.domain.limit =Math.min(coalesce(self.domain.limit, query.limit, 10), MAX_LIMIT)
        self.parts = list()
        self.key2index = {}
        self.computed_domain = False

        # WE ASSUME IF THE VARIABLES MATCH, THEN THE SORT TERM AND EDGE TERM MATCH, AND WE SORT BY TERM
        self.sorted = None
        edge_var = edge.value.vars()
        for s in query.sort:
            if not edge_var - s.value.vars():
                self.sorted = {1: "asc", -1: "desc"}[s.sort]

    def append_query(self, es_query, start):
        self.start = start

        if not isinstance(self.edge.value, Variable):
            script_field = self.edge.value.to_ruby()
            missing = self.edge.value.missing()

            output = wrap({"aggs": {
                "_match": set_default(
                    {"terms": {
                        "script_field": script_field,
                        "size": self.domain.limit,
                        "order": {"_term": self.sorted} if self.sorted else None
                    }},
                    es_query
                ),
                "_missing": set_default({"filter": missing.to_esfilter()}, es_query) if missing else None
            }})
            return output
        elif self.edge.value.var in [s.value.var for s in self.query.sort]:
            sort_dir = [s.sort for s in self.query.sort if s.value.var==self.edge.value.var][0]
            output = wrap({"aggs": {
                "_match": set_default(
                    {"terms": {
                        "field": self.edge.value.var,
                        "size": self.domain.limit,
                        "order": {"_term": "asc" if sort_dir == 1 else "desc"}
                    }},
                    es_query
                ),
                "_missing": set_default({"missing": {"field": self.edge.value}}, es_query)  # TODO: Use Expression.missing().esfilter() TO GET OPTIMIZED FILTER
            }})
            return output
        else:
            output = wrap({"aggs": {
                "_match": set_default(
                    {"terms": {
                        "field": self.edge.value.var,
                        "size": self.domain.limit
                    }},
                    es_query
                ),
                "_missing": set_default({"missing": {"field": self.edge.value}}, es_query)  # TODO: Use Expression.missing().esfilter() TO GET OPTIMIZED FILTER
            }})
            return output

    def count(self, row):
        part = row[self.start]
        if part == None:
            self.edge.allowNulls = True  # OK! WE WILL ALLOW NULLS
        else:
            self.parts.append(part["key"])

    def done_count(self):
        self.edge.domain = self.domain = SimpleSetDomain(
            partitions=jx.sort(set(self.parts))
        )
        self.parts = None
        self.computed_domain = True

    def get_index(self, row):
        if self.computed_domain:
            try:
                part = row[self.start]
                return self.domain.getIndexByKey(part["key"])
            except Exception as e:
                Log.error("problem", cause=e)
        else:
            try:
                part = row[self.start]
                key = part['key']
                i = self.key2index.get(key)
                if i is None:
                    i = len(self.parts)
                    part = {"key": key, "dataIndex": i}
                    self.parts.append({"key": key, "dataIndex": i})
                    self.key2index[i] = part
                return i
            except Exception as e:
                Log.error("problem", cause=e)

    @property
    def num_columns(self):
        return 1


class DimFieldListDecoder(SetDecoder):
    def __init__(self, edge, query, limit):
        AggsDecoder.__init__(self, edge, query, limit)
        self.fields = edge.domain.dimension.fields
        self.domain = self.edge.domain
        self.domain.limit =Math.min(coalesce(self.domain.limit, query.limit, 10), MAX_LIMIT)
        self.parts = list()


    def append_query(self, es_query, start):
        #TODO: USE "reverse_nested" QUERY TO PULL THESE

        self.start = start
        for i, v in enumerate(self.fields):
            nest = wrap({"aggs": {
                "_match": set_default({"terms": {
                    "field": v,
                    "size": self.domain.limit
                }}, es_query)
            }})
            if self.edge.allowNulls:
                nest.aggs._missing = set_default({"missing": {"field": v}}, es_query)  # TODO: Use Expression.missing().esfilter() TO GET OPTIMIZED FILTER
            es_query = nest

        if self.domain.where:
            filter = simplify_esfilter(self.domain.where)
            es_query = {"aggs": {"_filter": set_default({"filter": filter}, es_query)}}

        return es_query

    def count(self, row):
        part = row[self.start:self.start + len(self.fields):]
        value = tuple(p["key"] for p in part)
        self.parts.append(value)

    def done_count(self):
        columns = map(unicode, range(len(self.fields)))
        parts = wrap([{unicode(i): p for i, p in enumerate(part)} for part in set(self.parts)])
        self.parts = None
        sorted_parts = jx.sort(parts, columns)

        self.edge.domain = self.domain = SimpleSetDomain(
            key="value",
            partitions=[{"value": tuple(v[k] for k in columns), "dataIndex": i} for i, v in enumerate(sorted_parts)]
        )

    def get_index(self, row):
        find = tuple(p["key"] for p in row[self.start:self.start + self.num_columns:])
        return self.domain.getIndexByKey(find)

    @property
    def num_columns(self):
        return len(self.fields)


class ObjectDecoder(SetDecoder):
    def __init__(self, edge, query, limit):
        AggsDecoder.__init__(self, edge, query, limit)
        if isinstance(edge.value, LeavesOp):
            flatter = literal_field
            prefix = edge.value.term.var+"."
        else:
            prefix = edge.value.var + "."
            prefix_length = len(prefix)
            flatter = (lambda k: k[prefix_length:])

        self.put, self.fields = zip(*[
            (flatter(k), c.es_column)
            for k, cs in query.frum.schema.lookup.items()
            if k.startswith(prefix)
            for c in cs
            if c.type not in STRUCT
        ])

        self.domain = self.edge.domain = wrap({"dimension": {"fields": self.fields}})
        self.domain.limit =Math.min(coalesce(self.domain.limit, query.limit, 10), MAX_LIMIT)
        self.parts = list()
        self.key2index = {}
        self.computed_domain = False

    def append_query(self, es_query, start):
        self.start = start
        for i, v in enumerate(self.fields):
            # output = wrap({"aggs": {
            #     "_match": set_default(
            #         {"terms": {
            #             "script_field": script_field,
            #             "size": self.domain.limit,
            #             "order": {"_term": self.sorted} if self.sorted else None
            #         }},
            #         es_query
            #     ),
            #     "_missing": set_default({"filter": missing.to_esfilter()}, es_query) if missing else None
            # }})

            nest = wrap({"aggs": {
                "_match": set_default({"terms": {
                    "field": v,
                    "size": self.domain.limit
                }}, es_query),
                "_missing": set_default({"missing": {"field": v}}, es_query)
            }})
            es_query = nest
        return es_query

    def count(self, row):
        value = self.get_value_from_row(row)
        i = self.key2index.get(value)
        if i is None:
            i = self.key2index[value] = len(self.parts)
            self.parts.append(value)

    def done_count(self):
        self.computed_domain = True
        self.edge.domain = self.domain = SimpleSetDomain(
            key="value",
            partitions=[{"value": p, "dataIndex": i} for i, p in enumerate(self.parts)]
        )

    def get_index(self, row):
        value=self.get_value_from_row(row)
        if self.computed_domain:
            return self.domain.getIndexByKey(value)

        i = self.key2index.get(value)
        if i is None:
            i = self.key2index[value] = len(self.parts)
            self.parts.append(value)
        return i

    def get_value_from_row(self, row):
        output = Data()
        for k, v in zip(self.put, row[self.start:self.start + self.num_columns:]):
            output[k] = v["key"]
        return output

    @property
    def num_columns(self):
        return len(self.fields)
