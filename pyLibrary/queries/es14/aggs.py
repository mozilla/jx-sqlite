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

from mo_dots import listwrap, Data, wrap, literal_field, set_default, coalesce, Null, split_field, FlatList, unwrap, \
    unwraplist
from mo_logs import Log
from mo_math import Math, MAX
from mo_times.timer import Timer
from pyLibrary.queries import es09
from pyLibrary.queries.es14.decoders import DefaultDecoder, AggsDecoder, ObjectDecoder
from pyLibrary.queries.es14.decoders import DimFieldListDecoder
from pyLibrary.queries.es14.util import aggregates1_4, NON_STATISTICAL_AGGS
from pyLibrary.queries.expressions import simplify_esfilter, split_expression_by_depth, AndOp, Variable, NullOp
from pyLibrary.queries.query import MAX_LIMIT


def is_aggsop(es, query):
    es.cluster.get_metadata()
    if any(map(es.cluster.version.startswith, ["1.4.", "1.5.", "1.6.", "1.7."])) and (query.edges or query.groupby or any(a != None and a != "none" for a in listwrap(query.select).aggregate)):
        return True
    return False


def get_decoders_by_depth(query):
    """
    RETURN A LIST OF DECODER ARRAYS, ONE ARRAY FOR EACH NESTED DEPTH
    """
    schema = query.frum.schema
    output = FlatList()

    if query.sort:
        # REORDER EDGES/GROUPBY TO MATCH THE SORT
        if len(query.edges)>1 and query.format == "cube":
            Log.error("can not use sort clause with edges: add sort clause to each edge")
        ordered_edges = []
        remaining_edges = query.edges+query.groupby
        for s in query.sort:
            if not isinstance(s.value, Variable):
                Log.error("can only sort by terms")
            for e in remaining_edges:
                if e.value.var == s.value.var:
                    ordered_edges.append(e)
                    remaining_edges.remove(e)
                    break
        ordered_edges.extend(remaining_edges)
        query.groupby = wrap(list(reversed(ordered_edges)))
        query.edges = Null

    for edge in wrap(coalesce(query.edges, query.groupby, [])):
        if edge.value != None and not isinstance(edge.value, NullOp):
            edge = edge.copy()
            vars_ = edge.value.vars()
            for v in vars_:
                if not schema[v]:
                    Log.error("{{var}} does not exist in schema", var=v)

            edge.value = edge.value.map({v: schema[v][0].es_column for v in vars_})
        elif edge.range:
            edge = edge.copy()
            min_ = edge.range.min
            max_ = edge.range.max
            vars_ = min_.vars() | max_.vars()

            for v in vars_:
                if not schema[v]:
                    Log.error("{{var}} does not exist in schema", var=v)

            map_ = {v: schema[v][0].es_column for v in vars_}
            edge.range = {
                "min": min_.map(map_),
                "max": max_.map(map_)
            }
        elif edge.domain.dimension:
            vars_ = edge.domain.dimension.fields
            edge.domain.dimension = edge.domain.dimension.copy()
            edge.domain.dimension.fields = [schema[v].es_column for v in vars_]
        elif all(edge.domain.partitions.where):
            vars_ = set()
            for p in edge.domain.partitions:
                vars_ |= p.where.vars()

        try:
            depths = set(len(c.nested_path)-1 for v in vars_ for c in schema[v])
            if -1 in depths:
                Log.error(
                    "Do not know of column {{column}}",
                    column=unwraplist([v for v in vars_ if schema[v]==None])
                )
            if len(depths) > 1:
                Log.error("expression {{expr}} spans tables, can not handle", expr=edge.value)
            max_depth = MAX(depths)
            while len(output) <= max_depth:
                output.append([])
        except Exception as edge:
            # USUALLY THE SCHEMA IS EMPTY, SO WE ASSUME THIS IS A SIMPLE QUERY
            max_depth = 0
            output.append([])

        limit = 0
        output[max_depth].append(AggsDecoder(edge, query, limit))
    return output


def es_aggsop(es, frum, query):
    select = wrap([s.copy() for s in listwrap(query.select)])
    # [0] is a cheat; each es_column should be a dict of columns keyed on type, like in sqlite
    es_column_map = {v: frum.schema[v][0].es_column for v in query.vars()}

    es_query = Data()
    new_select = Data()  #MAP FROM canonical_name (USED FOR NAMES IN QUERY) TO SELECT MAPPING
    formula = []
    for s in select:
        if s.aggregate == "count" and isinstance(s.value, Variable) and s.value.var == ".":
            s.pull = "doc_count"
        elif isinstance(s.value, Variable):
            if s.value.var == ".":
                if frum.typed:
                    # STATISITCAL AGGS IMPLY $value, WHILE OTHERS CAN BE ANYTHING
                    if s.aggregate in NON_STATISTICAL_AGGS:
                        #TODO: HANDLE BOTH $value AND $objects TO COUNT
                        Log.error("do not know how to handle")
                    else:
                        s.value.var = "$value"
                        new_select["$value"] += [s]
                else:
                    if s.aggregate in NON_STATISTICAL_AGGS:
                        #TODO:  WE SHOULD BE ABLE TO COUNT, BUT WE MUST *OR* ALL LEAF VALUES TO DO IT
                        Log.error("do not know how to handle")
                    else:
                        Log.error('Not expecting ES to have a value at "." which {{agg}} can be applied', agg=s.aggregate)
            elif s.aggregate == "count":
                s.value = s.value.map(es_column_map)
                new_select["count_"+literal_field(s.value.var)] += [s]
            else:
                s.value = s.value.map(es_column_map)
                new_select[literal_field(s.value.var)] += [s]
        else:
            formula.append(s)

    for canonical_name, many in new_select.items():
        representative = many[0]
        if representative.value.var == ".":
            Log.error("do not know how to handle")
        else:
            field_name = representative.value.var

        # canonical_name=literal_field(many[0].name)
        for s in many:
            if s.aggregate == "count":
                es_query.aggs[literal_field(canonical_name)].value_count.field = field_name
                s.pull = literal_field(canonical_name) + ".value"
            elif s.aggregate == "median":
                # ES USES DIFFERENT METHOD FOR PERCENTILES
                key = literal_field(canonical_name + " percentile")

                es_query.aggs[key].percentiles.field = field_name
                es_query.aggs[key].percentiles.percents += [50]
                s.pull = key + ".values.50\.0"
            elif s.aggregate == "percentile":
                # ES USES DIFFERENT METHOD FOR PERCENTILES
                key = literal_field(canonical_name + " percentile")
                if isinstance(s.percentile, basestring) or s.percetile < 0 or 1 < s.percentile:
                    Log.error("Expecting percentile to be a float from 0.0 to 1.0")
                percent = Math.round(s.percentile * 100, decimal=6)

                es_query.aggs[key].percentiles.field = field_name
                es_query.aggs[key].percentiles.percents += [percent]
                s.pull = key + ".values." + literal_field(unicode(percent))
            elif s.aggregate == "cardinality":
                # ES USES DIFFERENT METHOD FOR CARDINALITY
                key = literal_field(canonical_name + " cardinality")

                es_query.aggs[key].cardinality.field = field_name
                s.pull = key + ".value"
            elif s.aggregate == "stats":
                # REGULAR STATS
                stats_name = literal_field(canonical_name)
                es_query.aggs[stats_name].extended_stats.field = field_name

                # GET MEDIAN TOO!
                median_name = literal_field(canonical_name + " percentile")
                es_query.aggs[median_name].percentiles.field = field_name
                es_query.aggs[median_name].percentiles.percents += [50]

                s.pull = {
                    "count": stats_name + ".count",
                    "sum": stats_name + ".sum",
                    "min": stats_name + ".min",
                    "max": stats_name + ".max",
                    "avg": stats_name + ".avg",
                    "sos": stats_name + ".sum_of_squares",
                    "std": stats_name + ".std_deviation",
                    "var": stats_name + ".variance",
                    "median": median_name + ".values.50\.0"
                }
            elif s.aggregate == "union":
                # USE TERMS AGGREGATE TO SIMULATE union
                stats_name = literal_field(canonical_name)
                es_query.aggs[stats_name].terms.field = field_name
                es_query.aggs[stats_name].terms.size = Math.min(s.limit, MAX_LIMIT)
                s.pull = stats_name + ".buckets.key"
            else:
                # PULL VALUE OUT OF THE stats AGGREGATE
                es_query.aggs[literal_field(canonical_name)].extended_stats.field = field_name
                s.pull = literal_field(canonical_name) + "." + aggregates1_4[s.aggregate]

    for i, s in enumerate(formula):
        canonical_name = literal_field(s.name)
        abs_value = s.value.map(es_column_map)

        if s.aggregate == "count":
            es_query.aggs[literal_field(canonical_name)].value_count.script = abs_value.to_ruby()
            s.pull = literal_field(canonical_name) + ".value"
        elif s.aggregate == "median":
            # ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
            key = literal_field(canonical_name + " percentile")

            es_query.aggs[key].percentiles.script = abs_value.to_ruby()
            es_query.aggs[key].percentiles.percents += [50]
            s.pull = key + ".values.50\.0"
        elif s.aggregate == "percentile":
            # ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
            key = literal_field(canonical_name + " percentile")
            percent = Math.round(s.percentile * 100, decimal=6)

            es_query.aggs[key].percentiles.script = abs_value.to_ruby()
            es_query.aggs[key].percentiles.percents += [percent]
            s.pull = key + ".values." + literal_field(unicode(percent))
        elif s.aggregate == "cardinality":
            # ES USES DIFFERENT METHOD FOR CARDINALITY
            key = canonical_name + " cardinality"

            es_query.aggs[key].cardinality.script = abs_value.to_ruby()
            s.pull = key + ".value"
        elif s.aggregate == "stats":
            # REGULAR STATS
            stats_name = literal_field(canonical_name)
            es_query.aggs[stats_name].extended_stats.script = abs_value.to_ruby()

            # GET MEDIAN TOO!
            median_name = literal_field(canonical_name + " percentile")
            es_query.aggs[median_name].percentiles.script = abs_value.to_ruby()
            es_query.aggs[median_name].percentiles.percents += [50]

            s.pull = {
                "count": stats_name + ".count",
                "sum": stats_name + ".sum",
                "min": stats_name + ".min",
                "max": stats_name + ".max",
                "avg": stats_name + ".avg",
                "sos": stats_name + ".sum_of_squares",
                "std": stats_name + ".std_deviation",
                "var": stats_name + ".variance",
                "median": median_name + ".values.50\.0"
            }
        elif s.aggregate=="union":
            # USE TERMS AGGREGATE TO SIMULATE union
            stats_name = literal_field(canonical_name)
            es_query.aggs[stats_name].terms.script_field = abs_value.to_ruby()
            s.pull = stats_name + ".buckets.key"
        else:
            # PULL VALUE OUT OF THE stats AGGREGATE
            s.pull = canonical_name + "." + aggregates1_4[s.aggregate]
            es_query.aggs[canonical_name].extended_stats.script = abs_value.to_ruby()

    decoders = get_decoders_by_depth(query)
    start = 0

    vars_ = query.where.vars()

    #<TERRIBLE SECTION> THIS IS WHERE WE WEAVE THE where CLAUSE WITH nested
    split_where = split_expression_by_depth(query.where, schema=frum.schema)

    if len(split_field(frum.name)) > 1:
        if any(split_where[2::]):
            Log.error("Where clause is too deep")

        for d in decoders[1]:
            es_query = d.append_query(es_query, start)
            start += d.num_columns

        if split_where[1]:
            #TODO: INCLUDE FILTERS ON EDGES
            filter_ = simplify_esfilter(AndOp("and", split_where[1]).to_esfilter())
            es_query = Data(
                aggs={"_filter": set_default({"filter": filter_}, es_query)}
            )

        es_query = wrap({
            "aggs": {"_nested": set_default(
                {
                    "nested": {
                        "path": frum.query_path
                    }
                },
                es_query
            )}
        })
    else:
        if any(split_where[1::]):
            Log.error("Where clause is too deep")

    for d in decoders[0]:
        es_query = d.append_query(es_query, start)
        start += d.num_columns

    if split_where[0]:
        #TODO: INCLUDE FILTERS ON EDGES
        filter = simplify_esfilter(AndOp("and", split_where[0]).to_esfilter())
        es_query = Data(
            aggs={"_filter": set_default({"filter": filter}, es_query)}
        )
    # </TERRIBLE SECTION>

    if not es_query:
        es_query = wrap({"query": {"match_all": {}}})

    es_query.size = 0

    with Timer("ES query time") as es_duration:
        result = es09.util.post(es, es_query, query.limit)

    try:
        format_time = Timer("formatting")
        with format_time:
            decoders = [d for ds in decoders for d in ds]
            result.aggregations.doc_count = coalesce(result.aggregations.doc_count, result.hits.total)  # IT APPEARS THE OLD doc_count IS GONE

            formatter, groupby_formatter, aggop_formatter, mime_type = format_dispatch[query.format]
            if query.edges:
                output = formatter(decoders, result.aggregations, start, query, select)
            elif query.groupby:
                output = groupby_formatter(decoders, result.aggregations, start, query, select)
            else:
                output = aggop_formatter(decoders, result.aggregations, start, query, select)

        output.meta.timing.formatting = format_time.duration
        output.meta.timing.es_search = es_duration.duration
        output.meta.content_type = mime_type
        output.meta.es_query = es_query
        return output
    except Exception as e:
        if query.format not in format_dispatch:
            Log.error("Format {{format|quote}} not supported yet", format=query.format, cause=e)
        Log.error("Some problem", e)



EMPTY = {}
EMPTY_LIST = []


def drill(agg):
    deeper = agg.get("_filter", agg.get("_nested"))
    while deeper:
        agg = deeper
        deeper = agg.get("_filter", agg.get("_nested"))
    return agg


def aggs_iterator(aggs, decoders, coord=True):
    """
    DIG INTO ES'S RECURSIVE aggs DATA-STRUCTURE:
    RETURN AN ITERATOR OVER THE EFFECTIVE ROWS OF THE RESULTS

    :param aggs: ES AGGREGATE OBJECT
    :param decoders:
    :param coord: TURN ON LOCAL COORDINATE LOOKUP
    """
    depth = max(d.start + d.num_columns for d in decoders)
    parts = [None] * depth

    def _aggs_iterator(agg, d):
        agg = drill(agg)

        if d > 0:
            for k, v in agg.items():
                if k == "_match":
                    for i, b in enumerate(v.get("buckets", EMPTY_LIST)):
                        parts[d] = b
                        b["_index"] = i
                        for a in _aggs_iterator(b, d - 1):
                            yield a
                elif k == "_other":
                    parts[d] = Null
                    for b in v.get("buckets", EMPTY_LIST):
                        for a in _aggs_iterator(b, d - 1):
                            yield a
                elif k == "_missing":
                    parts[d] = Null
                    b = drill(v)
                    if b.get("doc_count"):
                        for a in _aggs_iterator(b, d - 1):
                            yield a
                elif k.startswith("_join_"):
                    v["key"] = int(k[6:])
                    parts[d] = v
                    for a in _aggs_iterator(v, d - 1):
                        yield a
        else:
            for k, v in agg.items():
                if k == "_match":
                    for i, b in enumerate(v.get("buckets", EMPTY_LIST)):
                        parts[d] = b
                        if b.get("doc_count"):
                            b = drill(b)
                            b["_index"] = i
                            yield b
                elif k == "_other":
                    parts[d] = Null
                    for b in v.get("buckets", EMPTY_LIST):
                        b = drill(b)
                        if b.get("doc_count"):
                            yield b
                elif k == "_missing":
                    parts[d] = Null
                    b = drill(v)
                    if b.get("doc_count"):
                        yield b
                elif k.startswith("_join_"):
                    v["_index"] = int(k[6:])
                    parts[d] = v
                    yield v

    if coord:
        for a in _aggs_iterator(unwrap(aggs), depth - 1):
            coord = tuple(d.get_index(parts) for d in decoders)
            yield parts, coord, a
    else:
        for a in _aggs_iterator(unwrap(aggs), depth - 1):
            yield parts, None, a


def count_dim(aggs, decoders):
    if any(isinstance(d, (DefaultDecoder, DimFieldListDecoder, ObjectDecoder)) for d in decoders):
        # ENUMERATE THE DOMAINS, IF UNKNOWN AT QUERY TIME
        for row, coord, agg in aggs_iterator(aggs, decoders, coord=False):
            for d in decoders:
                d.count(row)
        for d in decoders:
            d.done_count()
    new_edges = wrap([d.edge for d in decoders])
    return new_edges


format_dispatch = {}
from pyLibrary.queries.es14.format import format_cube

_ = format_cube

