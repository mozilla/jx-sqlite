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

from collections import Mapping

from mo_logs import Log
from mo_dots import set_default, wrap, split_field, join_field, concat_field
from mo_math import Math
from jx_base.queries import is_variable_name
from pyLibrary.queries.expressions import Expression
from pyLibrary.queries.namespace import convert_list, Namespace
from pyLibrary.queries.query import QueryOp
from mo_times.dates import Date


class Typed(Namespace):
    """
     NOTE: USING THE ".$value" SUFFIX IS DEPRECIATED: CURRENT VERSIONS OF ES ARE STRONGLY TYPED, LEAVING NO
     CASE WHERE A GENERAL "value" IS USEFUL.  WE WOULD LIKE TO MOVE TO ".$number", ".$string", ETC. FOR
     EACH TYPE, LIKE WE DO WITH DATABASES
    """

    def __init__(self):
        self.converter_map = {
            "and": self._convert_many,
            "or": self._convert_many,
            "not": self.convert,
            "missing": self.convert,
            "exists": self.convert
        }

    def convert(self, expr):
        """
        ADD THE ".$value" SUFFIX TO ALL VARIABLES
        """
        if isinstance(expr, Expression):
            vars_ = expr.vars()
            rename = {v: concat_field(v, "$value") for v in vars_}
            return expr.map(rename)

        if expr is True or expr == None or expr is False:
            return expr
        elif Math.is_number(expr):
            return expr
        elif expr == ".":
            return "."
        elif is_variable_name(expr):
            #TODO: LOOKUP SCHEMA AND ADD ALL COLUMNS WITH THIS PREFIX
            return expr + ".$value"
        elif isinstance(expr, basestring):
            Log.error("{{name|quote}} is not a valid variable name", name=expr)
        elif isinstance(expr, Date):
            return expr
        elif isinstance(expr, QueryOp):
            return self._convert_query(expr)
        elif isinstance(expr, Mapping):
            if expr["from"]:
                return self._convert_query(expr)
            elif len(expr) >= 2:
                #ASSUME WE HAVE A NAMED STRUCTURE, NOT AN EXPRESSION
                return wrap({name: self.convert(value) for name, value in expr.items()})
            else:
                # ASSUME SINGLE-CLAUSE EXPRESSION
                k, v = expr.items()[0]
                return self.converter_map.get(k, self._convert_bop)(k, v)
        elif isinstance(expr, (list, set, tuple)):
            return wrap([self.convert(value) for value in expr])

    def _convert_query(self, query):
        output = QueryOp("from", None)
        output.select = self._convert_clause(query.select)
        output.where = self.convert(query.where)
        output.frum = self._convert_from(query.frum)
        output.edges = self._convert_clause(query.edges)
        output.groupby = self._convert_clause(query.groupby)
        output.window = convert_list(self._convert_window, query.window)
        output.having = convert_list(self._convert_having, query.having)
        output.sort = self._convert_clause(query.sort)
        output.limit = query.limit
        output.format = query.format

        return output

    def _convert_clause(self, clause):
        """
        JSON QUERY EXPRESSIONS HAVE MANY CLAUSES WITH SIMILAR COLUMN DELCARATIONS
        """
        if clause == None:
            return None
        elif isinstance(clause, Mapping):
            return set_default({"value": self.convert(clause["value"])}, clause)
        else:
            return [set_default({"value": self.convert(c.value)}, c) for c in clause]

    def _convert_from(self, frum):
        return frum

    def _convert_having(self, having):
        raise NotImplementedError()

    def _convert_window(self, window):
        raise NotImplementedError()

    def _convert_many(self, k, v):
        return {k: map(self.convert, v)}

    def _convert_bop(self, op, term):
        if isinstance(term, list):
            return {op: map(self.convert, term)}

        return {op: {var: val for var, val in term.items()}}

