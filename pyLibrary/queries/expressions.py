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
from collections import Mapping
from decimal import Decimal

from mo_dots import coalesce, wrap, set_default, literal_field, Null, split_field, startswith_field
from mo_dots import Data, join_field, unwraplist, ROOT_PATH, relative_field, unwrap
from mo_json import json2value, quote
from mo_logs import Log
from mo_logs.exceptions import suppress_exception
from mo_math import Math, OR, MAX
from mo_times.dates import Date

from pyLibrary import convert
from pyLibrary.queries.containers import STRUCT, OBJECT
from jx_base.queries import is_variable_name
from pyLibrary.queries.expression_compiler import compile_expression
from pyLibrary.sql.sqlite import quote_column

ALLOW_SCRIPTING = False
TRUE_FILTER = True
FALSE_FILTER = False
EMPTY_DICT = {}

_Query = None


def _late_import():
    global _Query

    from pyLibrary.queries.query import QueryOp as _Query

    _ = _Query


def jx_expression(expr):
    """
    WRAP A JSON EXPRESSION WITH OBJECT REPRESENTATION
    """
    if isinstance(expr, Expression):
        Log.error("Expecting JSON, not expression")

    if expr in (True, False, None) or expr == None or isinstance(expr, (float, int, Decimal, Date)):
        return Literal(None, expr)
    elif isinstance(expr, unicode):
        if is_variable_name(expr):
            return Variable(expr)
        elif not expr.strip():
            Log.error("expression is empty")
        else:
            Log.error("expression is not recognized: {{expr}}", expr=expr)
    elif isinstance(expr, (list, tuple)):
        return TupleOp("tuple", map(jx_expression, expr))  # FORMALIZE

    expr = wrap(expr)
    if expr.date:
        return DateOp("date", expr)

    try:
        items = expr.items()
    except Exception as e:
        Log.error("programmer error expr = {{value|quote}}", value=expr, cause=e)

    for item in items:
        op, term = item
        class_ = operators.get(op)
        if class_:
            term, clauses = class_.preprocess(op, expr)
            break
    else:
        if not items:
            return NullOp()
        raise Log.error("{{operator|quote}} is not a known operator", operator=op)

    if class_ is Literal:
        return class_(op, term)
    elif class_ is ScriptOp:
        if ALLOW_SCRIPTING:
            Log.warning("Scripting has been activated:  This has known security holes!!\nscript = {{script|quote}}", script=term)
            return class_(op, term)
        else:
            Log.error("scripting is disabled")
    elif term == None:
        return class_(op, [], **clauses)
    elif isinstance(term, list):
        terms = map(jx_expression, term)
        return class_(op, terms, **clauses)
    elif isinstance(term, Mapping):
        items = term.items()
        if class_.has_simple_form:
            if len(items) == 1:
                k, v = items[0]
                return class_(op, [Variable(k), Literal(None, v)], **clauses)
            else:
                return class_(op, {k: Literal(None, v) for k, v in items}, **clauses)
        else:
            return class_(op, jx_expression(term), **clauses)
    else:
        if op in ["literal", "date", "offset"]:
            return class_(op, term, **clauses)
        else:
            return class_(op, jx_expression(term), **clauses)


def jx_expression_to_function(expr):
    """
    RETURN FUNCTION THAT REQUIRES PARAMETERS (row, rownum=None, rows=None):
    """
    if isinstance(expr, Expression):
        if isinstance(expr, ScriptOp) and not isinstance(expr.script, unicode):
            return expr.script
        else:
            return compile_expression(expr.to_python())
    if expr != None and not isinstance(expr, (Mapping, list)) and hasattr(expr, "__call__"):
        return expr
    return compile_expression(jx_expression(expr).to_python())


class Expression(object):
    has_simple_form = False

    def __init__(self, op, terms):
        if isinstance(terms, (list, tuple)):
            if not all(isinstance(t, Expression) for t in terms):
                Log.error("Expecting an expression")
        elif isinstance(terms, Mapping):
            if not all(isinstance(k, Variable) and isinstance(v, Literal) for k, v in terms.items()):
                Log.error("Expecting an {<variable>: <literal>}")
        elif terms == None:
            pass
        else:
            if not isinstance(terms, Expression):
                Log.error("Expecting an expression")

    @property
    def name(self):
        return self.__class_.__name__

    @classmethod
    def preprocess(cls, op, clauses):
        return clauses[op], {k: jx_expression(v) for k, v in clauses.items() if k != op}

    def to_ruby(self, not_null=False, boolean=False):
        """
        :param not_null:  (Optimization) SET TO True IF YOU KNOW THIS EXPRESSION CAN NOT RETURN null
        :param boolean:   (Optimization) SET TO True IF YOU WANT A BOOLEAN RESULT
        :return: jRuby/ES code (unicode)
        """
        raise NotImplementedError

    def to_python(self, not_null=False, boolean=False):
        """
        :param not_null:  (Optimization) SET TO True IF YOU KNOW THIS EXPRESSION CAN NOT RETURN null
        :param boolean:   (Optimization) SET TO True IF YOU WANT A BOOLEAN RESULT
        :return: Python code (unicode)
        """
        raise Log.error("{{type}} has no `to_python` method", type=self.__class__.__name__)

    def to_sql(self, schema, not_null=False, boolean=False):
        """
        :param not_null:  IF YOU KNOW THIS WILL NOT RETURN NULL (DO NOT INCLUDE NULL CHECKS)
        :param boolean: IF YOU KNOW THIS WILL RETURN A BOOLEAN VALUE
        :return: A LIST OF {"name": col, "sql": value, "nested_path":nested_path} dicts WHERE
            col (string) IS THE PATH VALUE TO SET
            value IS A dict MAPPING TYPE TO SQL : (s=string, n=number, b=boolean, 0=null, j=json)
            nested_path IS THE IDEAL DEPTH THIS VALUE IS CALCULATED AT
        """
        raise Log.error("{{type}} has no `to_sql` method", type=self.__class__.__name__)

    def to_esfilter(self):
        raise Log.error("{{type}} has no `to_esfilter` method", type=self.__class__.__name__)

    def __data__(self):
        raise NotImplementedError

    def vars(self):
        raise Log.error("{{type}} has no `vars` method", type=self.__class__.__name__)

    def map(self, map):
        raise Log.error("{{type}} has no `map` method", type=self.__class__.__name__)

    def missing(self):
        # RETURN FILTER THAT INDICATE THIS EXPRESSIOn RETURNS null
        raise Log.error("{{type}} has no `missing` method", type=self.__class__.__name__)

    def exists(self):
        missing = self.missing()
        if not missing:
            return TrueOp()
        else:
            return NotOp("not", missing)

    def is_true(self):
        """
        :return: True, IF THIS EXPRESSION ALWAYS RETURNS BOOLEAN true
        """
        return FalseOp()  # GOOD DEFAULT ASSUMPTION

    def is_false(self):
        """
        :return: True, IF THIS EXPRESSION ALWAYS RETURNS BOOLEAN false
        """
        return FalseOp()  # GOOD DEFAULT ASSUMPTION


class Variable(Expression):

    def __init__(self, var):
        Expression.__init__(self, "", None)
        if not is_variable_name(var):
            Log.error("Expecting a variable name")
        self.var = var

    def to_ruby(self, not_null=False, boolean=False):
        if self.var == ".":
            return "_source"
        else:
            if self.var == "_id":
                return 'doc["_uid"].value.substring(doc["_uid"].value.indexOf(\'#\')+1)'
            q = quote(self.var)
            if not_null:
                if boolean:
                    return "doc[" + q + "].value==\"T\""
                else:
                    return "doc[" + q + "].value"
            else:
                if boolean:
                    return "doc[" + q + "].isEmpty() ? null : (doc[" + q + "].value==\"T\")"
                else:
                    return "doc[" + q + "].isEmpty() ? null : doc[" + q + "].value"

    def to_python(self, not_null=False, boolean=False):
        path = split_field(self.var)
        agg = "row"
        if not path:
            return agg
        elif path[0] in ["row", "rownum"]:
            # MAGIC VARIABLES
            agg = path[0]
            path = path[1:]
            if len(path)==0:
                return agg
        elif path[0] == "rows":
            if len(path) == 1:
                return "rows"
            elif path[1] in ["first", "last"]:
                agg = "rows." + path[1] + "()"
                path = path[2:]
            else:
                Log.error("do not know what {{var}} of `rows` is", var=path[1])

        for p in path[:-1]:
            if not_null:
                agg = agg+".get("+convert.value2quote(p)+")"
            else:
                agg = agg+".get("+convert.value2quote(p)+", EMPTY_DICT)"
        return agg+".get("+convert.value2quote(path[-1])+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        cols = [c for cname, cs in schema.items() if startswith_field(cname, self.var) for c in cs]
        if not cols:
            # DOES NOT EXIST
            return wrap([{"name": ".", "sql": {"0": "NULL"}, "nested_path": ROOT_PATH}])
        acc = Data()
        for col in cols:
            if col.type == OBJECT:
                prefix = self.var + "."
                for cn, cs in schema.items():
                    if cn.startswith(prefix):
                        for child_col in cs:
                            acc[literal_field(child_col.nested_path[0])][literal_field(schema.get_column_name(child_col))][json_type_to_sql_type[child_col.type]] = quote_column(child_col.es_column).sql
            else:
                nested_path = col.nested_path[0]
                acc[literal_field(nested_path)][literal_field(schema.get_column_name(col))][json_type_to_sql_type[col.type]] = quote_column(col.es_column).sql

        return wrap([
            {"name": relative_field(cname, self.var), "sql": types, "nested_path": nested_path}
            for nested_path, pairs in acc.items() for cname, types in pairs.items()
        ])

    def __call__(self, row, rownum=None, rows=None):
        path = split_field(self.var)
        for p in path:
            row = row.get(p)
            if row is None:
                return None
        if isinstance(row, list) and len(row) == 1:
            return row[0]
        return row

    def __data__(self):
        return self.var

    def vars(self):
        return {self.var}

    def map(self, map_):
        if not isinstance(map_, Mapping):
            Log.error("Expecting Mapping")

        return Variable(coalesce(map_.get(self.var), self.var))

    def missing(self):
        # RETURN FILTER THAT INDICATE THIS EXPRESSION RETURNS null
        return MissingOp("missing", self)

    def exists(self):
        return ExistsOp("exists", self)

    def __hash__(self):
        return self.var.__hash__()

    def __eq__(self, other):
        return self.var.__eq__(other)

    def __unicode__(self):
        return self.var

    def __str__(self):
        return str(self.var)


class OffsetOp(Expression):
    """
    OFFSET INDEX INTO A TUPLE
    """

    def __init__(self, op, var):
        Expression.__init__(self, "offset", None)
        if not Math.is_integer(var):
            Log.error("Expecting an integer")
        self.var = var

    def to_python(self, not_null=False, boolean=False):
        return "row[" + unicode(self.var) + "] if 0<=" + unicode(self.var) + "<len(row) else None"

    def __call__(self, row, rownum=None, rows=None):
        try:
            return row[self.var]
        except Exception:
            return None

    def __data__(self):
        return {"offset": self.var}

    def vars(self):
        return {}

    def missing(self):
        # RETURN FILTER THAT INDICATE THIS EXPRESSION RETURNS null
        return MissingOp("missing", self)

    def exists(self):
        return ExistsOp("exists", self)

    def __hash__(self):
        return self.var.__hash__()

    def __eq__(self, other):
        return self.var == other

    def __unicode__(self):
        return unicode(self.var)

    def __str__(self):
        return str(self.var)


class RowsOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.var, self.offset = term
        if isinstance(self.var, Variable):
            if isinstance(self.var, Variable) and not any(self.var.var.startswith(p) for p in ["row.", "rows.", "rownum"]):  # VARIABLES ARE INTERPRETED LITERALLY
                self.var = Literal("literal", self.var.var)
            else:
                Log.error("can not handle")
        else:
            Log.error("can not handle")

    def to_python(self, not_null=False, boolean=False):
        agg = "rows[rownum+" + self.offset.to_python() + "]"
        path = split_field(json2value(self.var.json))
        if not path:
            return agg

        for p in path[:-1]:
            agg = agg+".get("+convert.value2quote(p)+", EMPTY_DICT)"
        return agg+".get("+convert.value2quote(path[-1])+")"

    def __data__(self):
        if isinstance(self.var, Literal) and isinstance(self.offset, Literal):
            return {"rows": {self.var.json, json2value(self.offset.json)}}
        else:
            return {"rows": [self.var.__data__(), self.offset.__data__()]}

    def vars(self):
        return self.var.vars() | self.offset.vars() | {"rows", "rownum"}

    def map(self, map_):
        return BinaryOp("rows", [self.var.map(map_), self.offset.map(map_)])

    def missing(self):
        return MissingOp("missing", self)


class GetOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.var, self.offset = term

    def to_python(self, not_null=False, boolean=False):
        obj = self.var.to_python()
        code = self.offset.to_python()
        return obj + "[" + code + "]"

    def __data__(self):
        if isinstance(self.var, Literal) and isinstance(self.offset, Literal):
            return {"get": {self.var.json, json2value(self.offset.json)}}
        else:
            return {"get": [self.var.__data__(), self.offset.__data__()]}

    def vars(self):
        return self.var.vars() | self.offset.vars()

    def map(self, map_):
        return BinaryOp("get", [self.var.map(map_), self.offset.map(map_)])


class ScriptOp(Expression):
    """
    ONLY FOR TESTING AND WHEN YOU TRUST THE SCRIPT SOURCE
    """

    def __init__(self, op, script):
        Expression.__init__(self, op, None)
        self.script = script

    def to_ruby(self, not_null=False, boolean=False):
        return self.script

    def to_python(self, not_null=False, boolean=False):
        return self.script

    def to_esfilter(self):
        return {"script": {"script": self.script}}

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def __unicode__(self):
        return self.script

    def __str__(self):
        return str(self.script)


class Literal(Expression):
    """
    A literal JSON document
    """

    def __new__(cls, op, term):
        if term == None:
            return NullOp()
        if term is True:
            return TrueOp()
        if term is False:
            return FalseOp()
        if isinstance(term, Mapping) and term.date:
            # SPECIAL CASE
            return object.__new__(DateOp, None, term)
        return object.__new__(cls, op, term)

    def __init__(self, op, term):
        Expression.__init__(self, "", None)
        if term == "":
            self.json = '""'
        else:
            self.json = convert.value2json(term)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        if other == None:
            if self.json == "null":
                return True
            else:
                return False
        elif self.json == "null":
            return False

        Log.warning("expensive")

        from mo_testing.fuzzytestcase import assertAlmostEqual

        try:
            assertAlmostEqual(json2value(self.json), other)
            return True
        except Exception:
            return False

    def to_ruby(self, not_null=False, boolean=False):
        def _convert(v):
            if v is None:
                return "null"
            if v is True:
                return "true"
            if v is False:
                return "false"
            if isinstance(v, basestring):
                return quote(v)
            if isinstance(v, (int, long, float)):
                return unicode(v)
            if isinstance(v, dict):
                return "[" + ", ".join(quote(k) + ": " + _convert(vv) for k, vv in v.items()) + "]"
            if isinstance(v, list):
                return "[" + ", ".join(_convert(vv) for vv in v) + "]"

        return _convert(convert.json_decoder(self.json))

    def to_python(self, not_null=False, boolean=False):
        return repr(unwrap(json2value(self.json)))

    def to_sql(self, schema, not_null=False, boolean=False):
        value = json2value(self.json)
        v = sql_quote(value)
        if v == None:
            return wrap([{"name": "."}])
        elif isinstance(value, unicode):
            return wrap([{"name": ".", "sql": {"s": sql_quote(value)}}])
        elif Math.is_number(v):
            return wrap([{"name": ".", "sql": {"n": sql_quote(value)}}])
        elif v in [True, False]:
            return wrap([{"name": ".", "sql": {"b": sql_quote(value)}}])
        else:
            return wrap([{"name": ".", "sql": {"j": sql_quote(self.json)}}])

    def to_esfilter(self):
        return json2value(self.json)

    def __data__(self):
        return {"literal": json2value(self.json)}

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        if self.json == '""':
            return TrueOp()
        return FalseOp()

    def __call__(self, row=None, rownum=None, rows=None):
        return json2value(self.json)

    def __unicode__(self):
        return self.json

    def __str__(self):
        return str(self.json)

class NullOp(Literal):
    """
    FOR USE WHEN EVERYTHING IS EXPECTED TO BE AN Expression
    USE IT TO EXPECT A NULL VALUE IN assertAlmostEqual
    """

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, op, None)

    def __nonzero__(self):
        return False

    def __eq__(self, other):
        return other == None

    def to_ruby(self, not_null=False, boolean=False):
        return "null"

    def to_python(self, not_null=False, boolean=False):
        return "None"

    def to_sql(self, schema, not_null=False, boolean=False):
        return Null

    def to_esfilter(self):
        return {"not": {"match_all": {}}}

    def __data__(self):
        return {"null": {}}

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return TrueOp()

    def exists(self):
        return FalseOp()

    def __call__(self, row=None, rownum=None, rows=None):
        return Null

    def __unicode__(self):
        return "null"

    def __str__(self):
        return b"null"

    def __data__(self):
        return None


class TrueOp(Literal):
    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, op, True)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        return other == True

    def to_ruby(self, not_null=False, boolean=False):
        return "true"

    def to_python(self, not_null=False, boolean=False):
        return "True"

    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap([{"name": ".", "sql": {"b": "1"}}])

    def to_esfilter(self):
        return {"match_all": {}}

    def __data__(self):
        return True

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return FalseOp()

    def is_true(self):
        return TrueOp()

    def is_false(self):
        return FalseOp()

    def __call__(self, row=None, rownum=None, rows=None):
        return True

    def __unicode__(self):
        return "true"

    def __str__(self):
        return b"true"


class FalseOp(Literal):
    def __new__(cls, *args, **kwargs):
        return object.__new__(cls, *args, **kwargs)

    def __init__(self, op=None, term=None):
        Literal.__init__(self, op, False)

    def __nonzero__(self):
        return False

    def __eq__(self, other):
        return other == False

    def to_ruby(self, not_null=False, boolean=False):
        return "false"

    def to_python(self, not_null=False, boolean=False):
        return "False"

    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap([{"name": ".", "sql": {"b": "0"}}])

    def to_esfilter(self):
        return {"not": {"match_all": {}}}

    def __data__(self):
        return False

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        return self

    def is_true(self):
        return FalseOp()

    def is_false(self):
        return TrueOp()

    def __call__(self, row=None, rownum=None, rows=None):
        return False

    def __unicode__(self):
        return "false"

    def __str__(self):
        return b"false"


class DateOp(Literal):
    def __init__(self, op, term):
        self.value = term.date
        Literal.__init__(self, op, Date(term.date).unix)

    def to_python(self, not_null=False, boolean=False):
        return unicode(Date(self.value).unix)

    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap([{"name": ".", "sql": {"n": sql_quote(json2value(self.json))}}])

    def to_esfilter(self):
        return json2value(self.json)

    def __data__(self):
        return {"date": self.value}

    def __call__(self, row=None, rownum=None, rows=None):
        return Date(self.value)

    def __unicode__(self):
        return self.json

    def __str__(self):
        return str(self.json)


class TupleOp(Expression):

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def to_ruby(self, not_null=False, boolean=False):
        Log.error("not supported")

    def to_python(self, not_null=False, boolean=False):
        if len(self.terms) == 0:
            return "tuple()"
        elif len(self.terms) == 1:
            return "(" + self.terms[0].to_python() + ",)"
        else:
            return "(" + (",".join(t.to_python() for t in self.terms)) + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap([{"name": ".", "sql": t.to_sql(schema)[0].sql} for t in self.terms])

    def to_esfilter(self):
        Log.error("not supported")

    def __data__(self):
        return {"tuple": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return TupleOp("tuple", [t.map(map_) for t in self.terms])

    def missing(self):
        return False


class LeavesOp(Expression):

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.term = term

    def to_ruby(self, not_null=False, boolean=False):
        Log.error("not supported")

    def to_python(self, not_null=False, boolean=False):
        return "Data(" + self.term.to_python() + ").leaves()"

    def to_sql(self, schema, not_null=False, boolean=False):
        if not isinstance(self.term, Variable):
            Log.error("Can only handle Variable")
        term = self.term.var
        prefix_length = len(split_field(term))
        return wrap([
            {
                "name": join_field(split_field(schema.get_column_name(c))[prefix_length:]),
                "sql": Variable(schema.get_column_name(c)).to_sql(schema)[0].sql
            }
            for n, cols in schema.items()
            if startswith_field(n, term)
            for c in cols
            if c.type not in STRUCT
        ])

    def to_esfilter(self):
        Log.error("not supported")

    def __data__(self):
        return {"leaves": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return LeavesOp("leaves", self.term.map(map_))

    def missing(self):
        return False


class BinaryOp(Expression):
    has_simple_form = True

    operators = {
        "sub": "-",
        "subtract": "-",
        "minus": "-",
        "mul": "*",
        "mult": "*",
        "multiply": "*",
        "div": "/",
        "divide": "/",
        "exp": "**",
        "mod": "%"
    }

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        if op not in BinaryOp.operators:
            Log.error("{{op|quote}} not a recognized operator", op=op)
        self.op = op
        self.lhs, self.rhs = terms
        self.default = default

    @property
    def name(self):
        return self.op;

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby(not_null=True)
        rhs = self.rhs.to_ruby(not_null=True)
        script = "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"
        missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

        if self.op in BinaryOp.operators:
            script = "(" + script + ").doubleValue()"  # RETURN A NUMBER, NOT A STRING

        output = WhenOp(
            "when",
            missing,
            **{
                "then": self.default,
                "else":
                    ScriptOp("script", script)
            }
        ).to_ruby()
        return output

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.lhs.to_python() + ") " + BinaryOp.operators[self.op] + " (" + self.rhs.to_python()+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema)[0].sql.n
        rhs = self.rhs.to_sql(schema)[0].sql.n

        return wrap([{"name": ".", "sql": {"n": "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"}}])

    def to_esfilter(self):
        if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal) or self.op in BinaryOp.algebra_ops:
            return {"script": {"script": self.to_ruby()}}

        if self.op in ["eq", "term"]:
            return {"term": {self.lhs.var: self.rhs.to_esfilter()}}
        elif self.op in ["ne", "neq"]:
            return {"not": {"term": {self.lhs.var: self.rhs.to_esfilter()}}}
        elif self.op in BinaryOp.ineq_ops:
            return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
        else:
            Log.error("Logic error")

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {self.op: {self.lhs.var, json2value(self.rhs.json)}, "default": self.default}
        else:
            return {self.op: [self.lhs.__data__(), self.rhs.__data__()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return BinaryOp(self.op, [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FalseOp()
        else:
            return OrOp("or", [self.lhs.missing(), self.rhs.missing()])


class InequalityOp(Expression):
    has_simple_form = True

    operators = {
        "gt": ">",
        "gte": ">=",
        "lte": "<=",
        "lt": "<"
    }

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        if op not in InequalityOp.operators:
            Log.error("{{op|quote}} not a recognized operator", op=op)
        self.op = op
        self.lhs, self.rhs = terms
        self.default = default

    @property
    def name(self):
        return self.op;

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby(not_null=True)
        rhs = self.rhs.to_ruby(not_null=True)
        script = "(" + lhs + ") " + InequalityOp.operators[self.op] + " (" + rhs + ")"
        missing = OrOp("or", [self.lhs.missing(), self.rhs.missing()])

        output = WhenOp(
            "when",
            missing,
            **{
                "then": self.default,
                "else":
                    ScriptOp("script", script)
            }
        ).to_ruby()
        return output

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.lhs.to_python() + ") " + InequalityOp.operators[self.op] + " (" + self.rhs.to_python()+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema, not_null=True)[0].sql
        rhs = self.rhs.to_sql(schema, not_null=True)[0].sql
        lhs_exists = self.lhs.exists().to_sql(schema)[0].sql
        rhs_exists = self.rhs.exists().to_sql(schema)[0].sql

        if len(lhs) == 1 and len(rhs) == 1:
            return wrap([{"name":".", "sql": {
                "b": "(" + lhs.values()[0] + ") " + InequalityOp.operators[self.op] + " (" + rhs.values()[0] + ")"
            }}])

        ors = []
        for l in "bns":
            ll = lhs[l]
            if not ll:
                continue
            for r in "bns":
                rr = rhs[r]
                if not rr:
                    continue
                elif r == l:
                    ors.append(
                        "(" + lhs_exists[l] + ") AND (" + rhs_exists[r] + ") AND (" + lhs[l] + ") " +
                        InequalityOp.operators[self.op] + " (" + rhs[r] + ")"
                    )
                elif (l > r and self.op in ["gte", "gt"]) or (l < r and self.op in ["lte", "lt"]):
                    ors.append(
                        "(" + lhs_exists[l] + ") AND (" + rhs_exists[r] + ")"
                    )
        sql = "(" + ") OR (".join(ors) + ")"

        return wrap([{"name":".", "sql": {"b": sql}}])

    def to_esfilter(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"range": {self.lhs.var: {self.op: json2value(self.rhs.json)}}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {self.op: {self.lhs.var, json2value(self.rhs.json)}, "default": self.default}
        else:
            return {self.op: [self.lhs.__data__(), self.rhs.__data__()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return InequalityOp(self.op, [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FalseOp()
        else:
            return OrOp("or", [self.lhs.missing(), self.rhs.missing()])


class DivOp(Expression):
    has_simple_form = True

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        self.lhs, self.rhs = terms
        self.default = default

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby(not_null=True)
        rhs = self.rhs.to_ruby(not_null=True)
        script = "((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue()"

        output = WhenOp(
            "when",
            OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
            **{
                "then": self.default,
                "else":
                    ScriptOp("script", script)
            }
        ).to_ruby()
        return output

    def to_python(self, not_null=False, boolean=False):
        return "None if ("+self.missing().to_python()+") else (" + self.lhs.to_python(not_null=True) + ") / (" + self.rhs.to_python(not_null=True)+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema)[0].sql.n
        rhs = self.rhs.to_sql(schema)[0].sql.n
        d = self.default.to_sql(schema)[0].sql.n

        if lhs and rhs:
            if d == None:
                return wrap([{
                    "name": ".",
                    "sql": {"n": "(" + lhs + ") / (" + rhs + ")"}
                }])
            else:
                return wrap([{
                    "name": ".",
                    "sql": {"n": "COALESCE((" + lhs + ") / (" + rhs + "), " + d + ")"}
                }])
        else:
            return Null

    def to_esfilter(self):
        if not isinstance(self.lhs, Variable) or not isinstance(self.rhs, Literal):
            return {"script": {"script": self.to_ruby()}}
        else:
            Log.error("Logic error")

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"div": {self.lhs.var, json2value(self.rhs.json)}, "default": self.default}
        else:
            return {"div": [self.lhs.__data__(), self.rhs.__data__()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return DivOp("div", [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FalseOp()
        else:
            return OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])])


class FloorOp(Expression):
    has_simple_form = True

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        self.lhs, self.rhs = terms
        self.default = default

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby(not_null=True)
        rhs = self.rhs.to_ruby(not_null=True)
        script = "Math.floor(((double)(" + lhs + ") / (double)(" + rhs + ")).doubleValue())*(" + rhs + ")"

        output = WhenOp(
            "when",
            OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])]),
            **{
                "then": self.default,
                "else":
                    ScriptOp("script", script)
            }
        ).to_ruby()
        return output

    def to_python(self, not_null=False, boolean=False):
        return "Math.floor(" + self.lhs.to_python() + ", " + self.rhs.to_python()+")"

    def to_esfilter(self):
        Log.error("Logic error")

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"floor": {self.lhs.var, json2value(self.rhs.json)}, "default": self.default}
        else:
            return {"floor": [self.lhs.__data__(), self.rhs.__data__()], "default": self.default}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars() | self.default.vars()

    def map(self, map_):
        return FloorOp("floor", [self.lhs.map(map_), self.rhs.map(map_)], default=self.default.map(map_))

    def missing(self):
        if self.default.exists():
            return FalseOp()
        else:
            return OrOp("or", [self.lhs.missing(), self.rhs.missing(), EqOp("eq", [self.rhs, Literal("literal", 0)])])


class EqOp(Expression):
    has_simple_form = True

    def __new__(cls, op, terms):
        if isinstance(terms, list):
            return object.__new__(cls, op, terms)

        items = terms.items()
        if len(items) == 1:
            if isinstance(items[0][1], list):
                return InOp("in", items[0])
            else:
                return EqOp("eq", items[0])
        else:
            acc = []
            for a, b in items:
                if b.json.startswith("["):
                    acc.append(InOp("in", [Variable(a), b]))
                else:
                    acc.append(EqOp("eq", [Variable(a), b]))
            return AndOp("and", acc)

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        self.op = op
        self.lhs, self.rhs = terms

    def to_ruby(self, not_null=False, boolean=False):
        return "(" + self.lhs.to_ruby() + ") == (" + self.rhs.to_ruby()+")"

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.lhs.to_python() + ") == (" + self.rhs.to_python()+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema)
        rhs = self.rhs.to_sql(schema)
        acc = []
        if len(lhs) != len(rhs):
            Log.error("lhs and rhs have different dimensionality!?")
        for l, r in zip(lhs, rhs):
            for t in "bsnj":
                if l.sql[t] == None:
                    if r.sql[t] == None:
                        pass
                    else:
                        acc.append("(" + r.sql[t] + ") IS NULL")
                else:
                    if r.sql[t] == None:
                        acc.append("(" + l.sql[t] + ") IS NULL")
                    else:
                        acc.append("((" + l.sql[t] + ") = (" + r.sql[t] + ") OR ((" + l.sql[t] + ") IS NULL AND (" + r.sql[t] + ") IS NULL))")
        if not acc:
            return FalseOp().to_sql(schema)
        else:
            return wrap([{"name": ".", "sql": {"b": " OR ".join(acc)}}])

    def to_esfilter(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            rhs = json2value(self.rhs.json)
            if isinstance(rhs, list):
                if len(rhs) == 1:
                    return {"term": {self.lhs.var: rhs[0]}}
                else:
                    return {"terms": {self.lhs.var: rhs}}
            else:
                return {"term": {self.lhs.var: rhs}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"eq": {self.lhs.var, json2value(self.rhs.json)}}
        else:
            return {"eq": [self.lhs.__data__(), self.rhs.__data__()]}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return EqOp(self.op, [self.lhs.map(map_), self.rhs.map(map_)])

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()


class NeOp(Expression):
    has_simple_form = True

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if isinstance(terms, (list, tuple)):
            self.lhs, self.rhs = terms
        elif isinstance(terms, Mapping):
            self.rhs, self.lhs = terms.items()[0]
        else:
            Log.error("logic error")

    def to_ruby(self, not_null=False, boolean=False):
        lhs = self.lhs.to_ruby()
        rhs = self.rhs.to_ruby()
        return "((" + lhs + ")!=null) && ((" + rhs + ")!=null) && ((" + lhs + ")!=(" + rhs + "))"

    def to_python(self, not_null=False, boolean=False):
        lhs = self.lhs.to_python()
        rhs = self.rhs.to_python()
        return "((" + lhs + ") != None and (" + rhs + ") != None and (" + lhs + ") != (" + rhs + "))"

    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = self.lhs.to_sql(schema)[0].sql
        rhs = self.rhs.to_sql(schema)[0].sql
        acc = []
        for t in "bsnj":
            if lhs[t] and rhs[t]:
                acc.append("(" + lhs[t] + ") = (" + rhs[t] + ")")
        if not acc:
            return FalseOp().to_sql(schema)
        else:
            return wrap([{"name": ".", "sql": {"b": "NOT (" + " OR ".join(acc) + ")"}}])

    def to_esfilter(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"not": {"term": {self.lhs.var: self.rhs.to_esfilter()}}}
        else:
            return {"and": [
                {"and": [{"exists": {"field": v}} for v in self.vars()]},
                {"script": {"script": self.to_ruby()}}
            ]}

    def __data__(self):
        if isinstance(self.lhs, Variable) and isinstance(self.rhs, Literal):
            return {"ne": {self.lhs.var, json2value(self.rhs.json)}}
        else:
            return {"ne": [self.lhs.__data__(), self.rhs.__data__()]}

    def vars(self):
        return self.lhs.vars() | self.rhs.vars()

    def map(self, map_):
        return NeOp("ne", [self.lhs.map(map_), self.rhs.map(map_)])

    def missing(self):
        return OrOp("or", [self.lhs.missing(), self.rhs.missing()])


class NotOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.term = term

    def to_ruby(self, not_null=False, boolean=False):
        return "!(" + self.term.to_ruby() + ")"

    def to_python(self, not_null=False, boolean=False):
        return "not (" + self.term.to_python() + " == None)"

    def to_sql(self, schema, not_null=False, boolean=False):
        sql = self.term.to_sql(schema)[0].sql
        return wrap([{"name": ".", "sql": {
            "0": "1",
            "b": "NOT (" + sql.b + ")",
            "n": "(" + sql.n + " IS NULL)",
            "s": "(" + sql.s + " IS NULL)"
        }}])

    def vars(self):
        return self.term.vars()

    def to_esfilter(self):
        operand = self.term.to_esfilter()
        if operand.get("script"):
            return {"script": {"script": "!(" + operand.get("script", {}).get("script") + ")"}}
        else:
            return {"not": operand}

    def __data__(self):
        return {"not": self.term.__data__()}

    def map(self, map_):
        return NotOp("not", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class AndOp(Expression):
    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def to_ruby(self, not_null=False, boolean=False):
        if not self.terms:
            return "true"
        else:
            return " && ".join("(" + t.to_ruby() + ")" for t in self.terms)

    def to_python(self, not_null=False, boolean=False):
        if not self.terms:
            return "True"
        else:
            return " and ".join("(" + t.to_python() + ")" for t in self.terms)

    def to_sql(self, schema, not_null=False, boolean=False):
        if not self.terms:
            return wrap([{"name":".", "sql": {"b": "1"}}])
        elif all(self.terms):
            return wrap([{"name":".", "sql": {"b": " AND ".join("(" + t.to_sql(schema, boolean=True)[0].sql.b + ")" for t in self.terms)}}])
        else:
            return wrap([{"name":".", "sql": {"b": "0"}}])

    def to_esfilter(self):
        if not len(self.terms):
            return {"match_all": {}}
        else:
            return {"bool": {"must": [t.to_esfilter() for t in self.terms]}}

    def __data__(self):
        return {"and": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return AndOp("and", [t.map(map_) for t in self.terms])

    def missing(self):
        return False


class OrOp(Expression):
    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        self.terms = terms

    def to_ruby(self, not_null=False, boolean=False):
        return " || ".join("(" + t.to_ruby(boolean=True) + ")" for t in self.terms if t)

    def to_python(self, not_null=False, boolean=False):
        return " or ".join("(" + t.to_python() + ")" for t in self.terms)

    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap([{"name":".", "sql":{"b": " OR ".join("(" + t.to_sql(schema, boolean=True)[0].sql.b + ")" for t in self.terms)}}])

    def to_esfilter(self):
        return {"or": [t.to_esfilter() for t in self.terms]}

    def __data__(self):
        return {"or": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return OrOp("or", [t.map(map_) for t in self.terms])

    def missing(self):
        return False

    def __call__(self, row=None, rownum=None, rows=None):
        return any(t(row, rownum, rows) for t in self.terms)


class LengthOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.term = term

    def to_ruby(self, not_null=False, boolean=False):
        value = self.term.to_ruby(not_null=True)
        if not_null:
            return "(" + value + ").length()"
        else:
            missing = self.missing().to_ruby()
            return "(" + missing + " ) ? null : (" + value + ").length()"

    def to_python(self, not_null=False, boolean=False):
        value = self.term.to_python()
        return "len(" + value + ") if (" + value + ") != None else None"

    def to_sql(self, schema, not_null=False, boolean=False):
        if isinstance(self.term, Literal):
            val = json2value(self.term)
            if isinstance(val, unicode):
                return wrap([{"name":".", "sql": {"n": convert.value2json(len(val))}}])
            elif isinstance(val, (float, int)):
                return wrap([{"name":".", "sql": {"n": convert.value2json(len(convert.value2json(val)))}}])
            else:
                return Null
        value = self.term.to_sql(schema)[0].sql.s
        return wrap([{"name":".", "sql": {"n": "LENGTH(" + value + ")"}}])

    def __data__(self):
        return {"length": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return LengthOp("length", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class NumberOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.term = term

    def to_ruby(self, not_null=False, boolean=False):
        test = self.term.missing().to_ruby(boolean=True)
        value = self.term.to_ruby(not_null=True)
        return "(" + test + ") ? null : (((" + value + ") instanceof String) ? Double.parseDouble(" + value + ") : (" + value + "))"

    def to_python(self, not_null=False, boolean=False):
        test = self.term.missing().to_python(boolean=True)
        value = self.term.to_python(not_null=True)
        return "float(" + value + ") if (" + test + ") else None"

    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.term.to_sql(schema, not_null=True)
        acc = []
        for c in value:
            for t, v in c.sql.items():
                if t == "s":
                    acc.append("CAST(" + v + " as FLOAT)")
                else:
                    acc.append(v)

        if not acc:
            return wrap([])
        else:
            return wrap([{"name": ".", "sql": {"n": "COALESCE(" + ",".join(acc) + ")"}}])

    def __data__(self):
        return {"number": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return NumberOp("number", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class StringOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.term = term

    def to_ruby(self, not_null=False, boolean=False):
        value = self.term.to_ruby(not_null=True)
        missing = self.term.missing().to_ruby()
        return "(" + missing + ") ? null : (((" + value + ") instanceof java.lang.Double) ? String.valueOf(" + value + ").replaceAll('\\\\.0$', '') : String.valueOf(" + value + "))"  #"\\.0$"

    def to_python(self, not_null=False, boolean=False):
        missing = self.term.missing().to_python(boolean=True)
        value = self.term.to_python(not_null=True)
        return "null if (" + missing + ") else unicode(" + value + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        test = self.term.missing().to_sql(schema, boolean=True)[0].sql.b
        value = self.term.to_sql(schema, not_null=True)[0].sql
        acc = []
        for t, v in value.items():
            if t == "b":
                acc.append("CASE WHEN (" + test + ") THEN NULL WHEN (" + v + ") THEN 'true' ELSE 'false' END")
            elif t == "s":
                acc.append(v)
            else:
                acc.append("CASE WHEN (" + test + ") THEN NULL ELSE RTRIM(RTRIM(CAST(" + v + " as TEXT), '0'), '.') END")
        if not acc:
            return wrap([{}])
        elif len(acc) == 1:
            return wrap([{"name": ".", "sql": {"s": acc[0]}}])
        else:
            return wrap([{"name": ".", "sql": {"s": "COALESCE(" + ",".join(acc) + ")"}}])

    def __data__(self):
        return {"string": self.term.__data__()}

    def vars(self):
        return self.term.vars()

    def map(self, map_):
        return StringOp("string", self.term.map(map_))

    def missing(self):
        return self.term.missing()


class CountOp(Expression):
    has_simple_form = False

    def __init__(self, op, terms, **clauses):
        Expression.__init__(self, op, terms)
        self.terms = terms

    def to_ruby(self, not_null=False, boolean=False):
        return "+".join("((" + t.missing().to_ruby(boolean=True) + ") ? 0 : 1)" for t in self.terms)

    def to_python(self, not_null=False, boolean=False):
        return "+".join("(0 if (" + t.missing().to_python(boolean=True) + ") else 1)" for t in self.terms)

    def to_sql(self, schema, not_null=False, boolean=False):
        acc = []
        for term in self.terms:
            sqls = term.to_sql(schema)
            if len(sqls)>1:
                acc.append("1")
            else:
                for t, v in sqls[0].sql.items():
                    if t in ["b", "s", "n"]:
                        acc.append("CASE WHEN (" + v + ") IS NULL THEN 0 ELSE 1 END")
                    else:
                        acc.append("1")

        if not acc:
            return wrap([{}])
        else:
            return wrap([{"nanme":".", "sql":{"n": "+".join(acc)}}])

    def __data__(self):
        return {"count": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return CountOp("count", [t.map(map_) for t in self.terms])

    def missing(self):
        return FalseOp

    def exists(self):
        return TrueOp


class MultiOp(Expression):
    has_simple_form = True

    operators = {
        "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
        "sum": (" + ", "0"),
        "mul": (" * ", "1"),
        "mult": (" * ", "1"),
        "multiply": (" * ", "1")
    }

    def __init__(self, op, terms, **clauses):
        Expression.__init__(self, op, terms)
        self.op = op
        self.terms = terms
        self.default = coalesce(clauses.get("default"), NullOp())
        self.nulls = coalesce(clauses.get("nulls"), FalseOp())

    def to_ruby(self, not_null=False, boolean=False):
        if self.nulls:
            op, unit = MultiOp.operators[self.op]
            null_test = CoalesceOp("coalesce", self.terms).missing().to_ruby(boolean=True)
            acc = op.join("((" + t.missing().to_ruby(boolean=True) + ") ? " + unit + " : (" + t.to_ruby(not_null=True) + "))" for t in self.terms)
            return "((" + null_test + ") ? (" + self.default.to_ruby() + ") : (" + acc + "))"
        else:
            op, unit = MultiOp.operators[self.op]
            null_test = OrOp("or", [t.missing() for t in self.terms]).to_ruby()
            acc = op.join("(" + t.to_ruby(not_null=True) + ")" for t in self.terms)
            return "((" + null_test + ") ? (" + self.default.to_ruby() + ") : (" + acc + "))"

    def to_python(self, not_null=False, boolean=False):
        return MultiOp.operators[self.op][0].join("(" + t.to_python() + ")" for t in self.terms)

    def to_sql(self, schema, not_null=False, boolean=False):
        terms = [t.to_sql(schema) for t in self.terms]
        default = coalesce(self.default.to_sql(schema)[0].sql.n, "NULL")

        op, identity = MultiOp.operators[self.op]
        sql_terms = []
        for t in terms:
            if len(t) > 1:
                return wrap([{"name": ".", "sql": {"0": "NULL"}}])
            sql = t[0].sql.n
            if not sql:
                return wrap([{"name": ".", "sql": {"0": "NULL"}}])
            sql_terms.append(sql)

        if self.nulls.json=="true":
            sql = (
                " CASE " +
                " WHEN " + " AND ".join("(" + s + " IS NULL)" for s in sql_terms) +
                " THEN " + default +
                " ELSE " + op.join("COALESCE(" + s + ", 0)" for s in sql_terms) +
                " END"
            )
            return wrap([{"name": ".", "sql": {"n": sql}}])
        else:
            sql = (
                " CASE " +
                " WHEN " + " OR ".join("(" + s + " IS NULL)" for s in sql_terms) +
                " THEN " + default +
                " ELSE " + op.join("(" + s + ")" for s in sql_terms) +
                " END"
            )
            return wrap([{"name": ".", "sql": {"n": sql}}])

    def __data__(self):
        return {self.op: [t.__data__() for t in self.terms], "default": self.default, "nulls": self.nulls}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return MultiOp(self.op, [t.map(map_) for t in self.terms], **{"default": self.default, "nulls": self.nulls})

    def missing(self):
        if self.nulls:
            if self.default == None:
                return AndOp("and", [t.missing() for t in self.terms])
            else:
                return FalseOp
        else:
            if self.default == None:
                return OrOp("or", [t.missing() for t in self.terms])
            else:
                return FalseOp

    def exists(self):
        if self.nulls:
            return OrOp("or", [t.exists() for t in self.terms])
        else:
            return AndOp("and", [t.exists() for t in self.terms])


class RegExpOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.var, self.pattern = term

    def to_python(self, not_null=False, boolean=False):
        return "re.match(" + quote(json2value(self.pattern.json) + "$") + ", " + self.var.to_python() + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        pattern = schema.db.quote_value(convert.json2value(self.pattern.json))
        value = self.var.to_sql(schema)[0].sql.s
        return wrap([
            {"name": ".", "sql": {"s": value + " REGEXP " + pattern}}
        ])

    def to_esfilter(self):
        return {"regexp": {self.var.var: json2value(self.pattern.json)}}

    def __data__(self):
        return {"regexp": {self.var.var: self.pattern}}

    def vars(self):
        return {self.var.var}

    def map(self, map_):
        return RegExpOp("regex", [self.var.map(map_), self.pattern])

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()

class CoalesceOp(Expression):
    has_simple_form = True

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        self.terms = terms

    def to_ruby(self, not_null=False, boolean=False):
        if not self.terms:
            return "null"
        acc = self.terms[-1].to_ruby()
        for v in reversed(self.terms[:-1]):
            r = v.to_ruby()
            acc = "(((" + r + ") != null) ? (" + r + ") : (" + acc + "))"
        return acc

    def to_python(self, not_null=False, boolean=False):
        return "coalesce(" + (",".join(t.to_python() for t in self.terms)) + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        acc = {
            "b": [],
            "s": [],
            "n": []
        }

        for term in self.terms:
            for t, v in term.to_sql(schema)[0].sql.items():
                acc[t].append(v)

        output = {}
        for t, terms in acc.items():
            if not terms:
                continue
            elif len(terms) == 1:
                output[t] = terms[0]
            else:
                output[t] = "COALESCE(" + ",".join(terms) + ")"
        return wrap([{"name": ".", "sql": output}])

    def to_esfilter(self):
        return {"or": [{"exists": {"field": v}} for v in self.terms]}

    def __data__(self):
        return {"coalesce": [t.__data__() for t in self.terms]}

    def missing(self):
        # RETURN true FOR RECORDS THE WOULD RETURN NULL
        return AndOp("and", [v.missing() for v in self.terms])

    def vars(self):
        output = set()
        for v in self.terms:
            output |= v.vars()
        return output

    def map(self, map_):
        return CoalesceOp("coalesce", [v.map(map_) for v in self.terms])


class MissingOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.expr = term

    def to_ruby(self, not_null=False, boolean=True):
        if isinstance(self.expr, Variable):
            if self.expr.var == "_id":
                return "false"
            else:
                return "doc[" + quote(self.expr.var) + "].isEmpty()"
        elif isinstance(self.expr, Literal):
            return self.expr.missing().to_ruby()
        else:
            return self.expr.missing().to_ruby()

    def to_python(self, not_null=False, boolean=False):
        return self.expr.to_python() + " == None"

    def to_sql(self, schema, not_null=False, boolean=False):
        field = self.expr.to_sql(schema)

        if len(field) > 1:
            return wrap([{"name": ".", "sql": {"b": "0"}}])

        acc = []
        for c in field:
            for t, v in c.sql.items():
                if t == "b":
                    acc.append(v + " IS NULL")
                if t == "s":
                    acc.append("(" + v + " IS NULL OR " + v + "='')")
                if t == "n":
                    acc.append(v + " IS NULL")

        if not acc:
            return wrap([{"name": ".", "sql": {"b": "1"}}])
        else:
            return wrap([{"name": ".", "sql": {"b": " AND ".join(acc)}}])

    def to_esfilter(self):
        if isinstance(self.expr, Variable):
            return {"missing": {"field": self.expr.var}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def __data__(self):
        return {"missing": self.expr.var}

    def vars(self):
        return self.expr.vars()

    def map(self, map_):
        return MissingOp("missing", self.expr.map(map_))

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()


class ExistsOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.field = term

    def to_ruby(self, not_null=False, boolean=False):
        if isinstance(self.field, Variable):
            return "!doc["+quote(self.field.var)+"].isEmpty()"
        elif isinstance(self.field, Literal):
            return self.field.exists().to_ruby()
        else:
            return self.field.to_ruby() + " != null"

    def to_python(self, not_null=False, boolean=False):
        return self.field.to_python() + " != None"

    def to_sql(self, schema, not_null=False, boolean=False):
        field = self.field.to_sql(schema)[0].sql
        acc = []
        for t, v in field.items():
            if t in "bns":
                acc.append("(" + v + " IS NOT NULL)")

        if not acc:
            return wrap([{"name": ".", "sql": {"b": "0"}}])
        else:
            return wrap([{"name": ".", "sql": {"b":" OR ".join(acc)}}])

    def to_esfilter(self):
        if isinstance(self.field, Variable):
            return {"exists": {"field": self.field.var}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def __data__(self):
        return {"exists": self.field.__data__()}

    def vars(self):
        return self.field.vars()

    def map(self, map_):
        return ExistsOp("exists", self.field.map(map_))

    def missing(self):
        return FalseOp()

    def exists(self):
        return TrueOp()


class PrefixOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.field, self.prefix = term.items()[0]
        else:
            self.field, self.prefix = term

    def to_ruby(self, not_null=False, boolean=False):
        return "(" + self.field.to_ruby() + ").startsWith(" + self.prefix.to_ruby() + ")"

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.field.to_python() + ").startswith(" + self.prefix.to_python() + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        return {"b": "INSTR(" + self.field.to_sql(schema).s + ", " + self.prefix.to_sql().s + ")==1"}

    def to_esfilter(self):
        if isinstance(self.field, Variable) and isinstance(self.prefix, Literal):
            return {"prefix": {self.field.var: json2value(self.prefix.json)}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def __data__(self):
        if isinstance(self.field, Variable) and isinstance(self.prefix, Literal):
            return {"prefix": {self.field.var: json2value(self.prefix.json)}}
        else:
            return {"prefix": [self.field.__data__(), self.prefix.__data__()]}

    def vars(self):
        return {self.field.var}

    def map(self, map_):
        return PrefixOp("prefix", [self.field.map(map_), self.prefix.map(map_)])


class ConcatOp(Expression):
    has_simple_form = True

    def __init__(self, op, term, **clauses):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.terms = term.items()[0]
        else:
            self.terms = term
        self.separator = clauses.get("separator", Literal(None, ""))
        self.default = clauses.get("default", NullOp())
        if not isinstance(self.separator, Literal):
            Log.error("Expecting a literal separator")

    @classmethod
    def preprocess(cls, op, clauses):
        return clauses[op], {k: Literal(None, v) for k, v in clauses.items() if k in ["default", "separator"]}

    def to_ruby(self, not_null=False, boolean=False):
        if len(self.terms) == 0:
            return self.default.to_ruby()

        acc = []
        for t in self.terms:
            acc.append("((" + t.missing().to_ruby(boolean=True) + ") ? \"\" : (" + self.separator.json+"+"+t.to_ruby(not_null=True) + "))")
        expr_ = "("+"+".join(acc)+").substring("+unicode(len(json2value(self.separator.json)))+")"

        return "("+self.missing().to_ruby()+") ? ("+self.default.to_ruby()+") : ("+expr_+")"

    def to_sql(self, schema, not_null=False, boolean=False):
        defult = self.default.to_sql(schema)
        if len(self.terms) == 0:
            return defult
        defult = coalesce(defult[0].sql, "NULL")
        sep = self.separator.to_sql(schema)[0].sql.s

        acc = []
        for t in self.terms:
            missing = t.missing()

            term = t.to_sql(schema, not_null=True)[0].sql
            term_sql = coalesce(
                term.s,
                "cast(" + term.n + " as text)",
                "CASE WHEN " + term.b + " THEN `true` ELSE `false` END"
            )

            if isinstance(missing, TrueOp):
                acc.append("''")
            elif missing:
                acc.append("CASE WHEN (" + missing.to_sql(schema, boolean=True)[0].sql.b + ") THEN '' ELSE  ((" + sep +") || ("+term_sql + ")) END")
            else:
                acc.append("(" + sep + ") || (" + term_sql + ")")

        expr_ = "substr(" + " || ".join(acc) + ", " + LengthOp(None, self.separator).to_sql(schema)[0].sql.n + "+1)"

        missing = self.missing()
        if not missing:
            return wrap([{"name": ".", "sql": {"s": expr_}}])
        else:
            return wrap([{
                "name": ".",
                "sql": {
                    "s": "CASE WHEN (" + missing.to_sql(schema, boolean=True)[0].sql.b +
                         ") THEN (" + defult +
                         ") ELSE (" + expr_ +
                         ") END"
                }
            }])

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            output = {"concat": {self.terms[0].var: json2value(self.terms[2].json)}}
        else:
            output = {"concat": [t.__data__() for t in self.terms]}
        if self.separator.json != '""':
            output["separator"] = json2value(self.terms[2].json)
        return output

    def vars(self):
        if not self.terms:
            return set()
        return set.union(*(t.vars() for t in self.terms))

    def map(self, map_):
        return ConcatOp("concat", [t.map(map_) for t in self.terms], separator=self.separator)

    def missing(self):
        terms = [t.missing() for t in self.terms]
        if all(terms):
            return AndOp("and", terms)
        else:
            return FalseOp()


class UnixOp(Expression):
    """
    FOR USING ON DATABASES WHICH HAVE A DATE COLUMNS: CONVERT TO UNIX
    """
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.value = term

    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema)[0].sql
        return wrap([{
            "name": ".",
            "sql": {"n": "UNIX_TIMESTAMP("+v.n+")"}
        }])

    def vars(self):
        return self.value.vars()

    def map(self, map_):
        return UnixOp("map", self.value.map(map_))

    def missing(self):
        return self.value.missing()


class FromUnixOp(Expression):
    """
    FOR USING ON DATABASES WHICH HAVE A DATE COLUMNS: CONVERT TO UNIX
    """

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.value = term

    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema)[0].sql
        return wrap([{
            "name": ".",
            "sql": {"n": "FROM_UNIXTIME("+v.n+")"}
        }])

    def vars(self):
        return self.value.vars()

    def map(self, map_):
        return FromUnixOp("map", self.value.map(map_))

    def missing(self):
        return self.value.missing()


class LeftOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def to_ruby(self, not_null=False, boolean=False):
        test_v = self.value.missing()
        test_l = self.length.missing()
        v = self.value.to_ruby(not_null=True)
        l = self.length.to_ruby(not_null=True)

        if (not test_v or test_v.to_ruby(boolean=True)=="false") and not test_l:
            expr = v + ".substring(0, max(0, min(" + v + ".length(), " + l + ")).intValue())"
        else:
            expr = "((" + test_v.to_ruby(boolean=True) + ") || (" + test_l.to_ruby(boolean=True) + ")) ? null : (" + v + ".substring(0, max(0, min(" + v + ".length(), " + l + ")).intValue()))"
        return expr


    def to_python(self, not_null=False, boolean=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[0:max(0, " + l + ")]"

    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema)[0].sql.s
        l = self.length.to_sql(schema)[0].sql.n
        return wrap([{
            "name": ".",
            "sql": {"s": "substr(" + v + ", 1, " + l + ")"}
        }])

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"left": {self.value.var: json2value(self.length.json)}}
        else:
            return {"left": [self.value.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return LeftOp("left", [self.value.map(map_), self.length.map(map_)])

    def missing(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return MissingOp(None, self.value)
        else:
            return OrOp(None, [self.value.missing(), self.length.missing()])


class NotLeftOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def to_ruby(self, not_null=False, boolean=False):
        test_v = self.value.missing().to_ruby(boolean=True)
        test_l = self.length.missing().to_ruby(boolean=True)
        v = self.value.to_ruby(not_null=True)
        l = self.length.to_ruby(not_null=True)

        expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(max(0, min(" + v + ".length(), " + l + ")).intValue()))"
        return expr

    def to_python(self, not_null=False, boolean=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[max(0, " + l + "):]"

    def to_sql(self, schema, not_null=False, boolean=False):
        # test_v = self.value.missing().to_sql(boolean=True)[0].sql.b
        # test_l = self.length.missing().to_sql(boolean=True)[0].sql.b
        v = self.value.to_sql(schema, not_null=True)[0].sql.s
        l = "max(0, "+self.length.to_sql(schema, not_null=True)[0].sql.n+")"

        expr = "substr(" + v + ", " + l + "+1)"
        return wrap([{"name": ".", "sql": {"s": expr}}])

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"not_left": {self.value.var: json2value(self.length.json)}}
        else:
            return {"not_left": [self.value.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return NotLeftOp(None, [self.value.map(map_), self.length.map(map_)])

    def missing(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return MissingOp(None, self.value)
        else:
            return OrOp(None, [self.value.missing(), self.length.missing()])


class RightOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def to_ruby(self, not_null=False, boolean=False):
        test_v = self.value.missing().to_ruby(boolean=True)
        test_l = self.length.missing().to_ruby(boolean=True)
        v = self.value.to_ruby(not_null=True)
        l = self.length.to_ruby(not_null=True)

        expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(min("+v+".length(), max(0, (" + v + ").length() - (" + l + "))).intValue()))"
        return expr

    def to_python(self, not_null=False, boolean=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[max(0, len(" + v + ")-(" + l + ")):]"

    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema, not_null=True)[0].sql.s
        r = self.length.to_sql(schema, not_null=True)[0].sql.n
        l = "max(0, length("+v+")-max(0, "+r+"))"
        expr = "substr(" + v + ", " + l + "+1)"
        return wrap([{"name": ".", "sql": {"s": expr}}])

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"right": {self.value.var: json2value(self.length.json)}}
        else:
            return {"right": [self.value.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return RightOp("right", [self.value.map(map_), self.length.map(map_)])

    def missing(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return MissingOp(None, self.value)
        else:
            return OrOp(None, [self.value.missing(), self.length.missing()])


class NotRightOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        if isinstance(term, Mapping):
            self.value, self.length = term.items()[0]
        else:
            self.value, self.length = term

    def to_ruby(self, not_null=False, boolean=False):
        test_v = self.value.missing().to_ruby(boolean=True)
        test_l = self.length.missing().to_ruby(boolean=True)
        v = self.value.to_ruby(not_null=True)
        l = self.length.to_ruby(not_null=True)

        expr = "((" + test_v + ") || (" + test_l + ")) ? null : (" + v + ".substring(0, min("+v+".length(), max(0, (" + v + ").length() - (" + l + "))).intValue()))"
        return expr

    def to_python(self, not_null=False, boolean=False):
        v = self.value.to_python()
        l = self.length.to_python()
        return "None if " + v + " == None or " + l + " == None else " + v + "[0:max(0, len(" + v + ")-(" + l + "))]"

    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema, not_null=True)[0].sql.s
        r = self.length.to_sql(schema, not_null=True)[0].sql.n
        l = "max(0, length("+v+")-max(0, "+r+"))"
        expr = "substr(" + v + ", 1, " + l + ")"
        return wrap([{"name": ".", "sql": {"s": expr}}])

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return {"not_right": {self.value.var: json2value(self.length.json)}}
        else:
            return {"not_right": [self.value.__data__(), self.length.__data__()]}

    def vars(self):
        return self.value.vars() | self.length.vars()

    def map(self, map_):
        return NotRightOp(None, [self.value.map(map_), self.length.map(map_)])

    def missing(self):
        if isinstance(self.value, Variable) and isinstance(self.length, Literal):
            return MissingOp(None, self.value)
        else:
            return OrOp(None, [self.value.missing(), self.length.missing()])


class FindOp(Expression):
    """
    RETURN true IF substring CAN BE FOUND IN var, ELSE RETURN false
    """
    has_simple_form = True

    def __init__(self, op, term, **kwargs):
        Expression.__init__(self, op, term)
        self.value, self.find = term
        self.default = kwargs.get("default", NullOp())
        self.start = kwargs.get("start", Literal(None, 0))

    def to_python(self, not_null=False, boolean=False):
        return "((" + quote(self.substring) + " in " + self.var.to_python() + ") if " + self.var.to_python() + "!=None else False)"

    def to_ruby(self, not_null=False, boolean=False):
        missing = self.missing()
        v = self.value.to_ruby(not_null=True)
        find = self.find.to_ruby(not_null=True)
        start = self.start.to_ruby(not_null=True)
        index = v + ".indexOf(" + find + ", " + start + ")"

        if not_null:
            no_index = index + "==-1"
        else:
            no_index = missing.to_ruby(boolean=True)

        expr = "(" + no_index + ") ? " + self.default.to_ruby() + " : " + index
        return expr

    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.value.to_sql(schema)[0].sql.s
        find = self.find.to_sql(schema)[0].sql.s
        start_index = self.start.to_sql(schema)[0].sql.n

        if boolean:
            if start_index == "0":
                return wrap([{"name": ".", "sql": {
                    "b": "INSTR(" + value + "," + find + ")"
                }}])
            else:
                return wrap([{"name": ".", "sql": {
                    "b": "INSTR(SUBSTR(" + value + "," + start_index + "+1)," + find + ")"
                }}])
        else:
            default = self.default.to_sql(schema, not_null=True)[0].sql.n if self.default else "NULL"
            test = self.to_sql(schema, boolean=True)[0].sql.b
            if start_index == "0":
                index = "INSTR(" + value + "," + find + ")-1"
            else:
                index = "INSTR(SUBSTR(" + value + "," + start_index + "+1)," + find + ")+" + start_index

            sql = "CASE WHEN (" + test + ") THEN (" + index + ") ELSE (" + default + ") END"
            return wrap([{"name": ".", "sql": {"n": sql}}])


    def to_esfilter(self):
        if isinstance(self.value, Variable) and isinstance(self.find, Literal):
            return {"regexp": {self.value.var: ".*" + convert.string2regexp(json2value(self.find.json)) + ".*"}}
        else:
            return {"script": {"script": self.to_ruby()}}

    def __data__(self):
        return {"contains": {self.var.var: self.substring}}

    def vars(self):
        return self.value.vars() | self.find.vars() | self.default.vars() | self.start.vars()

    def map(self, map_):
        return FindOp(
            "find",
            [self.value.map(map_), self.find.map(map_)],
            start=self.start.map(map_),
            default=self.default.map(map_)
        )

    def missing(self):
        v = self.value.to_ruby(not_null=True)
        find = self.find.to_ruby(not_null=True)
        index = v + ".indexOf(" + find + ", " + self.start.to_ruby() + ")"

        return AndOp("and", [
            self.default.missing(),
            OrOp("or", [
                self.value.missing(),
                self.find.missing(),
                EqOp("eq", [ScriptOp("script", index), Literal(None, -1)])
            ])
        ])

    def exists(self):
        return TrueOp()


class BetweenOp(Expression):

    def __init__(self, op, term, **clauses):
        Expression.__init__(self, op, term)
        self.value, self.prefix, self.suffix = term
        self.default = coalesce(clauses["default"], NullOp())
        self.start = coalesce(clauses["start"], NullOp())
        if isinstance(self.prefix, Literal) and isinstance(self.suffix, Literal):
            pass
        else:
            Log.error("Exepcting literal prefix and sufix only")

    @classmethod
    def preprocess(cls, op, clauses):
        param = clauses["between"]
        if isinstance(param, list):
            param = param
        elif isinstance(param, Mapping):
            var, vals = param.items()[0]
            if isinstance(vals, list) and len(vals)==2:
                param = [var, {"literal":vals[0]}, {"literal":vals[1]}]
            else:
                Log.error("`between` parameters are expected to be in {var: [prefix, suffix]} form")
        else:
            Log.error("`between` parameters are expected to be in {var: [prefix, suffix]} form")

        return param, {
            "default": clauses["default"],
            "start": clauses["start"]
        }

    def to_ruby(self, not_null=False, boolean=False):
        if isinstance(self.prefix, Literal) and isinstance(json2value(self.prefix.json), int):
            value_is_missing = self.value.missing().to_ruby()
            value = self.value.to_ruby(not_null=True)
            start = "max("+self.prefix.json+", 0)"

            if isinstance(self.suffix, Literal) and isinstance(json2value(self.suffix.json), int):
                check = "(" + value_is_missing + ")"
                end = "min(" + self.suffix.to_ruby() + ", " + value + ".length())"
            else:
                end = value + ".indexOf(" + self.suffix.to_ruby() + ", " + start + ")"
                check = "((" + value_is_missing + ") || ("+end+"==-1))"

            expr = check + " ? " + self.default.to_ruby() + " : ((" + value + ").substring(" + start + ", " + end + "))"
            return expr

        else:
            #((Runnable)(() -> {int a=2; int b=3; System.out.println(a+b);})).run();
            value_is_missing = self.value.missing().to_ruby()
            value = self.value.to_ruby(not_null=True)
            prefix = self.prefix.to_ruby()
            len_prefix = unicode(len(json2value(self.prefix.json))) if isinstance(self.prefix, Literal) else "("+prefix+").length()"
            suffix = self.suffix.to_ruby()
            start_index = self.start.to_ruby()
            if start_index == "null":
                if prefix == "null":
                    start = "0"
                else:
                    start = value+".indexOf("+prefix+")"
            else:
                start = value+".indexOf("+prefix+", "+start_index+")"

            if suffix=="null":
                expr = "((" + value_is_missing + ") || (" + start + "==-1)) ? "+self.default.to_ruby()+" : ((" + value + ").substring(" + start + "+" + len_prefix + "))"
            else:
                end = value+".indexOf("+suffix+", "+start+"+"+len_prefix+")"
                expr = "((" + value_is_missing + ") || (" + start + "==-1) || ("+end+"==-1)) ? "+self.default.to_ruby()+" : ((" + value + ").substring(" + start + "+" + len_prefix + ", " + end + "))"

            return expr

    def to_sql(self, schema, not_null=False, boolean=False):
        if isinstance(self.prefix, Literal) and isinstance(convert.json2value(self.prefix.json), int):
            value_is_missing = self.value.missing().to_sql(schema, boolean=True)[0].sql.b
            value = self.value.to_sql(schema, not_null=True)[0].sql.s
            prefix = "max(0, " + self.prefix.to_sql(schema)[0].sql.n + ")"
            suffix = self.suffix.to_sql(schema)[0].sql.n
            start_index = self.start.to_sql(schema)[0].sql.n
            default = self.default.to_sql(schema, not_null=True).sql.s if self.default else "NULL"

            if start_index:
                start = prefix + "+" + start_index + "+1"
            else:
                if prefix:
                    start = prefix + "+1"
                else:
                    start = "1"

            if suffix:
                length = ","+suffix + "-" + prefix
            else:
                length = ""

            expr = (
                "CASE WHEN (" + value_is_missing + ")" +
                " THEN " + default +
                " ELSE substr(" + value + ", " + start + length + ")" +
                " END"
            )
            return wrap([{"name": ".", "sql": {"s": expr}}])
        else:
            value_is_missing = self.value.missing().to_sql(schema, boolean=True)[0].sql.b
            value = self.value.to_sql(schema, not_null=True)[0].sql.s
            prefix = self.prefix.to_sql(schema)[0].sql.s
            len_prefix = unicode(len(convert.json2value(self.prefix.json))) if isinstance(self.prefix, Literal) else "length(" + prefix + ")"
            suffix = self.suffix.to_sql(schema)[0].sql.s
            start_index = self.start.to_sql(schema)[0].sql.n
            default = self.default.to_sql(schema, not_null=True).sql.s if self.default else "NULL"

            if start_index:
                start = "instr(substr(" + value + ", " + start_index + "+1), " + prefix + ")+" + len_prefix
            else:
                if prefix:
                    start = "instr(" + value + ", " + prefix + ") + " + len_prefix
                else:
                    start = "1"

            if suffix:
                end = "instr(substr(" + value + "," + start + "), " + suffix + ")"
                length = "(" + end + "-1)"

                expr = (
                    " CASE WHEN (" + value_is_missing + ") OR NOT (" + start + ") OR NOT (" + end + ")" +
                    " THEN " + default +
                    " ELSE substr(" + value + ", " + start + ", " + length + ")" +
                    " END"
                )

            else:
                expr = (
                    "CASE WHEN (" + value_is_missing + ") OR NOT (" + start + ")" +
                    " THEN " + default +
                    " ELSE substr(" + value + ", " + start  + ")" +
                    " END"
                )

            return wrap([{"name": ".", "sql": {"s": expr}}])


    def vars(self):
        return self.value.vars() | self.prefix.vars() | self.suffix.vars() | self.default.vars() | self.start.vars()

    def map(self, map_):
        return BetweenOp(
            "between",
            [self.value.map(map_), self.prefix.map(map_), self.suffix.map(map_)],
            default=self.default.map(map_),
            start=self.start.map(map_)
        )

    def missing(self):
        value = self.value.to_ruby(not_null=True)
        prefix = self.prefix.to_ruby()
        len_prefix = "("+prefix+").length()"
        suffix = self.suffix.to_ruby()
        start = value+".indexOf("+prefix+")"
        end = value+".indexOf("+suffix+", "+start+"+"+len_prefix+")"

        expr = OrOp("or", [
            self.value.missing(),
            ScriptOp("script", start + "==-1"),
            ScriptOp("script", end + "==-1")
        ])
        return expr



class InOp(Expression):
    has_simple_form = True

    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.field, self.values = term

    def to_ruby(self, not_null=False, boolean=False):
        return self.values.to_ruby() + ".contains(" + self.field.to_ruby() + ")"

    def to_python(self, not_null=False, boolean=False):
        return self.field.to_python() + " in " + self.values.to_python()

    def to_sql(self, schema, not_null=False, boolean=False):
        if not isinstance(self.values, Literal):
            Log.error("Not supported")
        var = self.field.to_sql(schema)
        return " OR ".join("(" + var + "==" + sql_quote(v) + ")" for v in json2value(self.values))

    def to_esfilter(self):
        if isinstance(self.field, Variable):
            return {"terms": {self.field.var: json2value(self.values.json)}}
        else:
            return {"script": self.to_ruby()}

    def __data__(self):
        if isinstance(self.field, Variable) and isinstance(self.values, Literal):
            return {"in": {self.field.var: json2value(self.values.json)}}
        else:
            return {"in": [self.field.__data__(), self.values.__data__()]}

    def vars(self):
        return self.field.vars()

    def map(self, map_):
        return InOp("in", [self.field.map(map_), self.values])


class RangeOp(Expression):
    has_simple_form = True

    def __new__(cls, op, term, *args):
        Expression.__new__(cls, *args)
        field, comparisons = term  # comparisons IS A Literal()
        return AndOp("and", [operators[op](op, [field, Literal(None, value)]) for op, value in json2value(comparisons.json).items()])

    def __init__(self, op, term):
        Log.error("Should never happen!")


class WhenOp(Expression):
    def __init__(self, op, term, **clauses):
        Expression.__init__(self, op, [term])

        self.when = term
        self.then = coalesce(clauses.get("then"), NullOp())
        self.els_ = coalesce(clauses.get("else"), NullOp())

    def to_ruby(self, not_null=False, boolean=False):
        return "(" + self.when.to_ruby(boolean=True) + ") ? (" + self.then.to_ruby(not_null=not_null) + ") : (" + self.els_.to_ruby(not_null=not_null) + ")"

    def to_python(self, not_null=False, boolean=False):
        return "(" + self.when.to_python(boolean=True) + ") ? (" + self.then.to_python(not_null=not_null) + ") : (" + self.els_.to_python(not_null=not_null) + ")"

    def to_sql(self, schema, not_null=False, boolean=False):
        when = self.when.to_sql(schema, boolean=True)[0].sql
        then = self.then.to_sql(schema, not_null=not_null)[0].sql
        els_ = self.els_.to_sql(schema, not_null=not_null)[0].sql
        output = {}
        for t in "bsn":
            if then[t] == None:
                if els_[t] == None:
                    pass
                else:
                    output[t] = "CASE WHEN " + when.b + " THEN NULL ELSE " + els_[t] + " END"
            else:
                if els_[t] == None:
                    output[t] = "CASE WHEN " + when.b + " THEN " + then[t] + " END"
                else:
                    output[t] = "CASE WHEN " + when.b + " THEN " + then[t] + " ELSE " + els_[t] + " END"
        if not output:
            return wrap([{"name": ".", "sql": {"0": "NULL"}}])
        else:
            return wrap([{"name": ".", "sql": output}])

    def to_esfilter(self):
        return {"or": [
            {"and": [
                self.when.to_esfilter(),
                self.then.to_esfilter()
            ]},
            {"and": [
                {"not": self.when.to_esfilter()},
                self.els_.to_esfilter()
            ]}
        ]}
        # return {"script": {"script": self.to_ruby()}}

    def __data__(self):
        return {"when": self.when.__data__(), "then": self.then.__data__() if self.then else None, "else": self.els_.__data__() if self.els_ else None}

    def vars(self):
        return self.when.vars() | self.then.vars() | self.els_.vars()

    def map(self, map_):
        return WhenOp("when", self.when.map(map_), **{"then": self.then.map(map_), "else": self.els_.map(map_)})

    def missing(self):
        if self.then.missing() or self.els_.missing():
            return WhenOp("when", self.when, **{"then": self.then.missing(), "else": self.els_.missing()})
        else:
            return FalseOp()


class CaseOp(Expression):
    def __init__(self, op, term, **clauses):
        if not isinstance(term, (list, tuple)):
            Log.error("case expression requires a list of `when` sub-clauses")
        Expression.__init__(self, op, term)
        if len(term) == 0:
            self.whens = [NullOp()]
        else:
            for w in term[:-1]:
                if not isinstance(w, WhenOp) or w.els_:
                    Log.error("case expression does not allow `else` clause in `when` sub-clause")
            self.whens = term

    def to_ruby(self, not_null=False, boolean=False):
        acc = self.whens[-1].to_ruby()
        for w in reversed(self.whens[0:-1]):
            acc = "(" + w.when.to_ruby(boolean=True) + ") ? (" + w.then.to_ruby() + ") : (" + acc + ")"
        return acc

    def to_python(self, not_null=False, boolean=False):
        acc = self.whens[-1].to_python()
        for w in reversed(self.whens[0:-1]):
            acc = "(" + w.when.to_python(boolean=True) + ") ? (" + w.then.to_python() + ") : (" + acc + ")"
        return acc

    def to_sql(self, schema, not_null=False, boolean=False):
        output = {}
        for t in "bsn":  # EXPENSIVE LOOP to_sql() RUN 3 TIMES
            acc = " ELSE " + self.whens[-1].to_sql(schema)[t] + " END"
            for w in reversed(self.whens[0:-1]):
                acc = " WHEN " + w.when.to_sql(boolean=True).b + " THEN " + w.then.to_sql(schema)[t] + acc
            output[t]="CASE" + acc
        return output

    def to_esfilter(self):
        return {"script": {"script": self.to_ruby()}}

    def __data__(self):
        return {"case": [w.__data__() for w in self.whens]}

    def vars(self):
        output = set()
        for w in self.whens:
            output |= w.vars()
        return output

    def map(self, map_):
        return CaseOp("case", [w.map(map_) for w in self.whens])

    def missing(self):
        return MissingOp("missing", self)


USE_BOOL_MUST = True

def simplify_esfilter(esfilter):
    try:
        output = normalize_esfilter(esfilter)
        if output is TRUE_FILTER:
            return {"match_all": {}}
        elif output is FALSE_FILTER:
            return {"not": {"match_all": {}}}

        output.isNormal = None
        return output
    except Exception as e:
        from mo_logs import Log

        Log.unexpected("programmer error", cause=e)


def removeOr(esfilter):
    if esfilter["not"]:
        return {"not": removeOr(esfilter["not"])}

    if esfilter["and"]:
        return {"and": [removeOr(v) for v in esfilter["and"]]}

    if esfilter["or"]:  # CONVERT OR TO NOT.AND.NOT
        return {"not": {"and": [{"not": removeOr(v)} for v in esfilter["or"]]}}

    return esfilter


def normalize_esfilter(esfilter):
    """
    SIMPLFY THE LOGIC EXPRESSION
    """
    return wrap(_normalize(wrap(esfilter)))


def _normalize(esfilter):
    """
    TODO: DO NOT USE Data, WE ARE SPENDING TOO MUCH TIME WRAPPING/UNWRAPPING
    REALLY, WE JUST COLLAPSE CASCADING `and` AND `or` FILTERS
    """
    if esfilter is TRUE_FILTER or esfilter is FALSE_FILTER or esfilter.isNormal:
        return esfilter

    # Log.note("from: " + convert.value2json(esfilter))
    isDiff = True

    while isDiff:
        isDiff = False

        if coalesce(esfilter["and"], esfilter.bool.must):
            terms = coalesce(esfilter["and"], esfilter.bool.must)
            # MERGE range FILTER WITH SAME FIELD
            for (i0, t0), (i1, t1) in itertools.product(enumerate(terms), enumerate(terms)):
                if i0 >= i1:
                    continue  # SAME, IGNORE
                with suppress_exception:
                    f0, tt0 = t0.range.items()[0]
                    f1, tt1 = t1.range.items()[0]
                    if f0 == f1:
                        set_default(terms[i0].range[literal_field(f1)], tt1)
                        terms[i1] = True

            output = []
            for a in terms:
                if isinstance(a, (list, set)):
                    from mo_logs import Log

                    Log.error("and clause is not allowed a list inside a list")
                a_ = normalize_esfilter(a)
                if a_ is not a:
                    isDiff = True
                a = a_
                if a == TRUE_FILTER:
                    isDiff = True
                    continue
                if a == FALSE_FILTER:
                    return FALSE_FILTER
                if coalesce(a.get("and"), a.bool.must):
                    isDiff = True
                    a.isNormal = None
                    output.extend(coalesce(a.get("and"), a.bool.must))
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return TRUE_FILTER
            elif len(output) == 1:
                # output[0].isNormal = True
                esfilter = output[0]
                break
            elif isDiff:
                if USE_BOOL_MUST:
                    esfilter = wrap({"bool": {"must": output}})
                else:
                    esfilter = wrap({"and": output})
            continue

        if esfilter["or"] != None:
            output = []
            for a in esfilter["or"]:
                a_ = _normalize(a)
                if a_ is not a:
                    isDiff = True
                a = a_

                if a == TRUE_FILTER:
                    return TRUE_FILTER
                if a == FALSE_FILTER:
                    isDiff = True
                    continue
                if a.get("or"):
                    a.isNormal = None
                    isDiff = True
                    output.extend(a["or"])
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return FALSE_FILTER
            elif len(output) == 1:
                esfilter = output[0]
                break
            elif isDiff:
                esfilter = wrap({"or": output})
            continue

        if esfilter.term != None:
            if esfilter.term.keys():
                esfilter.isNormal = True
                return esfilter
            else:
                return TRUE_FILTER

        if esfilter.terms != None:
            for k, v in esfilter.terms.items():
                if len(v) > 0:
                    if OR(vv == None for vv in v):
                        rest = [vv for vv in v if vv != None]
                        if len(rest) > 0:
                            return {
                                "or": [
                                    {"missing": {"field": k}},
                                    {"terms": {k: rest}}
                                ],
                                "isNormal": True
                            }
                        else:
                            return {
                                "missing": {"field": k},
                                "isNormal": True
                            }
                    else:
                        esfilter.isNormal = True
                        return esfilter
            return FALSE_FILTER

        if esfilter["not"] != None:
            _sub = esfilter["not"]
            sub = _normalize(_sub)
            if sub is FALSE_FILTER:
                return TRUE_FILTER
            elif sub is TRUE_FILTER:
                return FALSE_FILTER
            elif sub is not _sub:
                sub.isNormal = None
                return wrap({"not": sub, "isNormal": True})
            else:
                sub.isNormal = None

    esfilter.isNormal = True
    return esfilter


def split_expression_by_depth(where, schema, map_=None, output=None, var_to_depth=None):
    """
    :param where: EXPRESSION TO INSPECT
    :param schema: THE SCHEMA
    :param map_: THE VARIABLE NAME MAPPING TO PERFORM ON where
    :param output:
    :param var_to_depth: MAP FROM EACH VARIABLE NAME TO THE DEPTH
    :return:
    """
    """
    It is unfortunate that ES can not handle expressions that
    span nested indexes.  This will split your where clause
    returning {"and": [filter_depth0, filter_depth1, ...]}
    """
    vars_ = where.vars()
    if not map_:
        map_ = {v: schema[v][0].es_column for v in vars_}

    if var_to_depth is None:
        if not vars_:
            return Null
        # MAP VARIABLE NAMES TO HOW DEEP THEY ARE
        var_to_depth = {v: len(c.nested_path)-1 for v in vars_ for c in schema[v]}
        all_depths = set(var_to_depth.values())
        if -1 in all_depths:
            Log.error(
                "Can not find column with name {{column|quote}}",
                column=unwraplist([k for k, v in var_to_depth.items() if v == -1])
            )
        if len(all_depths)==0:
            all_depths = {0}
        output = wrap([[] for _ in range(MAX(all_depths) + 1)])
    else:
        all_depths = set(var_to_depth[v] for v in vars_)

    if len(all_depths) == 1:
        output[list(all_depths)[0]] += [where.map(map_)]
    elif isinstance(where, AndOp):
        for a in where.terms:
            split_expression_by_depth(a, schema, map_, output, var_to_depth)
    else:
        Log.error("Can not handle complex where clause")

    return output


operators = {
    "add": MultiOp,
    "and": AndOp,
    "between": BetweenOp,
    "case": CaseOp,
    "coalesce": CoalesceOp,
    "concat": ConcatOp,
    "count": CountOp,
    "date": DateOp,
    "div": DivOp,
    "divide": DivOp,
    "eq": EqOp,
    "exists": ExistsOp,
    "exp": BinaryOp,
    "find": FindOp,
    "floor": FloorOp,
    "from_unix": FromUnixOp,
    "get": GetOp,
    "gt": InequalityOp,
    "gte": InequalityOp,
    "in": InOp,
    "instr": FindOp,
    "left": LeftOp,
    "length": LengthOp,
    "literal": Literal,
    "lt": InequalityOp,
    "lte": InequalityOp,
    "match_all": TrueOp,
    "minus": BinaryOp,
    "missing": MissingOp,
    "mod": BinaryOp,
    "mul": MultiOp,
    "mult": MultiOp,
    "multiply": MultiOp,
    "ne": NeOp,
    "neq": NeOp,
    "not": NotOp,
    "not_left": NotLeftOp,
    "not_right": NotRightOp,
    "null": NullOp,
    "number": NumberOp,
    "offset": OffsetOp,
    "or": OrOp,
    "prefix": PrefixOp,
    "range": RangeOp,
    "regex": RegExpOp,
    "regexp": RegExpOp,
    "right": RightOp,
    "rows": RowsOp,
    "script": ScriptOp,
    "string": StringOp,
    "sub": BinaryOp,
    "subtract": BinaryOp,
    "sum": MultiOp,
    "term": EqOp,
    "terms": InOp,
    "tuple": TupleOp,
    "unix": UnixOp,
    "when": WhenOp,
}


def sql_quote(value):
    if value == Null:
        return "NULL"
    elif value is True:
        return "0"
    elif value is False:
        return "1"
    elif isinstance(value, unicode):
        return "'" + value.replace("'", "''") + "'"
    else:
        return unicode(value)


json_type_to_sql_type = {
    "null": "0",
    "boolean": "b",
    "number": "n",
    "string": "s",
    "object": "j",
    "nested": "j"
}

sql_type_to_json_type = {
    "0": "null",
    "b": "boolean",
    "n": "number",
    "s": "string",
    "j": "object"
}


