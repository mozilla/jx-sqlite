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

import operator
from collections import Mapping
from decimal import Decimal

from future.utils import text_type
from mo_dots import coalesce, wrap, Null, split_field
from mo_json import json2value, value2json
from mo_logs import Log
from mo_math import Math, MAX

from jx_base.queries import is_variable_name, get_property_name
from mo_times.dates import Date

ALLOW_SCRIPTING = False
TRUE_FILTER = True
FALSE_FILTER = False
EMPTY_DICT = {}


def jx_expression(expr):
    """
    WRAP A JSON EXPRESSION WITH OBJECT REPRESENTATION
    """
    if isinstance(expr, Expression):
        Log.error("Expecting JSON, not expression")

    if expr in (True, False, None) or expr == None or isinstance(expr, (float, int, Decimal, Date)):
        return Literal(None, expr)
    elif isinstance(expr, text_type):
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


class Expression(object):
    has_simple_form = False

    def __init__(self, op, terms):
        self.simplified = False
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

    def partial_eval(self):
        """
        ATTEMPT TO SIMPLIFY THE EXPRESSION:
        PREFERABLY RETURNING A LITERAL, BUT MAYBE A SIMPLER EXPRESSION, OR self IF NOT POSSIBLE
        """
        self.simplified = True
        return self


class Variable(Expression):

    def __init__(self, var):
        Expression.__init__(self, "", None)
        if not is_variable_name(var):
            Log.error("Expecting a variable name")
        self.var = get_property_name(var)

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
        return text_type(self.var)

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
            self._json = '""'
        else:
            self._json = value2json(term)

    def __nonzero__(self):
        return True

    def __eq__(self, other):
        if other == None:
            if self._json == "null":
                return True
            else:
                return False
        elif self._json == "null":
            return False

        Log.warning("expensive")

        from mo_testing.fuzzytestcase import assertAlmostEqual

        try:
            assertAlmostEqual(json2value(self._json), other)
            return True
        except Exception:
            return False

    def __data__(self):
        return {"literal": json2value(self._json)}

    @property
    def value(self):
        return json2value(self._json)

    @property
    def json(self):
        return self._json

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def missing(self):
        if self._json == '""':
            return TrueOp()
        return FalseOp()

    def __call__(self, row=None, rownum=None, rows=None):
        return json2value(self.json)

    def __unicode__(self):
        return self._json

    def __str__(self):
        return str(self._json)


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

    def __data__(self):
        return {"date": self.value}

    def __call__(self, row=None, rownum=None, rows=None):
        return Date(self.value)

    def __unicode__(self):
        return self._json

    def __str__(self):
        return str(self._json)


class TupleOp(Expression):

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

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

    def partial_eval(self):
        lhs = self.lhs.partial_eval()
        rhs = self.rhs.partial_eval()

        if isinstance(lhs, Literal) and isinstance(rhs, Literal):
            output = Literal(None, ops[self.op](lhs, rhs))
        else:
            output = InequalityOp(self.op, [lhs, rhs], default=self.default)

        output.simplified = True
        return output


class DivOp(Expression):
    has_simple_form = True

    def __init__(self, op, terms, default=NullOp()):
        Expression.__init__(self, op, terms)
        self.lhs, self.rhs = terms
        self.default = default

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
            for lhs, rhs in items:
                if rhs.json.startswith("["):
                    acc.append(InOp("in", [Variable(lhs), rhs]))
                else:
                    acc.append(EqOp("eq", [Variable(lhs), rhs]))
            return AndOp("and", acc)

    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        self.op = op
        self.lhs, self.rhs = terms

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

    def partial_eval(self):
        lhs = self.lhs.partial_eval()
        rhs = self.rhs.partial_eval()

        if isinstance(lhs, Literal) and isinstance(rhs, Literal):
            output = Literal(None, ops["eq"](lhs, rhs))
        else:
            output = EqOp(self.op, [lhs, rhs])

        output.simplified = True
        return output


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

    def partial_eval(self):
        lhs = self.lhs.partial_eval()
        rhs = self.rhs.partial_eval()

        if isinstance(lhs, Literal) and isinstance(rhs, Literal):
            output = Literal(None, ops["ne"](lhs, rhs))
        else:
            output = NeOp("ne", [lhs, rhs])

        output.simplified = True
        return output


class NotOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, term)
        self.term = term

    def __data__(self):
        return {"not": self.term.__data__()}

    def vars(self):
            return self.term.vars()

    def map(self, map_):
        return NotOp("not", self.term.map(map_))

    def missing(self):
        return self.term.missing()

    def partial_eval(self):
        if self.simplified:
            return self

        term = self.term.partial_eval()
        if isinstance(term, TrueOp):
            return FalseOp()
        elif isinstance(term, FalseOp):
            return TrueOp()
        elif isinstance(term, NullOp):
            return term
        elif isinstance(term, Literal):
            Log.error("`not` operator expects a Boolean term")
        else:
            output = NotOp("not", term)
            output.simplified = True
            return output


class AndOp(Expression):
    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

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

    def partial_eval(self):
        if self.simplified:
            return self

        terms = []
        for t in self.terms:
            simple = t.partial_eval()
            if isinstance(simple, (TrueOp, NullOp)):
                pass
            elif isinstance(simple, FalseOp):
                return FalseOp()
            else:
                terms.append(simple)
        if len(terms) == 0:
            return TrueOp()
        output = AndOp("and", terms)
        output.simplified = True
        return output


class OrOp(Expression):
    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        self.terms = terms

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

    def partial_eval(self):
        if self.simplified:
            return self

        terms = []
        for t in self.terms:
            simple = t.partial_eval()
            if isinstance(simple, TrueOp):
                return TrueOp()
            elif isinstance(simple, (FalseOp, NullOp)):
                pass
            else:
                terms.append(simple)
        if len(terms) == 0:
            return FalseOp()
        output = OrOp("or", terms)
        output.simplified = True
        return output


class LengthOp(Expression):
    def __init__(self, op, term):
        Expression.__init__(self, op, [term])
        self.term = term

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


class MaxOp(Expression):
    def __init__(self, op, terms):
        Expression.__init__(self, op, terms)
        if terms == None:
            self.terms = []
        elif isinstance(terms, list):
            self.terms = terms
        else:
            self.terms = [terms]

    def __data__(self):
        return {"max": [t.__data__() for t in self.terms]}

    def vars(self):
        output = set()
        for t in self.terms:
            output |= t.vars()
        return output

    def map(self, map_):
        return MaxOp("max", [t.map(map_) for t in self.terms])

    def missing(self):
        return False

    def partial_eval(self):
        if self.simplified:
            return self

        maximum = None
        terms = []
        for t in self.terms:
            simple = t.partial_eval()
            if isinstance(simple, NullOp):
                pass
            elif isinstance(simple, Literal):
                maximum = MAX(maximum, simple.value)
            else:
                terms.append(simple)
        if len(terms) == 0:
            if maximum == None:
                return NullOp()
            else:
                return Literal(None, maximum)
        else:
            if maximum == None:
                output = MaxOp("max", terms)
            else:
                output = MaxOp("max", [Literal(None, maximum)] + terms)

        output.simplified = True
        return output


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

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.find, Literal):
            output = {
                "find": {self.value.var, self.find.value},
                "start":self.start.__data__()
            }
        else:
            output = {
                "find": [self.value.__data__(), self.find.__data__()],
                "start":self.start.__data__()
            }
        if self.default:
            output["default"]=self.default.__data__()
        return output

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


class SplitOp(Expression):
    """
    RETURN true IF substring CAN BE FOUND IN var, ELSE RETURN false
    """
    has_simple_form = True

    def __init__(self, op, term, **kwargs):
        Expression.__init__(self, op, term)
        self.value, self.find = term

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.find, Literal):
            return {"split": {self.value.var, self.find.value}}
        else:
            return {"split": [self.value.__data__(), self.find.__data__()]}

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
        self.value, self.superset = term

    def __data__(self):
        if isinstance(self.value, Variable) and isinstance(self.superset, Literal):
            return {"in": {self.value.var: json2value(self.superset.json)}}
        else:
            return {"in": [self.value.__data__(), self.superset.__data__()]}

    def vars(self):
        return self.value.vars()

    def map(self, map_):
        return InOp("in", [self.value.map(map_), self.superset])

    def partial_eval(self):
        if self.simplified:
            return self

        value = self.value.partial_eval()
        superset = self.superset.partial_eval()
        if isinstance(value, Literal) and isinstance(superset, Literal):
            return Literal(None, self())
        else:
            self.simplified = True
            return self

    def __call__(self):
        return self.value() in self.superset()


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

    def partial_eval(self):
        when = self.when.partial_eval()
        if isinstance(when, Literal):
            if isinstance(when, TrueOp):
                return self.then.partial_eval()
            elif isinstance(when, (FalseOp, NullOp)):
                return self.els_.partial_eval()
            else:
                Log.error("Expecting `when` clause to return a Boolean, or `null`")
        else:
            output = WhenOp("when", when, **{"then": self.then.partial_eval(), "else": self.els_.partial_eval()})
            output.simplified = True
            return self


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
    "max": MaxOp,
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
    "split": SplitOp,
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


def extend(cls):
    def extender(func):
        setattr(cls, func.func_name, func)
        return func
    return extender


ops={
    "ne": operator.ne,
    "eq": operator.eq,
    "gte": operator.ge,
    "gt": operator.gt,
    "lte": operator.le,
    "lt": operator.lt
}
