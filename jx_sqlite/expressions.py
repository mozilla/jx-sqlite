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

from future.utils import text_type
from mo_dots import coalesce, wrap, Null, split_field, startswith_field
from mo_dots import join_field, ROOT_PATH, relative_field, Data
from mo_json import json2value
from mo_logs import Log
from mo_math import Math
from pyLibrary import convert

from jx_base.queries import get_property_name
from jx_base.expressions import Variable, DateOp, TupleOp, LeavesOp, BinaryOp, OrOp, InequalityOp, extend, Literal, NullOp, TrueOp, FalseOp, DivOp, FloorOp, \
    NeOp, NotOp, LengthOp, NumberOp, StringOp, CountOp, MultiOp, RegExpOp, CoalesceOp, MissingOp, ExistsOp, \
    PrefixOp, UnixOp, FromUnixOp, NotLeftOp, RightOp, NotRightOp, FindOp, BetweenOp, InOp, RangeOp, CaseOp, AndOp, \
    ConcatOp, LeftOp, EqOp, WhenOp, BasicIndexOfOp, IntegerOp, MaxOp, BasicSubstringOp, BasicEqOp, FALSE, MinOp, BooleanOp, SuffixOp
from jx_base import STRUCT, OBJECT
from pyLibrary.sql import SQL, SQL_AND, SQL_EMPTY_STRING, SQL_OR, SQL_COMMA
from pyLibrary.sql.sqlite import quote_column, quote_value


@extend(Variable)
def to_sql(self, schema, not_null=False, boolean=False):
    cols = [Data({cname: c}) for cname, cs in schema.map_to_sql(self.var).items() for c in cs]
    if not cols:
        # DOES NOT EXIST
        return wrap([{"name": ".", "sql": {"0": "NULL"}, "nested_path": ROOT_PATH}])
    acc = {}
    if boolean:
        for col in cols:
            cname, col = col.items()[0]
            nested_path = col.nested_path[0]
            if col.type == OBJECT:
                value = "1"
            elif col.type == "boolean":
                value = quote_column(col.es_column)
            else:
                value = "(" + quote_column(col.es_column) + ") IS NOT NULL"
            tempa = acc.setdefault(nested_path, {})
            tempb = tempa.setdefault(get_property_name(cname), {})
            tempb['b'] = value
    else:
        for col in cols:
            cname, col = col.items()[0]
            if col.type == OBJECT:
                prefix = self.var + "."
                for cn, cs in schema.items():
                    if cn.startswith(prefix):
                        for child_col in cs:
                            tempa = acc.setdefault(child_col.nested_path[0], {})
                            tempb = tempa.setdefault(get_property_name(cname), {})
                            tempb[json_type_to_sql_type[col.type]] = quote_column(child_col.es_column)
            else:
                nested_path = col.nested_path[0]
                tempa = acc.setdefault(nested_path, {})
                tempb = tempa.setdefault(get_property_name(cname), {})
                tempb[json_type_to_sql_type[col.type]] = quote_column(col.es_column)

    return wrap([
        {"name": relative_field(cname, self.var), "sql": types, "nested_path": nested_path}
        for nested_path, pairs in acc.items() for cname, types in pairs.items()
    ])

@extend(Literal)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.value
    v = quote_value(value)
    if v == None:
        return wrap([{"name": "."}])
    elif isinstance(value, text_type):
        return wrap([{"name": ".", "sql": {"s": quote_value(value)}}])
    elif Math.is_number(v):
        return wrap([{"name": ".", "sql": {"n": quote_value(value)}}])
    elif v in [True, False]:
        return wrap([{"name": ".", "sql": {"b": quote_value(value)}}])
    else:
        return wrap([{"name": ".", "sql": {"j": quote_value(self.json)}}])


@extend(NullOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return Null


@extend(TrueOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"b": "1"}}])


@extend(FalseOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"b": "0"}}])


@extend(DateOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"n": quote_value(json2value(self.json))}}])


@extend(TupleOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": t.to_sql(schema)[0].sql} for t in self.terms])


@extend(LeavesOp)
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
        for n, cols in schema.map_to_sql(term).items()
        for c in cols
    ])


@extend(EqOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return self.partial_eval().to_sql(schema)


@extend(NeOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return NotOp("not", self).partial_eval().to_sql(schema)


@extend(BasicEqOp)
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
                    acc.append("(" + l.sql[t] + ") = (" + r.sql[t] + ")")
    if not acc:
        return FALSE.to_sql(schema)
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_OR.join(acc)}}])



@extend(BasicIndexOfOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.value.to_sql(schema)[0].sql.s
    find = self.find.to_sql(schema)[0].sql.s
    start = self.start

    if isinstance(start, Literal) and start.value == 0:
        return wrap([{"name": ".", "sql": {"n": "INSTR(" + value + "," + find + ")-1"}}])
    else:
        start_index = start.to_sql(schema)[0].sql.n
        return wrap([{"name": ".", "sql": {"n": "INSTR(SUBSTR(" + value + "," + start_index + "+1)," + find + ")+" + start_index + "-1"}}])


@extend(BasicSubstringOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.value.to_sql(schema)[0].sql.s
    start = MultiOp("add", [self.start, Literal(None, 1)]).partial_eval().to_sql(schema)[0].sql.n
    length = BinaryOp("subtract", [self.end, self.start]).partial_eval().to_sql(schema)[0].sql.n

    return wrap([{"name": ".", "sql": {"s": "SUBSTR(" + value + "," + start + ", " + length + ")"}}])


@extend(BinaryOp)
def to_sql(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.to_sql(schema)[0].sql.n
    rhs = self.rhs.to_sql(schema)[0].sql.n

    return wrap([{"name": ".", "sql": {"n": "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"}}])


@extend(MinOp)
def to_sql(self, schema, not_null=False, boolean=False):
    terms = [t.partial_eval().to_sql(schema)[0].sql.n for t in self.terms]
    return wrap([{"name": ".", "sql": {"n": "min(" + (SQL_COMMA.join(terms)) + ")"}}])


@extend(MaxOp)
def to_sql(self, schema, not_null=False, boolean=False):
    terms = [t.partial_eval().to_sql(schema)[0].sql.n for t in self.terms]
    return wrap([{"name": ".", "sql": {"n": "max(" + (SQL_COMMA.join(terms)) + ")"}}])





@extend(InequalityOp)
def to_sql(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.to_sql(schema, not_null=True)[0].sql
    rhs = self.rhs.to_sql(schema, not_null=True)[0].sql
    lhs_exists = self.lhs.exists().to_sql(schema)[0].sql
    rhs_exists = self.rhs.exists().to_sql(schema)[0].sql

    if len(lhs) == 1 and len(rhs) == 1:
        return wrap([{"name": ".", "sql": {
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
    sql = "(" + SQL(") OR (").join(ors) + ")"

    return wrap([{"name": ".", "sql": {"b": sql}}])


@extend(DivOp)
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


@extend(FloorOp)
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
                    acc.append("((" + l.sql[t] + ") = (" + r.sql[t] + ") OR ((" + l.sql[t] + ") IS NULL AND (" + r.sql[
                        t] + ") IS NULL))")
    if not acc:
        return FALSE.to_sql(schema)
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_OR.join(acc)}}])


# @extend(NeOp)
# def to_sql(self, schema, not_null=False, boolean=False):
#     return NotOp("not", EqOp("eq", [self.lhs, self.rhs])).to_sql(schema, not_null, boolean)


@extend(NotOp)
def to_sql(self, schema, not_null=False, boolean=False):
    not_expr = NotOp("not", BooleanOp("boolean", self.term)).partial_eval()
    if isinstance(not_expr, NotOp):
        return wrap([{"name": ".", "sql": {"b": "NOT (" + not_expr.term.to_sql(schema)[0].sql.b + ")"}}])
    else:
        return not_expr.to_sql(schema)


@extend(BooleanOp)
def to_sql(self, schema, not_null=False, boolean=False):
    sql = BooleanOp("boolean", self.term).partial_eval().to_sql(schema)[0].sql
    return wrap([{"name": ".", "sql": {
        "0": "1",
        "b": sql.b,
        "n": "(" + sql.n + ") IS NOT NULL",
        "s": "(" + sql.s + ") IS NOT NULL"
    }}])


@extend(AndOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not self.terms:
        return wrap([{"name": ".", "sql": {"b": "1"}}])
    elif all(self.terms):
        return wrap([{"name": ".", "sql": {
            "b": SQL_AND.join(["(" + t.to_sql(schema, boolean=True)[0].sql.b + ")" for t in self.terms])
        }}])
    else:
        return wrap([{"name": ".", "sql": {"b": "0"}}])


@extend(OrOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{
        "name": ".",
        "sql": {"b": SQL_OR.join(
            "(" + t.to_sql(schema, boolean=True)[0].sql.b + ")"
            for t in self.terms
        )}
    }])


@extend(LengthOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if isinstance(self.term, Literal):
        val = self.term.value
        if isinstance(val, text_type):
            return wrap([{"name": ".", "sql": {"n": convert.value2json(len(val))}}])
        elif isinstance(val, (float, int)):
            return wrap([{"name": ".", "sql": {"n": convert.value2json(len(convert.value2json(val)))}}])
        else:
            return Null
    value = self.term.to_sql(schema)[0].sql.s
    return wrap([{"name": ".", "sql": {"n": "LENGTH(" + value + ")"}}])


@extend(IntegerOp)
def to_sql(self, schema, not_null=False, boolean=False):
    value = self.term.to_sql(schema, not_null=True)
    acc = []
    for c in value:
        for t, v in c.sql.items():
            if t == "s":
                acc.append("CAST(" + v + " as INTEGER)")
            else:
                acc.append(v)

    if not acc:
        return wrap([])
    elif len(acc) == 1:
        return wrap([{"name": ".", "sql": {"n": acc[0]}}])
    else:
        return wrap([{"name": ".", "sql": {"n": "COALESCE(" + SQL_COMMA.join(acc) + ")"}}])



@extend(NumberOp)
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
    elif len(acc) == 1:
        return wrap([{"name": ".", "sql": {"n": acc}}])
    else:
        return wrap([{"name": ".", "sql": {"n": "COALESCE(" + SQL_COMMA.join(acc) + ")"}}])


@extend(StringOp)
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
        return wrap([{"name": ".", "sql": {"s": "COALESCE(" + SQL_COMMA.join(acc) + ")"}}])


@extend(CountOp)
def to_sql(self, schema, not_null=False, boolean=False):
    acc = []
    for term in self.terms:
        sqls = term.to_sql(schema)
        if len(sqls) > 1:
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
        return wrap([{"nanme": ".", "sql": {"n": SQL("+").join(acc)}}])


@extend(MultiOp)
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

    if self.nulls.json == "true":
        sql = (
            " CASE " +
            " WHEN " + SQL_AND.join("((" + s + ") IS NULL)" for s in sql_terms) +
            " THEN " + default +
            " ELSE " + op.join("COALESCE(" + s + ", 0)" for s in sql_terms) +
            " END"
        )
        return wrap([{"name": ".", "sql": {"n": sql}}])
    else:
        sql = (
            " CASE " +
            " WHEN " + SQL_OR.join("((" + s + ") IS NULL)" for s in sql_terms) +
            " THEN " + default +
            " ELSE " + op.join("(" + s + ")" for s in sql_terms) +
            " END"
        )
        return wrap([{"name": ".", "sql": {"n": sql}}])


@extend(RegExpOp)
def to_sql(self, schema, not_null=False, boolean=False):
    pattern = quote_value(convert.json2value(self.pattern.json))
    value = self.var.to_sql(schema)[0].sql.s
    return wrap([
        {"name": ".", "sql": {"b": value + " REGEXP " + pattern}}
    ])


@extend(CoalesceOp)
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
            output[t] = "COALESCE(" + SQL_COMMA.join(terms) + ")"
    return wrap([{"name": ".", "sql": output}])


@extend(MissingOp)
def to_sql(self, schema, not_null=False, boolean=False):
    field = self.expr.partial_eval().to_sql(schema)

    if len(field) > 1:
        return wrap([{"name": ".", "sql": {"b": "0"}}])

    acc = []
    for c in field:
        for t, v in c.sql.items():
            if t == "b":
                acc.append("(" + v + ") IS NULL")
            if t == "s":
                acc.append("((" + v + ") IS NULL OR " + v + "=" + SQL_EMPTY_STRING + ")")
            if t == "n":
                acc.append("(" + v + ") IS NULL")

    if not acc:
        return wrap([{"name": ".", "sql": {"b": "1"}}])
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_AND.join(acc)}}])


@extend(WhenOp)
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


@extend(ExistsOp)
def to_sql(self, schema, not_null=False, boolean=False):
    field = self.field.to_sql(schema)[0].sql
    acc = []
    for t, v in field.items():
        if t in "bns":
            acc.append("(" + v + " IS NOT NULL)")

    if not acc:
        return wrap([{"name": ".", "sql": {"b": "0"}}])
    else:
        return wrap([{"name": ".", "sql": {"b": SQL_OR.join(acc)}}])


@extend(PrefixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not self.field:
        return wrap([{"name": ".", "sql": {"b": "1"}}])
    else:
        return wrap([{"name": ".", "sql": {
            "b": "INSTR(" + self.field.to_sql(schema)[0].sql.s + ", " + self.prefix.to_sql(schema)[0].sql.s + ")==1"
        }}])


@extend(SuffixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not self.field:
        return wrap([{"name": ".", "sql": {"b": "1"}}])
    elif isinstance(self.suffix, Literal) and not self.suffix.value:
        return wrap([{"name": ".", "sql": {"b": "1"}}])
    else:
        return EqOp(
            "eq",
            [
                RightOp("right", [self.field, LengthOp("length", self.suffix)]),
                self.suffix
            ]
        ).partial_eval().to_sql(schema)


@extend(ConcatOp)
def to_sql(self, schema, not_null=False, boolean=False):
    defult = self.default.to_sql(schema)
    if len(self.terms) == 0:
        return defult
    defult = coalesce(defult[0].sql, "NULL")
    sep = self.separator.to_sql(schema)[0].sql.s

    acc = []
    for t in self.terms:
        missing = t.missing().partial_eval()

        term = t.to_sql(schema, not_null=True)[0].sql
        term_sql = coalesce(
            term.s,
            "cast(" + term.n + " as text)",
            "CASE WHEN " + term.b + " THEN `true` ELSE `false` END"
        )

        if isinstance(missing, TrueOp):
            acc.append(SQL_EMPTY_STRING)
        elif missing:
            acc.append(
                "CASE" +
                " WHEN (" + missing.to_sql(schema, boolean=True)[0].sql.b + ")" +
                " THEN " + SQL_EMPTY_STRING +
                " ELSE ((" + sep + ") || (" + term_sql + "))" +
                " END"
            )
        else:
            acc.append("(" + sep + ") || (" + term_sql + ")")

    expr_ = "substr(" + SQL(" || ").join(acc) + ", " + LengthOp(None, self.separator).to_sql(schema)[0].sql.n + "+1)"

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


@extend(UnixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema)[0].sql
    return wrap([{
        "name": ".",
        "sql": {"n": "UNIX_TIMESTAMP(" + v.n + ")"}
    }])


@extend(FromUnixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema)[0].sql
    return wrap([{
        "name": ".",
        "sql": {"n": "FROM_UNIXTIME(" + v.n + ")"}
    }])


@extend(LeftOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema)[0].sql.s
    l = self.length.to_sql(schema)[0].sql.n
    return wrap([{
        "name": ".",
        "sql": {"s": "substr(" + v + ", 1, " + l + ")"}
    }])


@extend(NotLeftOp)
def to_sql(self, schema, not_null=False, boolean=False):
    # test_v = self.value.missing().to_sql(boolean=True)[0].sql.b
    # test_l = self.length.missing().to_sql(boolean=True)[0].sql.b
    v = self.value.to_sql(schema, not_null=True)[0].sql.s
    l = "max(0, " + self.length.to_sql(schema, not_null=True)[0].sql.n + ")"

    expr = "substr(" + v + ", " + l + "+1)"
    return wrap([{"name": ".", "sql": {"s": expr}}])


@extend(RightOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema, not_null=True)[0].sql.s
    r = self.length.to_sql(schema, not_null=True)[0].sql.n
    l = "max(0, length(" + v + ")-max(0, " + r + "))"
    expr = "substr(" + v + ", " + l + "+1)"
    return wrap([{"name": ".", "sql": {"s": expr}}])


@extend(NotRightOp)
def to_sql(self, schema, not_null=False, boolean=False):
    v = self.value.to_sql(schema, not_null=True)[0].sql.s
    r = self.length.to_sql(schema, not_null=True)[0].sql.n
    l = "max(0, length(" + v + ")-max(0, " + r + "))"
    expr = "substr(" + v + ", 1, " + l + ")"
    return wrap([{"name": ".", "sql": {"s": expr}}])


@extend(FindOp)
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
            index = "INSTR(SUBSTR(" + value + "," + start_index + "+1)," + find + ")+" + start_index + "-1"

        sql = "CASE WHEN (" + test + ") THEN (" + index + ") ELSE (" + default + ") END"
        return wrap([{"name": ".", "sql": {"n": sql}}])


@extend(InOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not isinstance(self.superset, Literal):
        Log.error("Not supported")
    j_value = json2value(self.superset.json)
    if j_value:
        var = self.value.to_sql(schema)
        return SQL_OR.join("(" + var + "==" + quote_value(v) + ")" for v in j_value)
    else:
        return wrap([{"name": ".", "sql": {"b": "0"}}])


@extend(RangeOp)
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


@extend(CaseOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if len(self.whens) == 1:
        return self.whens[-1].to_sql(schema)

    output = {}
    for t in "bsn":  # EXPENSIVE LOOP to_sql() RUN 3 TIMES
        els_ = coalesce(self.whens[-1].to_sql(schema)[0].sql[t], "NULL")
        acc = " ELSE " + els_ + " END"
        for w in reversed(self.whens[0:-1]):
            acc = " WHEN " + w.when.to_sql(schema, boolean=True)[0].sql.b + " THEN " + coalesce(w.then.to_sql(schema)[0].sql[t], "NULL") + acc
        output[t] = "CASE" + acc
    return wrap([{"name": ".", "sql": output}])


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
