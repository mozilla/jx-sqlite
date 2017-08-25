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
    ConcatOp, LeftOp, EqOp, WhenOp
from jx_base import STRUCT, OBJECT
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
                value = quote_column(col.es_column).sql
            else:
                value = "(" + quote_column(col.es_column).sql + ") IS NOT NULL"
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
                            tempb[json_type_to_sql_type[col.type]] = quote_column(child_col.es_column).sql
            else:
                nested_path = col.nested_path[0]
                tempa = acc.setdefault(nested_path, {})
                tempb = tempa.setdefault(get_property_name(cname), {})
                tempb[json_type_to_sql_type[col.type]] = quote_column(col.es_column).sql

    return wrap([
        {"name": relative_field(cname, self.var), "sql": types, "nested_path": nested_path}
        for nested_path, pairs in acc.items() for cname, types in pairs.items()
    ])

@extend(Literal)
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
    return wrap([{"name": ".", "sql": {"n": sql_quote(json2value(self.json))}}])


@extend(TupleOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": t.to_sql(schema)[0].sql} for t in self.terms])


@extend(LeavesOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not isinstance(self.term, Variable):
        Log.error("Can only handle Variable")
    term = self.term.var
    prefix_length = len(split_field(term))
    db_columns = []
    for n, cols in schema.map_to_sql(term).items():
        for c in cols:
            col = schema.get_column_name(c)
            if startswith_field(col, term):
                db_columns.append({
                    "name": join_field(split_field(col)[prefix_length:]),
                    "sql": Variable(col).to_sql(schema)[0].sql
                })
            else:
                db_columns.append({
                    "name": col,
                    "sql": Variable(col).to_sql(schema)[0].sql
                })                
    
    return wrap(db_columns)


@extend(EqOp)
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
                    acc.append("COALESCE((" + l.sql[t] + ") = (" + r.sql[t] + "), (" + l.sql[t] + ") IS NULL AND (" + r.sql[t] + ") IS NULL)")
    if not acc:
        return FalseOp().to_sql(schema)
    else:
        return wrap([{"name": ".", "sql": {"b": " OR ".join(acc)}}])


@extend(BinaryOp)
def to_sql(self, schema, not_null=False, boolean=False):
    lhs = self.lhs.to_sql(schema)[0].sql.n
    rhs = self.rhs.to_sql(schema)[0].sql.n

    return wrap([{"name": ".", "sql": {"n": "(" + lhs + ") " + BinaryOp.operators[self.op] + " (" + rhs + ")"}}])


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
    sql = "(" + ") OR (".join(ors) + ")"

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
        return FalseOp().to_sql(schema)
    else:
        return wrap([{"name": ".", "sql": {"b": " OR ".join(acc)}}])


@extend(NeOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return NotOp("not", EqOp("eq", [self.lhs, self.rhs])).to_sql(schema, not_null, boolean)

@extend(NotOp)
def to_sql(self, schema, not_null=False, boolean=False):
    sql = self.term.to_sql(schema)[0].sql
    return wrap([{"name": ".", "sql": {
        "0": "1",
        "b": "NOT (" + sql.b + ")",
        "n": "(" + sql.n + ") IS NULL",
        "s": "(" + sql.s + ") IS NULL"
    }}])


@extend(AndOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not self.terms:
        return wrap([{"name": ".", "sql": {"b": "1"}}])
    elif all(self.terms):
        return wrap([{"name": ".", "sql": {
            "b": " AND ".join("(" + t.to_sql(schema, boolean=True)[0].sql.b + ")" for t in self.terms)}}])
    else:
        return wrap([{"name": ".", "sql": {"b": "0"}}])


@extend(OrOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".",
                  "sql": {"b": " OR ".join("(" + t.to_sql(schema, boolean=True)[0].sql.b + ")" for t in self.terms)}}])


@extend(LengthOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if isinstance(self.term, Literal):
        val = json2value(self.term)
        if isinstance(val, unicode):
            return wrap([{"name": ".", "sql": {"n": convert.value2json(len(val))}}])
        elif isinstance(val, (float, int)):
            return wrap([{"name": ".", "sql": {"n": convert.value2json(len(convert.value2json(val)))}}])
        else:
            return Null
    value = self.term.to_sql(schema)[0].sql.s
    return wrap([{"name": ".", "sql": {"n": "LENGTH(" + value + ")"}}])


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
    else:
        return wrap([{"name": ".", "sql": {"n": "COALESCE(" + ",".join(acc) + ")"}}])


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
        return wrap([{"name": ".", "sql": {"s": "COALESCE(" + ",".join(acc) + ")"}}])


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
        return wrap([{"nanme": ".", "sql": {"n": "+".join(acc)}}])


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
            " WHEN " + " AND ".join("((" + s + ") IS NULL)" for s in sql_terms) +
            " THEN " + default +
            " ELSE " + op.join("COALESCE(" + s + ", 0)" for s in sql_terms) +
            " END"
        )
        return wrap([{"name": ".", "sql": {"n": sql}}])
    else:
        sql = (
            " CASE " +
            " WHEN " + " OR ".join("((" + s + ") IS NULL)" for s in sql_terms) +
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
            output[t] = "COALESCE(" + ",".join(terms) + ")"
    return wrap([{"name": ".", "sql": output}])


@extend(MissingOp)
def to_sql(self, schema, not_null=False, boolean=False):
    field = self.expr.to_sql(schema)

    if len(field) > 1:
        return wrap([{"name": ".", "sql": {"b": "0"}}])

    acc = []
    for c in field:
        for t, v in c.sql.items():
            if t == "b":
                acc.append("(" + v + ") IS NULL")
            if t == "s":
                acc.append("((" + v + ") IS NULL OR " + v + "='')")
            if t == "n":
                acc.append("(" + v + ") IS NULL")

    if not acc:
        return wrap([{"name": ".", "sql": {"b": "1"}}])
    else:
        return wrap([{"name": ".", "sql": {"b": " AND ".join(acc)}}])


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
        return wrap([{"name": ".", "sql": {"b": " OR ".join(acc)}}])


@extend(PrefixOp)
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {
        "b": "INSTR(" + self.field.to_sql(schema)[0].sql.s + ", " + self.prefix.to_sql(schema)[0].sql.s + ")==1"}}])


@extend(ConcatOp)
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
            acc.append("CASE WHEN (" + missing.to_sql(schema, boolean=True)[
                0].sql.b + ") THEN '' ELSE  ((" + sep + ") || (" + term_sql + ")) END")
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


@extend(BetweenOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if isinstance(self.prefix, Literal) and isinstance(convert.json2value(self.prefix.json), int):
        value_is_missing = self.value.missing().to_sql(schema, boolean=True)[0].sql.b
        value = self.value.to_sql(schema, not_null=True)[0].sql.s
        prefix = "max(0, " + self.prefix.to_sql(schema)[0].sql.n + ")"
        suffix = self.suffix.to_sql(schema)[0].sql.n
        start_index = self.start.to_sql(schema)[0].sql.n
        default = self.default.to_sql(schema, not_null=True)[0].sql.s if self.default else "NULL"

        if start_index:
            start = prefix + "+" + start_index + "+1"
        else:
            if prefix:
                start = prefix + "+1"
            else:
                start = "1"

        if suffix:
            length = "," + suffix + "-" + prefix
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
        len_prefix = text_type(len(convert.json2value(self.prefix.json))) if isinstance(self.prefix,
                                                                                      Literal) else "length(" + prefix + ")"
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
                " ELSE substr(" + value + ", " + start + ")" +
                " END"
            )

        return wrap([{"name": ".", "sql": {"s": expr}}])


@extend(InOp)
def to_sql(self, schema, not_null=False, boolean=False):
    if not isinstance(self.superset, Literal):
        Log.error("Not supported")
    j_value = json2value(self.superset.json)
    if j_value:
        var = self.value.to_sql(schema)
        return " OR ".join("(" + var + "==" + sql_quote(v) + ")" for v in j_value)
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
    output = {}
    for t in "bsn":  # EXPENSIVE LOOP to_sql() RUN 3 TIMES
        acc = " ELSE " + self.whens[-1].to_sql(schema)[t] + " END"
        for w in reversed(self.whens[0:-1]):
            acc = " WHEN " + w.when.to_sql(boolean=True).b + " THEN " + w.then.to_sql(schema)[t] + acc
        output[t] = "CASE" + acc
    return output


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
        return text_type(value)


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
