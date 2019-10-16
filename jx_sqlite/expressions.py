# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import (
    AddOp as AddOp_,
    AndOp as AndOp_,
    BasicAddOp as BasicAddOp_,
    BasicEqOp as BasicEqOp_,
    BasicIndexOfOp as BasicIndexOfOp_,
    BasicMulOp as BasicMulOp_,
    BasicSubstringOp as BasicSubstringOp_,
    BetweenOp as BetweenOp_,
    BooleanOp as BooleanOp_,
    CaseOp as CaseOp_,
    CoalesceOp as CoalesceOp_,
    ConcatOp as ConcatOp_,
    CountOp as CountOp_,
    DateOp as DateOp_,
    DivOp as DivOp_,
    EqOp as EqOp_,
    ExistsOp as ExistsOp_,
    ExpOp as ExpOp_,
    FALSE,
    FalseOp,
    FirstOp as FirstOp_,
    FindOp as FindOp_,
    FloorOp as FloorOp_,
    FromUnixOp as FromUnixOp_,
    GetOp as GetOp_,
    GtOp as GtOp_,
    GteOp as GteOp_,
    InOp as InOp_,
    IntegerOp as IntegerOp_,
    LastOp as LastOp_,
    LeavesOp as LeavesOp_,
    LeftOp as LeftOp_,
    LengthOp as LengthOp_,
    Literal as Literal_,
    LtOp as LtOp_,
    LteOp as LteOp_,
    MaxOp as MaxOp_,
    MinOp as MinOp_,
    MissingOp as MissingOp_,
    ModOp as ModOp_,
    MulOp as MulOp_,
    NULL,
    NeOp as NeOp_,
    NotLeftOp as NotLeftOp_,
    NotOp as NotOp_,
    NotRightOp as NotRightOp_,
    NullOp,
    NumberOp as NumberOp_,
    ONE,
    OffsetOp as OffsetOp_,
    OrOp as OrOp_,
    PrefixOp as PrefixOp_,
    RangeOp as RangeOp_,
    RegExpOp as RegExpOp_,
    RightOp as RightOp_,
    RowsOp as RowsOp_,
    SQLScript as SQLScript_,
    ScriptOp as ScriptOp_,
    SelectOp as SelectOp_,
    SqlEqOp as SqlEqOp_,
    SqlInstrOp as SqlInstrOp_,
    SqlSubstrOp as SqlSubstrOp_,
    StringOp as StringOp_,
    SubOp as SubOp_,
    SuffixOp as SuffixOp_,
    TRUE,
    TrueOp,
    TupleOp as TupleOp_,
    UnixOp as UnixOp_,
    Variable as Variable_,
    WhenOp as WhenOp_,
    ZERO,
    builtin_ops,
    define_language,
    extend,
    simplified,
    is_literal)
from jx_base.language import is_op
from jx_base.queries import get_property_name
from jx_sqlite import GUID, quoted_GUID
from mo_dots import (
    Null,
    ROOT_PATH,
    coalesce,
    join_field,
    relative_field,
    split_field,
    startswith_field,
    wrap,
    FlatList,
    is_data,
)
from mo_future import PY2, text_type, decorate
import mo_json
from mo_json import BOOLEAN, EXISTS, NESTED, OBJECT, json2value
from mo_logs import Log
from mo_math import is_number
from pyLibrary import convert
from pyLibrary.sql import (
    SQL,
    SQL_AND,
    SQL_CASE,
    SQL_ELSE,
    SQL_EMPTY_STRING,
    SQL_END,
    SQL_FALSE,
    SQL_IS_NOT_NULL,
    SQL_IS_NULL,
    SQL_NULL,
    SQL_OR,
    SQL_THEN,
    SQL_TRUE,
    SQL_WHEN,
    sql_coalesce,
    sql_concat,
    sql_iso,
    sql_list,
    SQL_ZERO,
    SQL_ONE,
)
from pyLibrary.sql.sqlite import quote_column, quote_value


def check(func):
    @decorate(func)
    def to_sql(self, schema, not_null=False, boolean=False, **kwargs):
        if kwargs.get("many") != None:
            Log.error("not expecting many")
        try:
            output = func(self, schema, not_null, boolean)
        except Exception as e:
            Log.error("not expected", cause=e)
        if not isinstance(output, FlatList):
            Log.error("expecting FlatList")
        if not is_data(output[0].sql):
            Log.error("expecting Data")
        for k, v in output[0].sql.items():
            if k not in {"b", "n", "s", "j", "0"}:
                Log.error("expecting datatypes")
            if not isinstance(v, text_type):
                Log.error("expecting text")
        return output

    return to_sql


class SQLScript(SQLScript_):
    __slots__ = ("miss", "data_type", "expr", "frum", "many")

    def __init__(self, data_type, expr, frum, miss=None, many=False):
        object.__init__(self)
        if miss not in [None, NULL, FALSE, TRUE, ONE, ZERO]:
            if frum.lang != miss.lang:
                Log.error("logic error")

        self.miss = coalesce(
            miss, FALSE
        )  # Expression that will return true/false to indicate missing result
        self.data_type = type
        self.expr = expr
        self.many = many  # True if script returns multi-value
        self.frum = frum  # THE ORIGINAL EXPRESSION THAT MADE expr

    @property
    def type(self):
        return self.data_type

    def __str__(self):
        missing = SQLang[self.miss].partial_eval()
        if missing is FALSE:
            return self.partial_eval().to_sql.expr
        elif missing is TRUE:
            return "None"

        return "None if (" + missing.to_sql(self.schema).expr + ") else (" + self.expr + ")"

    def __add__(self, other):
        return text_type(self) + text_type(other)

    def __radd__(self, other):
        return text_type(other) + text_type(self)

    if PY2:
        __unicode__ = __str__

    @check
    def to_sql(self, schema, not_null=False, boolean=False, many=True):
        return self

    def missing(self):
        return self.miss

    def __data__(self):
        return {"script": self.script}

    def __eq__(self, other):
        if not isinstance(other, SQLScript_):
            return False
        elif self.expr == other.expr:
            return True
        else:
            return False


class Variable(Variable_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False, many=True):
        var_name = self.var
        if var_name == GUID:
            return wrap(
                [{"name": ".", "sql": {"s": quoted_GUID}, "nested_path": ROOT_PATH}]
            )
        cols = schema.leaves(var_name)
        if not cols:
            # DOES NOT EXIST
            return wrap(
                [{"name": ".", "sql": {"0": SQL_NULL}, "nested_path": ROOT_PATH}]
            )
        acc = {}
        if boolean:
            for col in cols:
                cname = relative_field(col.name, var_name)
                nested_path = col.nested_path[0]
                if col.type == OBJECT:
                    value = SQL_TRUE
                elif col.type == BOOLEAN:
                    value = quote_column(col.es_column)
                else:
                    value = quote_column(col.es_column) + SQL_IS_NOT_NULL
                tempa = acc.setdefault(nested_path, {})
                tempb = tempa.setdefault(get_property_name(cname), {})
                tempb["b"] = value
        else:
            for col in cols:
                cname = relative_field(col.name, var_name)
                if col.jx_type == OBJECT:
                    prefix = self.var + "."
                    for cn, cs in schema.items():
                        if cn.startswith(prefix):
                            for child_col in cs:
                                tempa = acc.setdefault(child_col.nested_path[0], {})
                                tempb = tempa.setdefault(get_property_name(cname), {})
                                tempb[json_type_to_sql_type[col.type]] = quote_column(
                                    child_col.es_column
                                )
                else:
                    nested_path = col.nested_path[0]
                    tempa = acc.setdefault(nested_path, {})
                    tempb = tempa.setdefault(get_property_name(cname), {})
                    tempb[json_type_to_sql_type[col.jx_type]] = quote_column(
                        col.es_column
                    )

        return wrap(
            [
                {"name": cname, "sql": types, "nested_path": nested_path}
                for nested_path, pairs in acc.items()
                for cname, types in pairs.items()
            ]
        )


class OffsetOp(OffsetOp_):
    pass


class RowsOp(RowsOp_):
    pass


class BooleanOp(BooleanOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        term = SQLang[self.term].partial_eval()
        if term.type == "boolean":
            sql = term.to_sql(schema)
            return sql
        else:
            sql = term.exists().partial_eval().to_sql(schema)
            return sql


class IntegerOp(IntegerOp_):
    pass


class GetOp(GetOp_):
    pass


class LastOp(LastOp_):
    pass


class SelectOp(SelectOp_):
    pass


class ScriptOp(ScriptOp_):
    pass


class Literal(Literal_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.value
        v = quote_value(value)
        if v == None:
            return wrap([{"name": "."}])
        elif isinstance(value, text_type):
            return wrap([{"name": ".", "sql": {"s": quote_value(value)}}])
        elif is_number(v):
            return wrap([{"name": ".", "sql": {"n": quote_value(value)}}])
        elif v in [True, False]:
            return wrap([{"name": ".", "sql": {"b": quote_value(value)}}])
        else:
            return wrap([{"name": ".", "sql": {"j": quote_value(self.json)}}])


@extend(NullOp)
@check
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"0": SQL_NULL}}])


@extend(TrueOp)
@check
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])


@extend(FalseOp)
@check
def to_sql(self, schema, not_null=False, boolean=False):
    return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])


class DateOp(DateOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap([{"name": ".", "sql": {"n": quote_value(self.value)}}])


class TupleOp(TupleOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap(
            [{"name": ".", "sql": SQLang[t].to_sql(schema)[0].sql} for t in self.terms]
        )


class LeavesOp(LeavesOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        if not is_op(self.term, Variable):
            Log.error("Can only handle Variable")
        term = self.term.var
        prefix_length = len(split_field(term))
        output = wrap(
            [
                {
                    "name": join_field(
                        split_field(schema.get_column_name(c))[prefix_length:]
                    ),
                    "sql": Variable(schema.get_column_name(c)).to_sql(schema)[0].sql,
                }
                for c in schema.columns
                if startswith_field(c.name, term)
                and (
                    (
                        c.jx_type not in (EXISTS, OBJECT, NESTED)
                        and startswith_field(schema.nested_path[0], c.nested_path[0])
                    )
                    or (
                        c.jx_type not in (EXISTS, OBJECT)
                        and schema.nested_path[0] == c.nested_path[0]
                    )
                )
            ]
        )
        return output


def _inequality_to_sql(self, schema, not_null=False, boolean=False, many=True):
    op, identity = _sql_operators[self.op]
    lhs = NumberOp(self.lhs).partial_eval().to_sql(schema, not_null=True)
    rhs = NumberOp(self.rhs).partial_eval().to_sql(schema, not_null=True)
    script = "(" + lhs + ") " + op + " (" + rhs + ")"

    output = (
        WhenOp(
            OrOp([self.lhs.missing(), self.rhs.missing()]),
            **{"then": FALSE, "else": SQLScript(type=BOOLEAN, expr=script, frum=self)}
        )
        .partial_eval()
        .to_sql(schema)
    )
    return output


class GtOp(GtOp_):
    to_sql = _inequality_to_sql


class GteOp(GteOp_):
    to_sql = _inequality_to_sql


class LtOp(LtOp_):
    to_sql = _inequality_to_sql


class LteOp(LteOp_):
    to_sql = _inequality_to_sql


@check
def _binaryop_to_sql(self, schema, not_null=False, boolean=False, many=True):
    op, identity = _sql_operators[self.op]

    lhs = NumberOp(self.lhs).partial_eval().to_sql(schema, not_null=True)[0].sql.n
    rhs = NumberOp(self.rhs).partial_eval().to_sql(schema, not_null=True)[0].sql.n
    script = sql_iso(lhs) + op + sql_iso(rhs)
    missing = OrOp([self.lhs.missing(), self.rhs.missing()]).partial_eval()
    if missing is FALSE:
        sql = script
    else:
        sql = (
            "CASE WHEN "
            + missing.to_sql(schema, boolean=True)[0].sql.b
            + " THEN NULL ELSE "
            + script
            + " END"
        )
    return wrap([{"name": ".", "sql": {"n": sql}}])


class SubOp(SubOp_):
    to_sql = _binaryop_to_sql


class ExpOp(ExpOp_):
    to_sql = _binaryop_to_sql


class ModOp(ModOp_):
    to_sql = _binaryop_to_sql


class DivOp(DivOp_):
    to_sql = _binaryop_to_sql


class InOp(InOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        if not isinstance(self.superset, Literal):
            Log.error("Not supported")
        j_value = json2value(self.superset.json)
        if j_value:
            var = self.value.to_sql(schema)
            return SQL_OR.join(sql_iso(var + "==" + quote_value(v)) for v in j_value)
        else:
            return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])


class EqOp(EqOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = SQLang[self.lhs].to_sql(schema)
        rhs = SQLang[self.rhs].to_sql(schema)
        acc = []
        if len(lhs) != len(rhs):
            Log.error("lhs and rhs have different dimensionality!?")

        for l, r in zip(lhs, rhs):
            for t in "bsnj":
                if l.sql[t] == None:
                    if r.sql[t] == None:
                        pass
                    else:
                        acc.append(sql_iso(r.sql[t]) + SQL_IS_NULL)
                else:
                    if r.sql[t] == None:
                        acc.append(sql_iso(l.sql[t]) + SQL_IS_NULL)
                    else:
                        acc.append(sql_iso(l.sql[t]) + " = " + sql_iso(r.sql[t]))
        if not acc:
            return FALSE.to_sql(schema)
        else:
            return wrap([{"name": ".", "sql": {"b": SQL_OR.join(acc)}}])

    @simplified
    def partial_eval(self):
        lhs = self.lhs.partial_eval()
        rhs = self.rhs.partial_eval()

        if isinstance(lhs, Literal) and isinstance(rhs, Literal):
            return TRUE if builtin_ops["eq"](lhs.value, rhs.value) else FALSE
        else:
            rhs_missing = rhs.missing().partial_eval()
            return CaseOp(
                [
                    WhenOp(lhs.missing(), **{"then": rhs_missing}),
                    WhenOp(rhs_missing, **{"then": FALSE}),
                    SqlEqOp([lhs, rhs]),
                ],
            ).partial_eval()


class BasicEqOp(BasicEqOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False, many=False):
        return (
            "("
            + SQL[self.rhs].to_sql(schema)
            + ") == ("
            + SQL[self.lhs].to_sql(schema)
            + ")"
        )


class NeOp(NeOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        return (
            NotOp("not", EqOp("eq", [self.lhs, self.rhs]).partial_eval())
            .partial_eval()
            .to_sql(schema)
        )


class BasicIndexOfOp(BasicIndexOfOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.value.to_sql(schema)[0].sql.s
        find = self.find.to_sql(schema)[0].sql.s
        start = self.start

        if isinstance(start, Literal) and start.value == 0:
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {"n": "INSTR" + sql_iso(value + "," + find) + "-1"},
                    }
                ]
            )
        else:
            start_index = start.to_sql(schema)[0].sql.n
            found = "INSTR(SUBSTR" + sql_iso(value + "," + start_index + "+1)," + find)
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {
                            "n": (
                                SQL_CASE
                                + SQL_WHEN
                                + found
                                + SQL_THEN
                                + found
                                + "+"
                                + start_index
                                + "-1"
                                + SQL_ELSE
                                + "-1"
                                + SQL_END
                            )
                        },
                    }
                ]
            )


class BasicSubstringOp(BasicSubstringOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = SQLang[self.value].to_sql(schema)[0].sql.s
        start = (
            AddOp([self.start, Literal(1)]).partial_eval().to_sql(schema)[0].sql.n
        )
        length = SubOp([self.end, self.start]).partial_eval().to_sql(schema)[0].sql.n

        return wrap(
            [
                {
                    "name": ".",
                    "sql": {
                        "s": "SUBSTR" + sql_iso(value + "," + start + ", " + length)
                    },
                }
            ]
        )


class MinOp(MinOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        terms = [SQLang[t].partial_eval().to_sql(schema)[0].sql.n for t in self.terms]
        return wrap([{"name": ".", "sql": {"n": "min" + sql_iso((sql_list(terms)))}}])


class MaxOp(MaxOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        terms = [SQLang[t].partial_eval().to_sql(schema)[0].sql.n for t in self.terms]
        return wrap([{"name": ".", "sql": {"n": "max" + sql_iso((sql_list(terms)))}}])


class DivOp(DivOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = SQLang[self.lhs].to_sql(schema)[0].sql.n
        rhs = SQLang[self.rhs].to_sql(schema)[0].sql.n
        d = SQLang[self.default].to_sql(schema)[0].sql.n

        if lhs and rhs:
            if d == None:
                return wrap(
                    [{"name": ".", "sql": {"n": sql_iso(lhs) + " / " + sql_iso(rhs)}}]
                )
            else:
                return wrap(
                    [
                        {
                            "name": ".",
                            "sql": {
                                "n": sql_coalesce(
                                    [sql_iso(lhs) + " / " + sql_iso(rhs), d]
                                )
                            },
                        }
                    ]
                )
        else:
            return Null


class FloorOp(FloorOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = SQLang[self.lhs].to_sql(schema)[0].sql.n
        rhs = SQLang[self.rhs].to_sql(schema)[0].sql.n
        modifier = lhs + " < 0 "

        if rhs.strip() != "1":
            floor = "CAST" + sql_iso(lhs + "/" + rhs + " AS INTEGER")
            sql = sql_iso(sql_iso(floor) + "-" + sql_iso(modifier)) + "*" + rhs
        else:
            floor = "CAST" + sql_iso(lhs + " AS INTEGER")
            sql = sql_iso(floor) + "-" + sql_iso(modifier)

        return wrap([{"name": ".", "sql": {"n": sql}}])


class NotOp(NotOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        not_expr = NotOp(BooleanOp(self.term)).partial_eval()
        if isinstance(not_expr, NotOp):
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {
                            "b": "NOT " + sql_iso(not_expr.term.to_sql(schema)[0].sql.b)
                        },
                    }
                ]
            )
        else:
            return not_expr.to_sql(schema)


class AndOp(AndOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        if not self.terms:
            return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
        elif all(self.terms):
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {
                            "b": SQL_AND.join(
                                [
                                    sql_iso(
                                        SQLang[t].to_sql(schema, boolean=True)[0].sql.b
                                    )
                                    for t in self.terms
                                ]
                            )
                        },
                    }
                ]
            )
        else:
            return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])


class OrOp(OrOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap(
            [
                {
                    "name": ".",
                    "sql": {
                        "b": SQL_OR.join(
                            sql_iso(SQLang[t].to_sql(schema, boolean=True)[0].sql.b)
                            for t in self.terms
                        )
                    },
                }
            ]
        )


class LengthOp(LengthOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        term = SQLang[self.term].partial_eval()
        if is_literal(term):
            val = term.value
            if isinstance(val, text_type):
                return wrap([{"name": ".", "sql": {"n": convert.value2json(len(val))}}])
            elif isinstance(val, (float, int)):
                return wrap(
                    [
                        {
                            "name": ".",
                            "sql": {
                                "n": convert.value2json(len(convert.value2json(val)))
                            },
                        }
                    ]
                )
            else:
                return Null
        value = term.to_sql(schema)[0].sql.s
        return wrap([{"name": ".", "sql": {"n": "LENGTH" + sql_iso(value)}}])


class IntegerOp(IntegerOp_):
    @check
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
            return wrap([{"name": ".", "sql": {"n": sql_coalesce(acc)}}])


class FirstOp(FirstOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = SQLang[self.term].to_sql(schema, not_null=True)
        for c in value:
            for t, v in c.sql.items():
                if t == "j":
                    Log.error("can not handle")
        return value


class NumberOp(NumberOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = SQLang[self.term].to_sql(schema, not_null=True)
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
            return wrap([{"name": ".", "sql": {"n": acc[0]}}])
        else:
            return wrap([{"name": ".", "sql": {"n": sql_coalesce(acc)}}])


class StringOp(StringOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        test = self.term.missing().to_sql(schema, boolean=True)[0].sql.b
        value = self.term.to_sql(schema, not_null=True)[0].sql
        acc = []
        for t, v in value.items():
            if t == "b":
                acc.append(
                    SQL_CASE
                    + SQL_WHEN
                    + sql_iso(test)
                    + SQL_THEN
                    + SQL_NULL
                    + SQL_WHEN
                    + sql_iso(v)
                    + SQL_THEN
                    + "'true'"
                    + SQL_ELSE
                    + "'false'"
                    + SQL_END
                )
            elif t == "s":
                acc.append(v)
            else:
                acc.append(
                    "RTRIM(RTRIM(CAST"
                    + sql_iso(v + " as TEXT), " + quote_value("0"))
                    + ", "
                    + quote_value(".")
                    + ")"
                )
        if not acc:
            return wrap([{}])
        elif len(acc) == 1:
            return wrap([{"name": ".", "sql": {"s": acc[0]}}])
        else:
            return wrap([{"name": ".", "sql": {"s": sql_coalesce(acc)}}])


class CountOp(CountOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        acc = []
        for term in self.terms:
            sqls = term.to_sql(schema)
            if len(sqls) > 1:
                acc.append(SQL_TRUE)
            else:
                for t, v in sqls[0].sql.items():
                    if t in ["b", "s", "n"]:
                        acc.append(
                            SQL_CASE
                            + SQL_WHEN
                            + sql_iso(v)
                            + SQL_IS_NULL
                            + SQL_THEN
                            + "0"
                            + SQL_ELSE
                            + "1"
                            + SQL_END
                        )
                    else:
                        acc.append(SQL_TRUE)

        if not acc:
            return wrap([{}])
        else:
            return wrap([{"nanme": ".", "sql": {"n": SQL("+").join(acc)}}])


def multiop_to_sql(self, schema, not_null=False, boolean=False, many=False):
    sign, zero = _sql_operators[self.op]
    if len(self.terms) == 0:
        return SQLang[self.default].to_sql(schema)
    elif self.default is NULL:
        return sign.join(
            "COALESCE(" + SQLang[t].to_sql(schema) + ", " + zero + ")"
            for t in self.terms
        )
    else:
        return (
            "COALESCE("
            + sign.join("(" + SQLang[t].to_sql(schema) + ")" for t in self.terms)
            + ", "
            + SQLang[self.default].to_sql(schema)
            + ")"
        )


class AddOp(AddOp_):
    to_sql = multiop_to_sql


class MulOp(MulOp_):
    to_sql = multiop_to_sql


def basic_multiop_to_sql(self, schema, not_null=False, boolean=False, many=False):
    op, identity = _sql_operators[self.op.split("basic.")[1]]
    sql = op.join(sql_iso(SQLang[t].to_sql(schema)[0].sql.n) for t in self.terms)
    return wrap([{"name": ".", "sql": {"n": sql}}])


class BasicAddOp(BasicAddOp_):
    to_sql = basic_multiop_to_sql


class BasicMulOp(BasicMulOp_):
    to_sql = basic_multiop_to_sql


class RegExpOp(RegExpOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        pattern = quote_value(json2value(self.pattern.json))
        value = self.var.to_sql(schema)[0].sql.s
        return wrap([{"name": ".", "sql": {"b": value + " REGEXP " + pattern}}])


class CoalesceOp(CoalesceOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        acc = {"b": [], "s": [], "n": []}

        for term in self.terms:
            for t, v in SQLang[term].to_sql(schema)[0].sql.items():
                acc[t].append(v)

        output = {}
        for t, terms in acc.items():
            if not terms:
                continue
            elif len(terms) == 1:
                output[t] = terms[0]
            else:
                output[t] = sql_coalesce(terms)
        return wrap([{"name": ".", "sql": output}])


class MissingOp(MissingOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = SQLang[self.expr].partial_eval()
        missing_value = value.missing().partial_eval()

        if not is_op(missing_value, MissingOp):
            return missing_value.to_sql(schema)

        value_sql = value.to_sql(schema)

        if len(value_sql) > 1:
            return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])

        acc = []
        for c in value_sql:
            for t, v in c.sql.items():
                if t == "b":
                    acc.append(sql_iso(v) + SQL_IS_NULL)
                if t == "s":
                    acc.append(
                        sql_iso(sql_iso(v) + SQL_IS_NULL)
                        + SQL_OR
                        + sql_iso(sql_iso(v) + "=" + SQL_EMPTY_STRING)
                    )
                if t == "n":
                    acc.append(sql_iso(v) + SQL_IS_NULL)

        if not acc:
            return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
        else:
            return wrap([{"name": ".", "sql": {"b": SQL_AND.join(acc)}}])


class WhenOp(WhenOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False, many=True):
        when = SQLang[self.when].partial_eval().to_sql(schema, boolean=True)[0].sql
        then = SQLang[self.then].partial_eval().to_sql(schema, not_null=not_null)[0].sql
        els_ = SQLang[self.els_].partial_eval().to_sql(schema, not_null=not_null)[0].sql
        output = {}
        for t in "bsn":
            if then[t] == None:
                if els_[t] == None:
                    pass
                else:
                    output[t] = (
                        SQL_CASE
                        + SQL_WHEN
                        + when.b
                        + SQL_THEN
                        + SQL_NULL
                        + SQL_ELSE
                        + els_[t]
                        + SQL_END
                    )
            else:
                if els_[t] == None:
                    output[t] = (
                        SQL_CASE + SQL_WHEN + when.b + SQL_THEN + then[t] + SQL_END
                    )
                else:
                    output[t] = (
                        SQL_CASE
                        + SQL_WHEN
                        + when.b
                        + SQL_THEN
                        + then[t]
                        + SQL_ELSE
                        + els_[t]
                        + SQL_END
                    )
        if not output:
            return wrap([{"name": ".", "sql": {"0": SQL_NULL}}])
        else:
            return wrap([{"name": ".", "sql": output}])


class ExistsOp(ExistsOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        field = self.field.to_sql(schema)[0].sql
        acc = []
        for t, v in field.items():
            if t in "bns":
                acc.append(sql_iso(v + SQL_IS_NOT_NULL))

        if not acc:
            return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])
        else:
            return wrap([{"name": ".", "sql": {"b": SQL_OR.join(acc)}}])


class PrefixOp(PrefixOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        if not self.expr:
            return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
        else:
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {
                            "b": "INSTR"
                            + sql_iso(
                                self.expr.to_sql(schema)[0].sql.s
                                + ", "
                                + self.prefix.to_sql(schema)[0].sql.s
                            )
                            + "==1"
                        },
                    }
                ]
            )


class SuffixOp(SuffixOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        if not self.expr:
            return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])
        elif isinstance(self.suffix, Literal) and not self.suffix.value:
            return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
        else:
            return (
                EqOp("eq", [RightOp([self.expr, LengthOp(self.suffix)]), self.suffix])
                .partial_eval()
                .to_sql(schema)
            )


class ConcatOp(ConcatOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        default = self.default.to_sql(schema)
        if len(self.terms) == 0:
            return default
        default = coalesce(default[0].sql.s, SQL_NULL)
        sep = SQLang[self.separator].to_sql(schema)[0].sql.s

        acc = []
        for t in self.terms:
            t = SQLang[t]
            missing = t.missing().partial_eval()

            term = t.to_sql(schema, not_null=True)[0].sql
            if term.s:
                term_sql = term.s
            elif term.n:
                term_sql = "cast(" + term.n + " as text)"
            else:
                term_sql = (
                    SQL_CASE
                    + SQL_WHEN
                    + term.b
                    + SQL_THEN
                    + quote_value("true")
                    + SQL_ELSE
                    + quote_value("false")
                    + SQL_END
                )

            if isinstance(missing, TrueOp):
                acc.append(SQL_EMPTY_STRING)
            elif missing:
                acc.append(
                    SQL_CASE
                    + SQL_WHEN
                    + sql_iso(missing.to_sql(schema, boolean=True)[0].sql.b)
                    + SQL_THEN
                    + SQL_EMPTY_STRING
                    + SQL_ELSE
                    + sql_iso(sql_concat([sep, term_sql]))
                    + SQL_END
                )
            else:
                acc.append(sql_concat([sep, term_sql]))

        expr_ = (
            "substr" + sql_iso(sql_list([
                sql_concat(acc),
                LengthOp(self.separator).to_sql(schema)[0].sql.n + SQL("+1")
            ]))
        )

        missing = self.missing()
        if not missing:
            return wrap([{"name": ".", "sql": {"s": expr_}}])
        else:
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {
                            "s": SQL_CASE
                            + SQL_WHEN
                            + sql_iso(missing.to_sql(schema, boolean=True)[0].sql.b)
                            + SQL_THEN
                            + sql_iso(default)
                            + SQL_ELSE
                            + sql_iso(expr_)
                            + SQL_END
                        },
                    }
                ]
            )


class UnixOp(UnixOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema)[0].sql
        return wrap([{"name": ".", "sql": {"n": "UNIX_TIMESTAMP" + sql_iso(v.n)}}])


class FromUnixOp(FromUnixOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema)[0].sql
        return wrap([{"name": ".", "sql": {"n": "FROM_UNIXTIME" + sql_iso(v.n)}}])


class LeftOp(LeftOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        return (
            SqlSubstrOp("substr", [self.value, ONE, self.length])
            .partial_eval()
            .to_sql(schema)
        )


class NotLeftOp(NotLeftOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        # test_v = self.value.missing().to_sql(boolean=True)[0].sql.b
        # test_l = self.length.missing().to_sql(boolean=True)[0].sql.b
        v = self.value.to_sql(schema, not_null=True)[0].sql.s
        l = "max(0, " + self.length.to_sql(schema, not_null=True)[0].sql.n + ")"

        expr = "substr(" + v + ", " + l + "+1)"
        return wrap([{"name": ".", "sql": {"s": expr}}])


class RightOp(RightOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema, not_null=True)[0].sql.s
        r = self.length.to_sql(schema, not_null=True)[0].sql.n
        l = "max(0, length" + sql_iso(v) + "-max(0, " + r + "))"
        expr = "substr(" + v + ", " + l + "+1)"
        return wrap([{"name": ".", "sql": {"s": expr}}])


class RightOp(RightOp_):
    @simplified
    def partial_eval(self):
        value = self.value.partial_eval()
        length = self.length.partial_eval()
        max_length = LengthOp(value)

        return BasicSubstringOp(
            [
                value,
                MaxOp([ZERO, MinOp([max_length, SubOp([max_length, length])])]),
                max_length,
            ]
        )


class NotRightOp(NotRightOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema, not_null=True)[0].sql.s
        r = self.length.to_sql(schema, not_null=True)[0].sql.n
        l = "max(0, length" + sql_iso(v) + "-max(0, " + r + "))"
        expr = "substr" + sql_iso(v + ", 1, " + l)
        return wrap([{"name": ".", "sql": {"s": expr}}])


class FindOp(FindOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        test = SqlInstrOp(
            [SqlSubstrOp([self.value, AddOp([self.start, ONE]), NULL]), self.find]
        ).partial_eval()

        if boolean:
            return test.to_sql(schema)
        else:
            offset = SubOp([self.start, ONE]).partial_eval()
            index = AddOp([test, offset]).partial_eval()
            temp = index.to_sql(schema)
            return (
                WhenOp(EqOp([test, ZERO]), **{"then": self.default, "else": index})
                .partial_eval()
                .to_sql(schema)
            )


class FindOp(FindOp_):
    @simplified
    def partial_eval(self):
        return FindOp(
            [SQLang[self.value].partial_eval(), SQLang[self.find].partial_eval()],
            **{
                "start": SQLang[self.start].partial_eval(),
                "default": SQLang[self.default].partial_eval(),
            }
        )

    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = SQLang[self.value].partial_eval().to_sql(schema)[0].sql.s
        find = SQLang[self.find].partial_eval().to_sql(schema)[0].sql.s
        start = SQLang[self.start].partial_eval().to_sql(schema)[0].sql.n
        default = coalesce(SQLang[self.default].partial_eval().to_sql(schema)[0].sql.n, SQL_NULL)

        if start.sql != SQL_ZERO.sql.strip():
            value = NotRightOp([self.value, self.start]).to_sql(schema)[0].sql.s

        index = "INSTR" + sql_iso(sql_list([value, find]))

        sql = (
                SQL_CASE +
                SQL_WHEN + index +
                SQL_THEN + index + SQL("-1+") + start +
                SQL_ELSE + default +
                SQL_END
        )

        return wrap([{"name":".", "sql": {"n":sql}}])


class BetweenOp(BetweenOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        return self.partial_eval().to_sql(schema)


class RangeOp(RangeOp_):
    @check
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
                    output[t] = (
                        SQL_CASE
                        + SQL_WHEN
                        + when.b
                        + SQL_THEN
                        + SQL_NULL
                        + SQL_ELSE
                        + els_[t]
                        + SQL_END
                    )
            else:
                if els_[t] == None:
                    output[t] = (
                        SQL_CASE + SQL_WHEN + when.b + SQL_THEN + then[t] + SQL_END
                    )
                else:
                    output[t] = (
                        SQL_CASE
                        + SQL_WHEN
                        + when.b
                        + SQL_THEN
                        + then[t]
                        + SQL_ELSE
                        + els_[t]
                        + SQL_END
                    )
        if not output:
            return wrap([{"name": ".", "sql": {"0": SQL_NULL}}])
        else:
            return wrap([{"name": ".", "sql": output}])


class CaseOp(CaseOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        if len(self.whens) == 1:
            return SQLang[self.whens[-1]].to_sql(schema)

        output = {}
        for t in "bsn":  # EXPENSIVE LOOP to_sql() RUN 3 TIMES
            els_ = coalesce(SQLang[self.whens[-1]].to_sql(schema)[0].sql[t], SQL_NULL)
            acc = SQL_ELSE + els_ + SQL_END
            for w in reversed(self.whens[0:-1]):
                acc = (
                    SQL_WHEN
                    + SQLang[w.when].to_sql(schema, boolean=True)[0].sql.b
                    + SQL_THEN
                    + coalesce(SQLang[w.then].to_sql(schema)[0].sql[t], SQL_NULL)
                    + acc
                )
            output[t] = SQL_CASE + acc
        return wrap([{"name": ".", "sql": output}])


class SqlEqOp(SqlEqOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = SQLang[self.lhs].partial_eval().to_sql(schema)[0].sql.values()[0]
        rhs = SQLang[self.rhs].partial_eval().to_sql(schema)[0].sql.values()[0]

        return wrap([{"name": ".", "sql": {"b": sql_iso(lhs) + "=" + sql_iso(rhs)}}])


class SqlInstrOp(SqlInstrOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.value.to_sql(schema)[0].sql.s
        find = self.find.to_sql(schema)[0].sql.s

        return wrap(
            [{"name": ".", "sql": {"n": "INSTR" + sql_iso(sql_list([value, find]))}}]
        )

    def partial_eval(self):
        value = self.value.partial_eval()
        find = self.find.partial_eval()
        return SqlInstrOp([value, find])


class SqlSubstrOp(SqlSubstrOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.value.to_sql(schema)[0].sql.s
        start = self.start.to_sql(schema)[0].sql.n
        if self.length is NULL:
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {"s": "SUBSTR" + sql_iso(sql_list([value, start]))},
                    }
                ]
            )
        else:
            length = self.length.to_sql(schema)[0].sql.n
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {
                            "s": "SUBSTR" + sql_iso(sql_list([value, start, length]))
                        },
                    }
                ]
            )

    def partial_eval(self):
        value = self.value.partial_eval()
        start = self.start.partial_eval()
        length = self.length.partial_eval()
        if isinstance(start, Literal) and start.value == 1:
            if length is NULL:
                return value
        return SqlSubstrOp([value, start, length])


SQLang = define_language("SQLang", vars())


_sql_operators = {
    # (operator, zero-array default value) PAIR
    "add": (SQL(" + "), SQL_ZERO),
    "sum": (SQL(" + "), SQL_ZERO),
    "mul": (SQL(" * "), SQL_ONE),
    "sub": (SQL(" - "), None),
    "div": (SQL(" / "), None),
    "exp": (SQL(" ** "), None),
    "mod": (SQL(" % "), None),
    "gt": (SQL(" > "), None),
    "gte": (SQL(" >= "), None),
    "lte": (SQL(" <= "), None),
    "lt": (SQL(" < "), None),
}


json_type_to_sql_type = {
    mo_json.IS_NULL: "0",
    mo_json.BOOLEAN: "b",
    mo_json.NUMBER: "n",
    mo_json.STRING: "s",
    mo_json.OBJECT: "j",
    mo_json.NESTED: "N",
}

sql_type_to_json_type = {
    None: None,
    "0": mo_json.IS_NULL,
    "b": mo_json.BOOLEAN,
    "n": mo_json.NUMBER,
    "s": mo_json.STRING,
    "j": mo_json.OBJECT,
}
