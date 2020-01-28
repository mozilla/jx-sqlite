# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import FALSE, FalseOp, NULL, NullOp, TrueOp, extend
from jx_base.language import Language
from jx_sqlite import quote_column
from jx_sqlite.sqlite import sql_call
from mo_dots import wrap, FlatList, is_data
from mo_future import decorate
from mo_json import BOOLEAN, NESTED, OBJECT, STRING, NUMBER, IS_NULL, TIME, INTERVAL
from mo_logs import Log
from mo_sql import (
    SQL,
    SQL_FALSE,
    SQL_NULL,
    SQL_TRUE,
    sql_iso,
    SQL_ZERO,
    SQL_ONE,
    SQL_PLUS,
    SQL_STAR,
    SQL_LT,
    ConcatSQL)

NumberOp, OrOp, SQLScript = [None] * 3


def check(func):
    """
    TEMPORARY TYPE CHECKING TO ENSURE to_sql() IS OUTPUTTING THE CORRECT FORMAT
    """

    @decorate(func)
    def to_sql(self, schema, not_null=False, boolean=False, **kwargs):
        if kwargs.get("many") != None:
            Log.error("not expecting many")
        try:
            output = func(self, schema, not_null, boolean)
        except Exception as e:
            Log.error("not expected", cause=e)
        if isinstance(output, SQLScript):
            return output
        if not isinstance(output, FlatList):
            Log.error("expecting FlatList")
        if not is_data(output[0].sql):
            Log.error("expecting Data")
        for k, v in output[0].sql.items():
            if k not in {"b", "n", "s", "j", "0"}:
                Log.error("expecting datatypes")
            if not isinstance(v, SQL):
                Log.error("expecting text")
        return output

    return to_sql


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


def _inequality_to_sql(self, schema, not_null=False, boolean=False, many=True):
    op, identity = _sql_operators[self.op]
    lhs = NumberOp(self.lhs).partial_eval().to_sql(schema, not_null=True)[0].sql.n
    rhs = NumberOp(self.rhs).partial_eval().to_sql(schema, not_null=True)[0].sql.n
    sql = sql_iso(lhs) + op + sql_iso(rhs)

    output = SQLScript(
        data_type=BOOLEAN,
        expr=sql,
        frum=self,
        miss=OrOp([self.lhs.missing(), self.rhs.missing()]),
        schema=schema,
    )
    return output


@check
def _binaryop_to_sql(self, schema, not_null=False, boolean=False, many=True):
    op, identity = _sql_operators[self.op]

    lhs = NumberOp(self.lhs).partial_eval().to_sql(schema, not_null=True)[0].sql.n
    rhs = NumberOp(self.rhs).partial_eval().to_sql(schema, not_null=True)[0].sql.n
    script = sql_iso(lhs) + op + sql_iso(rhs)
    if not_null:
        sql = script
    else:
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


def multiop_to_sql(self, schema, not_null=False, boolean=False, many=False):
    sign, zero = _sql_operators[self.op]
    if len(self.terms) == 0:
        return SQLang[self.default].to_sql(schema)
    elif self.default is NULL:
        return sign.join(
            sql_call("COALESCE", SQLang[t].to_sql(schema), zero)
            for t in self.terms
        )
    else:
        return sql_call(
            "COALESCE",
            sign.join(sql_iso(SQLang[t].to_sql(schema)) for t in self.terms),
            SQLang[self.default].to_sql(schema)
        )


def with_var(var, expression, eval):
    """
    :param var: NAME GIVEN TO expression
    :param expression: THE EXPRESSION TO COMPUTE FIRST
    :param eval: THE EXPRESSION TO COMPUTE SECOND, WITH var ASSIGNED
    :return: PYTHON EXPRESSION
    """
    return ConcatSQL(
        SQL("WITH x AS (SELECT ("),
        expression,
        SQL(") AS "),
        var,
        SQL(") SELECT "),
        eval,
        SQL(" FROM x")
    )


def basic_multiop_to_sql(self, schema, not_null=False, boolean=False, many=False):
    op, identity = _sql_operators[self.op.split("basic.")[1]]
    sql = op.join(sql_iso(SQLang[t].to_sql(schema)[0].sql.n) for t in self.terms)
    return wrap([{"name": ".", "sql": {"n": sql}}])


SQLang = Language("SQLang")


_sql_operators = {
    # (operator, zero-array default value) PAIR
    "add": (SQL_PLUS, SQL_ZERO),
    "sum": (SQL_PLUS, SQL_ZERO),
    "mul": (SQL_STAR, SQL_ONE),
    "sub": (SQL(" - "), None),
    "div": (SQL(" / "), None),
    "exp": (SQL(" ** "), None),
    "mod": (SQL(" % "), None),
    "gt": (SQL(" > "), None),
    "gte": (SQL(" >= "), None),
    "lte": (SQL(" <= "), None),
    "lt": (SQL_LT, None),
}

SQL_IS_NULL_TYPE = "0"
SQL_BOOLEAN_TYPE = "b"
SQL_NUMBER_TYPE = "n"
SQL_TIME_TYPE = "t"
SQL_INTERVAL_TYPE = "n"
SQL_STRING_TYPE = "s"
SQL_OBJECT_TYPE = "j"
SQL_NESTED_TYPE = "a"

json_type_to_sql_type = {
    IS_NULL: SQL_IS_NULL_TYPE,
    BOOLEAN: SQL_BOOLEAN_TYPE,
    NUMBER: SQL_NUMBER_TYPE,
    TIME: SQL_TIME_TYPE,
    INTERVAL: SQL_INTERVAL_TYPE,
    STRING: SQL_STRING_TYPE,
    OBJECT: SQL_OBJECT_TYPE,
    NESTED: SQL_NESTED_TYPE,
}

sql_type_to_json_type = {
    None: None,
    SQL_IS_NULL_TYPE: IS_NULL,
    SQL_BOOLEAN_TYPE: BOOLEAN,
    SQL_NUMBER_TYPE: NUMBER,
    SQL_TIME_TYPE: TIME,
    SQL_STRING_TYPE: STRING,
    SQL_OBJECT_TYPE: OBJECT,
}
