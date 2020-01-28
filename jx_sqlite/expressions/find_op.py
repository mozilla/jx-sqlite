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

from jx_base.expressions import FindOp as FindOp_, ZERO, simplified
from jx_sqlite.expressions._utils import SQLang, check, with_var
from jx_sqlite.expressions.and_op import AndOp
from jx_sqlite.expressions.ne_op import NeOp
from jx_sqlite.expressions.not_left_op import NotLeftOp
from jx_sqlite.expressions.not_right_op import NotRightOp
from jx_sqlite.expressions.or_op import OrOp
from jx_sqlite.expressions.sql_instr_op import SqlInstrOp
from jx_sqlite.sqlite import sql_call, quote_column
from mo_dots import coalesce, wrap
from mo_sql import (
    SQL,
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_NULL,
    SQL_THEN,
    SQL_WHEN,
    SQL_ZERO,
    ConcatSQL,
    SQL_ONE,
    SQL_PLUS,
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
        default = coalesce(
            SQLang[self.default].partial_eval().to_sql(schema)[0].sql.n, SQL_NULL
        )

        if start.sql != SQL_ZERO.sql:
            value = NotRightOp([self.value, self.start]).to_sql(schema)[0].sql.s

        index = sql_call("INSTR", value, find)
        i = quote_column("i")
        sql = with_var(
            i,
            index,
            ConcatSQL(
                SQL_CASE,
                SQL_WHEN,
                i,
                SQL_THEN,
                i,
                SQL(" - "),
                SQL_ONE,
                SQL_PLUS,
                start,
                SQL_ELSE,
                default,
                SQL_END,
            ),
        )
        return wrap([{"name": ".", "sql": {"n": sql}}])

    def exists(self):

        found = NeOp(
            [SqlInstrOp([NotLeftOp([self.value, self.start]), self.find]), ZERO]
        )

        output = self.lang[OrOp(
            [
                self.default.exists(),
                AndOp([self.value.exists(), self.find.exists(), found]),
            ]
        )].partial_eval()
        return output
