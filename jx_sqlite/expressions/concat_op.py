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

from jx_base.expressions import ConcatOp as ConcatOp_, TrueOp, ZERO, is_literal
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.length_op import LengthOp
from jx_sqlite.expressions.sql_script import SQLScript
from jx_sqlite.sqlite import quote_value, sql_call
from mo_dots import coalesce
from mo_json import STRING
from mo_sql import (
    SQL,
    SQL_CASE,
    SQL_ELSE,
    SQL_EMPTY_STRING,
    SQL_END,
    SQL_NULL,
    SQL_THEN,
    SQL_WHEN,
    sql_iso,
    sql_list,
    sql_concat_text,
    ConcatSQL, SQL_PLUS, SQL_ONE, SQL_ZERO)


class ConcatOp(ConcatOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        default = self.default.to_sql(schema)
        if len(self.terms) == 0:
            return default
        len_sep = LengthOp(self.separator).partial_eval()
        no_sep = is_literal(len_sep) and len_sep.value==0
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

            if no_sep:
                sep_term = term_sql
            else:
                sep_term = sql_iso(sql_concat_text([sep, term_sql]))

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
                    + sep_term
                    + SQL_END
                )
            else:
                acc.append(sep_term)

        if no_sep:
            expr_ = sql_concat_text(acc)
        else:
            expr_ = sql_call(
                "SUBSTR",
                sql_concat_text(acc),
                ConcatSQL(LengthOp(self.separator).to_sql(schema)[0].sql.n, SQL_PLUS, SQL_ONE)
            )

        return SQLScript(
            expr=expr_,
            data_type=STRING,
            frum=self,
            miss=self.missing(),
            many=False,
            schema=schema,
        )
