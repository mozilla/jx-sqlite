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

from jx_base.expressions import BasicStartsWithOp as BasicStartsWithOp_, ONE, is_literal
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.length_op import LengthOp
from jx_sqlite.expressions.sql_eq_op import SqlEqOp
from jx_sqlite.expressions.sql_substr_op import SqlSubstrOp
from mo_dots import wrap
from mo_sql import SQL, ConcatSQL, SQL_LIKE, SQL_ESCAPE


class BasicStartsWithOp(BasicStartsWithOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        prefix = SQLang[self.prefix].partial_eval()
        if is_literal(prefix):
            value = SQLang[self.value].partial_eval().to_sql(schema)[0].sql.s
            prefix = prefix.to_sql(schema)[0].sql.s
            if "%" in prefix or "_" in prefix:
                for r in "\\_%":
                    prefix = prefix.replaceAll(r, "\\" + r)
                sql = ConcatSQL((value, SQL_LIKE, prefix, SQL_ESCAPE, SQL("\\")))
            else:
                sql = ConcatSQL((value, SQL_LIKE, prefix))
            return wrap([{"name": ".", "sql": {"b": sql}}])
        else:
            return (
                SqlEqOp([SqlSubstrOp([self.value, ONE, LengthOp(prefix)]), prefix])
                .partial_eval()
                .to_sql()
            )
