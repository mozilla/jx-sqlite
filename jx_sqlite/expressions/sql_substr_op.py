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

from jx_base.expressions import NULL, SqlSubstrOp as SqlSubstrOp_
from jx_sqlite.expressions._utils import check, SQLang
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.sqlite import sql_call
from mo_dots import wrap


class SqlSubstrOp(SqlSubstrOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = SQLang[self.value].to_sql(schema, not_null=True)[0].sql.s
        start = SQLang[self.start].to_sql(schema, not_null=True)[0].sql.n
        if self.length is NULL:
            sql = sql_call("SUBSTR", value, start)
        else:
            length = SQLang[self.length].to_sql(schema, not_null=True)[0].sql.n
            sql = sql_call("SUBSTR", value, start, length)
        return wrap([{"name": ".", "sql": {"s": sql}}])

    def partial_eval(self):
        value = SQLang[self.value].partial_eval()
        start = SQLang[self.start].partial_eval()
        length = SQLang[self.length].partial_eval()
        if isinstance(start, Literal) and start.value == 1:
            if length is NULL:
                return value
        return SqlSubstrOp([value, start, length])
