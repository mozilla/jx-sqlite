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

from jx_base.expressions import NotRightOp as NotRightOp_
from jx_sqlite.expressions._utils import check
from jx_sqlite.sqlite import sql_call
from mo_dots import wrap
from mo_sql import SQL_ONE, SQL_ZERO, SQL


class NotRightOp(NotRightOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        v = self.value.to_sql(schema, not_null=True)[0].sql.s
        r = self.length.to_sql(schema, not_null=True)[0].sql.n
        l = sql_call("MAX", SQL_ZERO, sql_call("length", v) + SQL(" - ") + sql_call("MAX", SQL_ZERO, r))
        sql = sql_call("SUBSTR", v, SQL_ONE, l)
        return wrap([{"name": ".", "sql": {"s": sql}}])
