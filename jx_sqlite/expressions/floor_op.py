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

from jx_base.expressions import FloorOp as FloorOp_
from jx_sqlite.expressions._utils import SQLang, check
from mo_dots import wrap
from mo_future import text
from mo_sql import sql_iso


class FloorOp(FloorOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        lhs = SQLang[self.lhs].to_sql(schema)[0].sql.n
        rhs = SQLang[self.rhs].to_sql(schema)[0].sql.n
        modifier = lhs + " < 0 "

        if text(rhs).strip() != "1":
            floor = "CAST" + sql_iso(lhs + "/" + rhs + " AS INTEGER")
            sql = sql_iso(sql_iso(floor) + "-" + sql_iso(modifier)) + "*" + rhs
        else:
            floor = "CAST" + sql_iso(lhs + " AS INTEGER")
            sql = sql_iso(floor) + "-" + sql_iso(modifier)

        return wrap([{"name": ".", "sql": {"n": sql}}])
