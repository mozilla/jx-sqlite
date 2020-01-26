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

from jx_base.expressions import BasicEqOp as BasicEqOp_
from jx_sqlite.expressions._utils import check, SQLang
from mo_sql import sql_iso, SQL_EQ


class BasicEqOp(BasicEqOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False, many=False):
        return sql_iso(SQLang[self.rhs].to_sql(schema)) + SQL_EQ + sql_iso(+ SQLang[self.lhs].to_sql(schema))
