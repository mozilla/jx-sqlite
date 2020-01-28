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

from jx_base.expressions import SqlInstrOp as SqlInstrOp_
from jx_sqlite.expressions._utils import check
from jx_sqlite.sqlite import sql_call
from mo_dots import wrap


class SqlInstrOp(SqlInstrOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.value.to_sql(schema, not_null=True)[0].sql.s
        find = self.find.to_sql(schema, not_null=True)[0].sql.s

        return wrap(
            [{"name": ".", "sql": {"n": sql_call("INSTR", value, find)}}]
        )

    def partial_eval(self):
        value = self.value.partial_eval()
        find = self.find.partial_eval()
        return SqlInstrOp([value, find])
