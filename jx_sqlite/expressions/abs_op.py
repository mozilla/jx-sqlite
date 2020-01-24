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

from jx_base.expressions import AbsOp as AbsOp_
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SQLScript
from mo_json import NUMBER


class AbsOp(AbsOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        expr = SQLang[self.term].partial_eval().to_sql(schema)[0].sql.n
        return SQLScript(
            expr="ABS(" + expr + ")",
            data_type=NUMBER,
            frum=self,
            miss=self.missing(),
            schema=schema,
        )
