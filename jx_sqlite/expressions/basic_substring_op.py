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

from jx_base.expressions import BasicSubstringOp as BasicSubstringOp_
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.add_op import AddOp
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.expressions.sub_op import SubOp
from jx_sqlite.sqlite import sql_call
from mo_dots import wrap


class BasicSubstringOp(BasicSubstringOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = SQLang[self.value].to_sql(schema, not_null=True)[0].sql.s
        start = (
            AddOp([self.start, Literal(1)])
            .partial_eval()
            .to_sql(schema, not_null=True)[0]
            .sql.n
        )
        length = (
            SubOp([self.end, self.start])
            .partial_eval()
            .to_sql(schema, not_null=True)[0]
            .sql.n
        )
        sql = sql_call("SUBSTR", value, start, length)
        return wrap([{"name": ".", "sql": {"s": sql}}])
