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

from jx_base.expressions import SuffixOp as SuffixOp_
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.eq_op import EqOp
from jx_sqlite.expressions.length_op import LengthOp
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.expressions.right_op import RightOp
from mo_dots import wrap
from mo_sql import SQL_FALSE, SQL_TRUE


class SuffixOp(SuffixOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        if not self.expr:
            return wrap([{"name": ".", "sql": {"b": SQL_FALSE}}])
        elif isinstance(self.suffix, Literal) and not self.suffix.value:
            return wrap([{"name": ".", "sql": {"b": SQL_TRUE}}])
        else:
            return (
                EqOp([RightOp([self.expr, LengthOp(self.suffix)]), self.suffix])
                .partial_eval()
                .to_sql(schema)
            )
