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

from jx_base.expressions import DateOp as DateOp_
from jx_sqlite.expressions._utils import check
from jx_sqlite.sqlite import quote_value
from mo_dots import wrap


class DateOp(DateOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        return wrap([{"name": ".", "sql": {"n": quote_value(self.value)}}])
