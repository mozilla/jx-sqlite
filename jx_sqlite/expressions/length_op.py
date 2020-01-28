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

from jx_base.expressions import LengthOp as LengthOp_, is_literal
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.sqlite import quote_value
from mo_dots import Null, wrap
from mo_future import text
from mo_json import value2json
from mo_sql import SQL, sql_iso, ConcatSQL


class LengthOp(LengthOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        term = SQLang[self.term].partial_eval()
        if is_literal(term):
            val = term.value
            if isinstance(val, text):
                sql = quote_value(len(val))
            elif isinstance(val, (float, int)):
                sql = quote_value(len(value2json(val)))
            else:
                return Null
        else:
            value = term.to_sql(schema, not_null=not_null)[0].sql.s
            sql = ConcatSQL(SQL("LENGTH"), sql_iso(value))
        return wrap([{"name": ".", "sql": {"n": sql}}])
