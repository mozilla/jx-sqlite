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

from jx_base.expressions import BasicStartsWithOp as BasicStartsWithOp_, is_literal
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_eq_op import SqlEqOp
from jx_sqlite.expressions.sql_instr_op import SqlInstrOp
from jx_sqlite.sqlite import quote_value
from mo_dots import wrap
from mo_sql import SQL, ConcatSQL, SQL_LIKE, SQL_ESCAPE, SQL_ONE


class BasicStartsWithOp(BasicStartsWithOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        prefix = SQLang[self.prefix].partial_eval()
        if is_literal(prefix):
            value = SQLang[self.value].partial_eval().to_sql(schema)[0].sql.s
            prefix = prefix.value
            if "%" in prefix or "_" in prefix:
                for r in "\\_%":
                    prefix = prefix.replaceAll(r, "\\" + r)
                sql = ConcatSQL(value, SQL_LIKE, quote_value(prefix+"%"), SQL_ESCAPE, SQL("\\"))
            else:
                sql = ConcatSQL(value, SQL_LIKE, quote_value(prefix+"%"))
            return wrap([{"name": ".", "sql": {"b": sql}}])
        else:
            return (
                SqlEqOp([SqlInstrOp([self.value, prefix]), SQL_ONE])
                .partial_eval()
                .to_sql()
            )
