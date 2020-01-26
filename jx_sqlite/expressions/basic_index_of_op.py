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

from jx_base.expressions import BasicIndexOfOp as BasicIndexOfOp_
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.sqlite import sql_call
from mo_dots import wrap
from mo_sql import SQL_CASE, SQL_ELSE, SQL_END, SQL_THEN, SQL_WHEN, sql_iso, SQL_ONE


class BasicIndexOfOp(BasicIndexOfOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.value.to_sql(schema)[0].sql.s
        find = self.find.to_sql(schema)[0].sql.s
        start = self.start

        if isinstance(start, Literal) and start.value == 0:
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {"n": sql_call("INSTR", value, find) + "-1"},
                    }
                ]
            )
        else:
            start_index = start.to_sql(schema)[0].sql.n
            found = sql_call("INSTR", sql_call("SUBSTR", value, start_index), SQL_ONE, find)
            return wrap(
                [
                    {
                        "name": ".",
                        "sql": {
                            "n": (
                                SQL_CASE
                                + SQL_WHEN
                                + found
                                + SQL_THEN
                                + found
                                + "+"
                                + start_index
                                + "-1"
                                + SQL_ELSE
                                + "-1"
                                + SQL_END
                            )
                        },
                    }
                ]
            )
