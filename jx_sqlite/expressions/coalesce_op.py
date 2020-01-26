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

from jx_base.expressions import CoalesceOp as CoalesceOp_
from jx_sqlite.expressions._utils import SQLang, check
from mo_dots import wrap
from mo_sql import sql_coalesce


class CoalesceOp(CoalesceOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        acc = {"b": [], "s": [], "n": [], "0": []}

        for term in self.terms:
            for t, v in SQLang[term].to_sql(schema)[0].sql.items():
                acc[t].append(v)

        output = {}
        for t, terms in acc.items():
            if not terms:
                continue
            elif len(terms) == 1:
                output[t] = terms[0]
            else:
                output[t] = sql_coalesce(terms)
        return wrap([{"name": ".", "sql": output}])
