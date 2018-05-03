# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals


from jx_base.expressions import jx_expression
from jx_base.queries import is_variable_name
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times import Date, MONTH


class TestExpressions(FuzzyTestCase):

    def test_error_on_bad_var(self):
        self.assertFalse(
            is_variable_name(u'coalesce(rows[rownum+1].timestamp, Date.eod())'),
            "That's not a valid variable name!!"
        )

    def test_good_var(self):
        self.assertTrue(
            is_variable_name(u'_a._b'),
            "That's a good variable name!"
        )

    def test_dash_var(self):
        self.assertTrue(
            is_variable_name(u'a-b'),
            "That's a good variable name!"
        )

    def test_value_not_a_variable(self):
        result = jx_expression({"eq": {"result.test": "/XMLHttpRequest/send-entity-body-document.htm"}}).vars()
        expected = {"result.test"}
        self.assertEqual(result, expected, "expecting the one and only variable")

    def test_in_map(self):
        where = {"in": {"a": [1, 2]}}
        result = jx_expression(where).map({"a": "c"}).__data__()
        self.assertEqual(result, {"in": {"c": [1, 2]}})

    def test_date_literal(self):
        expr = {"date": {"literal": "today-month"}}

        from jx_python.expression_compiler import compile_expression
        result = compile_expression(jx_expression(expr).partial_eval().to_python())(None)
        expected = (Date.today()-MONTH).unix
        self.assertEqual(result, expected)
