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

from mo_testing.fuzzytestcase import FuzzyTestCase

from pyLibrary.queries.query import _normalize_edge, _normalize_select
from tests.test_jx import NULL


class TestQueryNormalization(FuzzyTestCase):
    def test_complex_edge_with_no_name(self):
        edge = {"value": ["a", "c"]}
        self.assertRaises(Exception, _normalize_edge, edge)

    def test_complex_edge_value(self):
        edge = {"name": "n", "value": ["a", "c"]}

        result = _normalize_edge(edge)
        expected = {"name": "n", "domain": {"dimension": {"fields": ["a", "c"]}}}
        self.assertEqual(result, expected)
        self.assertEqual(result.value, NULL)

    def test_naming_select(self):
        select = {"value": "result.duration", "aggregate": "avg"}
        result = _normalize_select(select, None)
        #DEEP NAMES ARE ALLOWED, AND NEEDED TO BUILD STRUCTURE FROM A QUERY
        expected = [{"name": "result.duration", "value": "result.duration", "aggregate": "average"}]
        self.assertEqual(result, expected)
