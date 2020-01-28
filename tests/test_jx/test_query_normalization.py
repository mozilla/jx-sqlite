# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

from jx_base.query import _normalize_edges, _normalize_select
from mo_dots import Null
from mo_json import json2value, value2json
from mo_testing.fuzzytestcase import FuzzyTestCase


class TestQueryNormalization(FuzzyTestCase):
    def test_complex_edge_with_no_name(self):
        edge = {"value": ["a", "c"]}
        self.assertRaises(Exception, _normalize_edges, edge)

    def test_complex_edge_value(self):
        edge = {"name": "n", "value": ["a", "c"]}

        result = json2value(value2json(_normalize_edges(edge, Null)[0]))
        expected = {
            "name": "n",
            "value": {"tuple": ["a", "c"]},
            "domain": {"dimension": {"fields": ["a", "c"]}}
        }
        self.assertEqual(result, expected)

    def test_naming_select(self):
        select = {"value": "result.duration", "aggregate": "avg"}
        result = _normalize_select(select, None)
        #DEEP NAMES ARE ALLOWED, AND NEEDED TO BUILD STRUCTURE FROM A QUERY
        expected = [{"name": "result.duration", "value": "result.duration", "aggregate": "average"}]
        self.assertEqual(result, expected)
