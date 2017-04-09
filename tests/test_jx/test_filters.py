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

from mo_dots import wrap
from tests.test_jx import BaseTestCase, TEST_TABLE

lots_of_data = wrap([{"a": i} for i in range(30)])


class TestFilters(BaseTestCase):
    def test_where_expression(self):
        test = {
            "data": [  # PROPERTIES STARTING WITH _ ARE NESTED AUTOMATICALLY
                {"a": {"b": 0, "c": 0}},
                {"a": {"b": 0, "c": 1}},
                {"a": {"b": 1, "c": 0}},
                {"a": {"b": 1, "c": 1}},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"eq": ["a.b", "a.c"]},
                "sort": "a.b"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a.b": 0, "a.c": 0},
                {"a.b": 1, "a.c": 1},
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.c"],
                "data": [[0, 0], [1, 1]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 2, "interval": 1}
                    }
                ],
                "data": {
                    "a.b": [0, 1],
                    "a.c": [0, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_add_expression(self):
        test = {
            "data": [  # PROPERTIES STARTING WITH _ ARE NESTED AUTOMATICALLY
                {"a": {"b": 0, "c": 0}},
                {"a": {"b": 0, "c": 1}},
                {"a": {"b": 1, "c": 0}},
                {"a": {"b": 1, "c": 1}},
            ],
            "query": {
                "select": "*",
                "from": TEST_TABLE,
                "where": {"eq": [{"add": ["a.b", 1]}, "a.c"]},
                "sort": "a.b"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                    {"a.b": 0, "a.c": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.c"],
                "data": [[0, 1]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "a.b": [0],
                    "a.c": [1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_regexp_expression(self):
        test = {
            "data": [{"_a": [
                {"a": "abba"},
                {"a": "aaba"},
                {"a": "aaaa"},
                {"a": "aa"},
                {"a": "aba"},
                {"a": "aa"},
                {"a": "ab"},
                {"a": "ba"},
                {"a": "a"},
                {"a": "b"}
            ]}],
            "query": {
                "from": TEST_TABLE+"._a",
                "select": "*",
                "where": {"regex": {"a": ".*b.*"}},
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": "abba"},
                {"a": "aaba"},
                {"a": "aba"},
                {"a": "ab"},
                {"a": "ba"},
                {"a": "b"}
            ]}
        }
        self.utils.execute_es_tests(test)

    def test_empty_or(self):
        test = {
            "data": [{"a": 1}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"or": []}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": []
            }
        }
        self.utils.execute_es_tests(test)


    def test_empty_and(self):
        test = {
            "data": [{"a": 1}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"and": []}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"a": 1}]
            }
        }
        self.utils.execute_es_tests(test)


    def test_empty_in(self):
        test = {
            "data": [{"a": 1}],
            "query": {
                "select": "a",
                "from": TEST_TABLE,
                "where": {"in": {"a": []}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": []
            }
        }
        self.utils.execute_es_tests(test)


    def test_empty_match_all(self):
        test = {
            "data": [{"a": 1}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"match_all": {}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"a": 1}]
            }
        }
        self.utils.execute_es_tests(test)



# TODO:  ADD TEST TO ENSURE BOOLEAN COLUMNS (WITH 'T' and 'F' VALUES) CAN BE USED WITH true AND false FILTERS
