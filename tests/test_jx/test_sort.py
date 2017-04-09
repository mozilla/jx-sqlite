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

from mo_logs import Log

from mo_logs.exceptions import extract_stack

from mo_dots import wrap

from tests.test_jx import BaseTestCase, TEST_TABLE, NULL

lots_of_data = wrap([{"a": i} for i in range(30)])


class TestSorting(BaseTestCase):

    def test_name_and_direction_sort(self):
        test = {
            "data": [
                {"a": 1},
                {"a": 3},
                {"a": 4},
                {"a": 6},
                {"a": 2}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a",
                "sort": {"a": "desc"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [6, 4, 3, 2, 1]
            }
        }
        self.utils.execute_es_tests(test)

    def test_edge_and_sort(self):
        test = {
            "data": [
                {"a": "c", "value": 1},
                {"a": "c", "value": 3},
                {"a": "c", "value": 4},
                {"a": "c", "value": 6},
                {"a": "a", "value": 7},
                {"value": 99},
                {"a": "a", "value": 8},
                {"a": "a", "value": 9},
                {"a": "a", "value": 10},
                {"a": "a", "value": 11}
            ],
            "query": {
                "from": TEST_TABLE,
                "edges": "a",
                "sort": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "a", "count": 5},
                    {"a": "c", "count": 4},
                    {"count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["a", 5],
                    ["c", 4],
                    [NULL, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [{"name": "a", "domain": {"type": "set", "partitions": [
                    {"value": "a"},
                    {"value": "c"}
                ]}}],
                "data": {
                    "count": [5, 4, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_2edge_and_sort(self):
        test = {
            "data": [
                {"a": "c", "b": 0, "value": 1},
                {"a": "c", "b": 0, "value": 3},
                {"a": "c", "b": 1, "value": 4},
                {"a": "c", "b": 1, "value": 6},
                {"a": "a", "b": 1, "value": 7},
                {"a": "a", "value": 20},
                {"b": 1, "value": 21},
                {"value": 22},
                {"a": "a", "b": 0, "value": 8},
                {"a": "a", "b": 0, "value": 9},
                {"a": "a", "b": 1, "value": 10},
                {"a": "a", "b": 1, "value": 11}
            ],
            "query": {
                "from": TEST_TABLE,
                "edges": ["a", "b"],
                "sort": [{"a": "desc"}, {"b": "desc"}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "c", "b": 1, "count": 2},
                    {"a": "c", "b": 0, "count": 2},
                    {"a": "a", "b": 1, "count": 3},
                    {"a": "a", "b": 0, "count": 2},
                    {"a": "a", "count": 1},
                    {"b": 1, "count": 1},
                    {"count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "count"],
                "data": [
                    ["c", 1, 2],
                    ["c", 0, 2],
                    ["a", 1, 3],
                    ["a", 0, 2],
                    ["a", NULL, 1],
                    [NULL, 1, 1],
                    [NULL, NULL, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "b", "domain": {"type": "set", "partitions": [
                        {"value": 0},
                        {"value": 1}
                    ]}},
                    {"name": "a", "domain": {"type": "set", "partitions": [
                        {"value": "a"},
                        {"value": "c"}
                    ]}}
                ],
                "data": {
                    "count": [[2, 2, 0], [3, 2, 1], [1, 0, 1]]
                }
            }
        }

        subtest = wrap(test)
        subtest.name = extract_stack()[0]['method']
        self.utils.fill_container(test)

        test = wrap(test)
        self.utils.send_queries({"query": test.query, "expecting_list": test.expecting_list})
        self.utils.send_queries({"query": test.query, "expecting_table": test.expecting_table})
        try:
            self.utils.send_queries({"query": test.query, "expecting_cube": test.expecting_cube})
            Log.error("expecting error regarding sorting edges")
        except Exception, e:
            pass

    def test_groupby_and_sort(self):
        test = {
            "data": [
                {"a": "c", "value": 1},
                {"a": "c", "value": 3},
                {"a": "c", "value": 4},
                {"a": "c", "value": 6},
                {"a": "a", "value": 7},
                {"value": 99},
                {"a": "a", "value": 8},
                {"a": "a", "value": 9},
                {"a": "a", "value": 10},
                {"a": "a", "value": 11}
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": "a",
                "sort": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "a", "count": 5},
                    {"a": "c", "count": 4},
                    {"count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["a", 5],
                    ["c", 4],
                    [NULL, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [{"name": "a", "domain": {"type": "set", "partitions": [
                    {"value": "a"},
                    {"value": "c"}
                ]}}],
                "data": {
                    "count": [5, 4, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_groupby2_and_sort(self):
        test = {
            "data": [
                {"a": "c", "b": 1, "value": 1},
                {"a": "c", "b": 2, "value": 3},
                {"a": "c", "value": 4},
                {"a": "c", "b": 1, "value": 6},
                {"a": "a", "b": 1, "value": 7},
                {"value": 99},
                {"a": "a", "b": 1, "value": 8},
                {"a": "a", "b": 2, "value": 9},
                {"a": "a", "b": 2, "value": 10},
                {"a": "a", "value": 11}
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": ["a", "b"],
                "sort": [{"b": "desc"}, {"a": "asc"}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "a", "b": 2, "count": 2},
                    {"a": "c", "b": 2, "count": 1},
                    {"a": "a", "b": 1, "count": 2},
                    {"a": "c", "b": 1, "count": 2},
                    {"a": "a", "b": NULL, "count": 1},
                    {"a": "c", "b": NULL, "count": 1},
                    {"count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "count"],
                "data": [
                    ["a", 2, 2],
                    ["c", 2, 1],
                    ["a", 1, 2],
                    ["c", 1, 2],
                    ["a", NULL, 1],
                    ["c", NULL, 1],
                    [NULL, NULL, 1]
                ]
            }
        }
        self.utils.execute_es_tests(test)

    def test_groupby2b_and_sort(self):
        test = {
            "data": [
                {"a": "c", "b": 0, "value": 1},
                {"a": "c", "b": 0, "value": 3},
                {"a": "c", "b": 1, "value": 4},
                {"a": "c", "b": 1, "value": 6},
                {"a": "a", "b": 1, "value": 7},
                {"a": "a", "value": 20},
                {"b": 1, "value": 21},
                {"value": 22},
                {"a": "a", "b": 0, "value": 8},
                {"a": "a", "b": 0, "value": 9},
                {"a": "a", "b": 1, "value": 10},
                {"a": "a", "b": 1, "value": 11}
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": ["a", "b"],
                "sort": [{"a": "desc"}, {"b": "desc"}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "c", "b": 1, "count": 2},
                    {"a": "c", "b": 0, "count": 2},
                    {"a": "a", "b": 1, "count": 3},
                    {"a": "a", "b": 0, "count": 2},
                    {"a": "a", "count": 1},
                    {"b": 1, "count": 1},
                    {"count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "count"],
                "data": [
                    ["c", 1, 2],
                    ["c", 0, 2],
                    ["a", 1, 3],
                    ["a", 0, 2],
                    ["a", NULL, 1],
                    [NULL, 1, 1],
                    [NULL, NULL, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "b", "domain": {"type": "set", "partitions": [
                        {"value": 1},
                        {"value": 0}
                    ]}},
                    {"name": "a", "domain": {"type": "set", "partitions": [
                        {"value": "c"},
                        {"value": "a"}
                    ]}}
                ],
                "data": {
                    "count": [[2, 2, 0], [3, 2, 1], [1, 0, 1]]
                }
            }
        }
        self.utils.execute_es_tests(test)
