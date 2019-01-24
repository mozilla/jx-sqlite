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

from jx_base.expressions import NULL
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestGroupBy2(BaseTestCase):
    def test_count_rows(self):
        test = {
            "data": two_dim_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"aggregate": "count"},
                "groupby": ["a", "b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "x", "b": "m", "count": 2},
                    {"a": "x", "b": "n", "count": 1},
                    {"a": "x", "b": NULL, "count": 1},
                    {"a": "y", "b": "m", "count": 1},
                    {"a": "y", "b": "n", "count": 2},
                    {"a": "y", "b": NULL, "count": 1},
                    {"a": NULL, "b": "m", "count": 1},
                    {"a": NULL, "b": "n", "count": 1},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "count"],
                "data": [
                    ["x", "m", 2],
                    ["x", "n", 1],
                    ["x", NULL, 1],
                    ["y", "m", 1],
                    ["y", "n", 2],
                    ["y", NULL, 1],
                    [NULL, "m", 1],
                    [NULL, "n", 1],
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_sum_rows(self):
        test = {
            "name": "sum rows",
            "metadata": {},
            "data": two_dim_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "sum"},
                "groupby": ["a", "b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "x", "b": "m", "v": 29},
                    {"a": "x", "b": "n", "v": 3},
                    {"a": "x", "b": NULL, "v": 5},
                    {"a": "y", "b": "m", "v": 7},
                    {"a": "y", "b": "n", "v": 50},
                    {"a": "y", "b": NULL, "v": 13},
                    {"a": NULL, "b": "m", "v": 17},
                    {"a": NULL, "b": "n", "v": 19}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "v"],
                "data": [
                    ["x", "m", 29],
                    ["x", "n", 3],
                    ["x", NULL, 5],
                    ["y", "m", 7],
                    ["y", "n", 50],
                    ["y", NULL, 13],
                    [NULL, "m", 17],
                    [NULL, "n", 19]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_sum_rows_w_domain(self):
        test = {
            "name": "sum rows",
            "metadata": {},
            "data": two_dim_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "sum"},
                "groupby": ["a", "b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "x", "b": "m", "v": 29},
                    {"a": "x", "b": "n", "v": 3},
                    {"a": "x", "b": NULL, "v": 5},
                    {"a": "y", "b": "m", "v": 7},
                    {"a": "y", "b": "n", "v": 50},
                    {"a": "y", "b": NULL, "v": 13},
                    {"a": NULL, "b": "m", "v": 17},
                    {"a": NULL, "b": "n", "v": 19},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "v"],
                "data": [
                    ["x", "m", 29],
                    ["x", "n", 3],
                    ["x", NULL, 5],
                    ["y", "m", 7],
                    ["y", "n", 50],
                    ["y", NULL, 13],
                    [NULL, "m", 17],
                    [NULL, "n", 19],
                ]
            }
        }
        self.utils.execute_tests(test)


# TODO:  APPEARS THERE IS A COLUMN SWAP PROBLEM, NOTICE THE QUERY IS DEEP
# {
#     "from": "coverage.source.file.covered",
#     "where": {"and": [
#         {"missing": "source.method.name"},
#         {"eq": {
#             "source.file.name": not_summarized.source.file.name,
#             "build.revision12": not_summarized.build.revision12
#         }},
#     ]},
#     "groupby": [
#         "line",
#         "test.url"
#     ],
#     "limit": 100000,
#     "format": "list"
# })



two_dim_test_data = [
    {"a": "x", "b": "m", "v": 2},
    {"a": "x", "b": "n", "v": 3},
    {"a": "x", "b": NULL, "v": 5},
    {"a": "y", "b": "m", "v": 7},
    {"a": "y", "b": "n", "v": 11},
    {"a": "y", "b": NULL, "v": 13},
    {"a": NULL, "b": "m", "v": 17},
    {"a": NULL, "b": "n", "v": 19},
    {"a": "x", "b": "m", "v": 27},
    {"a": "y", "b": "n", "v": 39}
]

metadata = {
    "properties": {
        "a": {
            "type": "string",
            "domain": {
                "type": "set",
                "partitions": ["x", "y", "z"]
            }
        },
        "b": {
            "type": "string",
            "domain": {
                "type": "set",
                "partitions": ["m", "n"]
            }
        }
    }
}

