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


class TestEdge2(BaseTestCase):
    def test_count_rows(self):
        test = {
            "name": "count rows, 2d",
            "metadata": {},
            "data": two_dim_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"aggregate": "count"},
                "edges": [
                    {"value": "a", "domain": {"type": "set", "partitions": ["x", "y", "z"]}},
                    "b"
                ]
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
                    {"a": "z", "b": NULL, "count": 0},
                    {"a": NULL, "b": NULL, "count": 0},
                    {"a": "z", "b": "m", "count": 0},
                    {"a": "z", "b": "n", "count": 0}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "count"],
                "data": [
                    [NULL, "m", 1],
                    [NULL, "n", 1],
                    ["x", NULL, 1],
                    ["x", "m", 2],
                    ["x", "n", 1],
                    ["y", NULL, 1],
                    ["y", "m", 1],
                    ["y", "n", 2],
                    ["z", NULL, 0],
                    [NULL, NULL, 0],
                    ["z", "m", 0],
                    ["z", "n", 0]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "x",
                                    "value": "x"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "y",
                                    "value": "y"
                                },
                                {
                                    "dataIndex": 2,
                                    "name": "z",
                                    "value": "z"
                                }
                            ]
                        }
                    },
                    {
                        "name": "b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "m",
                                    "value": "m"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "n",
                                    "value": "n"
                                }
                            ]
                        }
                    }
                ],
                "data": {
                    "count": [
                        [2, 1, 1],
                        [1, 2, 1],
                        [0, 0, 0],
                        [1, 1, 0]
                    ]
                }
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
                "edges": ["a", "b"]
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
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {"name": "x", "value": "x", "dataIndex": 0},
                                {"name": "y", "value": "y", "dataIndex": 1}
                            ]
                        }
                    },
                    {
                        "name": "b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {"name": "m", "value": "m", "dataIndex": 0},
                                {"name": "n", "value": "n", "dataIndex": 1}
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [
                        [29, 3, 5],
                        [7, 50, 13],
                        [17, 19, NULL]
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_avg_rows_w_default(self):
        test = {
            "metadata": {},
            "data": [
                {"a": "x", "b": "m", "v": 2},
                {"a": "x", "b": "m"},
                {"a": "x", "b": "n", "v": 3},
                {"a": "x"},
                {"a": "y", "b": "m", "v": 7},
                {"a": "y", "b": "n"},
                {"a": "y", "b": "n"},
                {"a": "y", "v": 13},
                {"b": "m", "v": 17},
                {"b": "n", "v": 19}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "average", "default": 0},
                "edges": ["a", "b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "x", "b": "m", "v": 2},
                    {"a": "x", "b": "n", "v": 3},
                    {"a": "x", "v": 0},
                    {"a": "y", "b": "m", "v": 7},
                    {"a": "y", "b": "n", "v": 0},
                    {"a": "y", "v": 13},
                    {"b": "m", "v": 17},
                    {"b": "n", "v": 19}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "b", "v"],
                "data": [
                    ["x", NULL, 0],
                    ["x", "m", 2],
                    ["x", "n", 3],
                    ["y", NULL, 13],
                    ["y", "m", 7],
                    ["y", "n", 0],
                    [NULL, "m", 17],
                    [NULL, "n", 19]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "x",
                                    "value": "x"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "y",
                                    "value": "y"
                                }
                            ]
                        }
                    },
                    {
                        "name": "b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "m",
                                    "value": "m"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "n",
                                    "value": "n"
                                }
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [
                        [2, 3, 0],
                        [7, 0, 13],
                        [17, 19, 0]
                    ]
                }
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
                "edges": [
                    {
                        "value": "a",
                        "domain": {
                            "type": "set",
                            "partitions": ["x", "y", "z"]
                        }
                    },
                    {
                        "value": "b",
                        "domain": {
                            "type": "set",
                            "partitions": ["m", "n"]
                        }
                    }
                ]
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
                    {"a": "z", "b": "m", "v": NULL},
                    {"a": "z", "b": "n", "v": NULL},
                    {"a": "z", "b": NULL, "v": NULL},
                    {"a": NULL, "b": "m", "v": 17},
                    {"a": NULL, "b": "n", "v": 19},
                    {"a": NULL, "b": NULL, "v": NULL}
                ]
            },
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
                    ["z", "m", NULL],
                    ["z", "n", NULL],
                    ["z", NULL, NULL],
                    [NULL, "m", 17],
                    [NULL, "n", 19],
                    [NULL, NULL, NULL]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"name": "x", "value": "x", "dataIndex": 0},
                                {"name": "y", "value": "y", "dataIndex": 1},
                                {"name": "z", "value": "z", "dataIndex": 2}
                            ]
                        }
                    },
                    {
                        "name": "b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"name": "m", "value": "m", "dataIndex": 0},
                                {"name": "n", "value": "n", "dataIndex": 1}
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [
                        [29, 3, 5],
                        [7, 50, 13],
                        [NULL, NULL, NULL],
                        [17, 19, NULL]
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_edge_using_missing_between2(self):
        test = {
            "data": [
                {"url": None},
                {"url": "/"},
                {"url": "https://hg.mozilla.org/"},
                {"url": "https://hg.mozilla.org/a/"},
                {"url": "https://hg.mozilla.org/b/"},
                {"url": "https://hg.mozilla.org/b/1"},
                {"url": "https://hg.mozilla.org/b/2"},
                {"url": "https://hg.mozilla.org/b/3"},
                {"url": "https://hg.mozilla.org/c/"},
                {"url": "https://hg.mozilla.org/d"},
                {"url": "https://hg.mozilla.org/e"}
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": [
                    {
                        "name": "filename",
                        "value": {
                            "when": {"missing": {"between": {"url": ["https://hg.mozilla.org/", "/"]}}},
                            "then": "url"
                        }
                    },
                    {
                        "name": "subdir",
                        "value": {"between": {"url": ["https://hg.mozilla.org/", "/"]}}
                    }
                ],
                "where": {"prefix": {"url": "https://hg.mozilla.org/"}},
                "limit": 100
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"filename": "https://hg.mozilla.org/d", "subdir": NULL, "count": 1},
                    {"filename": "https://hg.mozilla.org/e", "subdir": NULL, "count": 1},
                    {"filename": "https://hg.mozilla.org/", "subdir": NULL, "count": 1},
                    {"subdir": "a", "count": 1},
                    {"subdir": "b", "count": 4},
                    {"subdir": "c", "count": 1}
                ]}

        }
        self.utils.execute_tests(test)

    def test_edge_using_missing_between1(self):
        test = {
            "data": [
                {"url": None},
                {"url": "/"},
                {"url": "https://hg.mozilla.org/"},
                {"url": "https://hg.mozilla.org/a/"},
                {"url": "https://hg.mozilla.org/b/"},
                {"url": "https://hg.mozilla.org/b/1"},
                {"url": "https://hg.mozilla.org/b/2"},
                {"url": "https://hg.mozilla.org/b/3"},
                {"url": "https://hg.mozilla.org/c/"},
                {"url": "https://hg.mozilla.org/d"},
                {"url": "https://hg.mozilla.org/e"}
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": [
                    {
                        "name": "filename",
                        "value": {
                            "when": {"missing": {"between": {"url": ["https://hg.mozilla.org/", "/"]}}},
                            "then": "url"
                        }
                    }
                ],
                "where": {"prefix": {"url": "https://hg.mozilla.org/"}},
                "limit": 100
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"filename": "https://hg.mozilla.org/d", "subdir": NULL, "count": 1},
                    {"filename": "https://hg.mozilla.org/e", "subdir": NULL, "count": 1},
                    {"filename": "https://hg.mozilla.org/", "subdir": NULL, "count": 1},
                    {"count": 6}
                ]}

        }
        self.utils.execute_tests(test)

    def test_edge_find_w_start(self):
        test = {
            "data": [{"url": "/"}],
            "query": {
                "from": TEST_TABLE,
                "groupby": [
                    {
                        "name": "suffix missing",
                        "value": {"missing": {"find": ["url", {"literal": "/"}], "start": {"literal": 23}}}
                    }
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [{"suffix missing": True}]}

        }
        self.utils.execute_tests(test)


two_dim_test_data = [
    {"a": "x", "b": "m", "v": 2},
    {"a": "x", "b": "n", "v": 3},
    {"a": "x", "b": None, "v": 5},
    {"a": "y", "b": "m", "v": 7},
    {"a": "y", "b": "n", "v": 11},
    {"a": "y", "b": None, "v": 13},
    {"a": None, "b": "m", "v": 17},
    {"a": None, "b": "n", "v": 19},
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
