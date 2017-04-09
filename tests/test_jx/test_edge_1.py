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

from unittest import skipIf

from tests.test_jx import BaseTestCase, TEST_TABLE, global_settings, NULL


class TestEdge1(BaseTestCase):

    def test_no_select(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 2},
                    {"a": "c", "count": 3},
                    {"a": NULL, "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [NULL, 1]
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
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count": [2, 3, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_count_rows(self):
        test = {
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"aggregate": "count"},
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 2},
                    {"a": "c", "count": 3},
                    {"a": NULL, "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [NULL, 1]
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
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count": [2, 3, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_count_self(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "count_a", "value": "a", "aggregate": "count"},
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count_a": 2},
                    {"a": "c", "count_a": 3},
                    {"count_a": 0}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count_a"],
                "data": [
                    ["b", 2],
                    ["c", 3],
                    [NULL, 0]
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
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count_a": [2, 3, 0]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_count_other(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "count_v", "value": "v", "aggregate": "count"},
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count_v": 1},
                    {"a": "c", "count_v": 3},
                    {"count_v": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count_v"],
                "data": [
                    ["b", 1],
                    ["c", 3],
                    [NULL, 1]
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
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count_v": [1, 3, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)


    def test_sum_default(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": [
                {"a": "c", "v": 13},
                {"a": "b"},
                {"v": 3},
                {"a": "b"},
                {"a": "c", "v": 7},
                {"a": "c", "v": 11}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "sum_v", "value": "v", "aggregate": "sum", "default": -1},
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "sum_v": -1},
                    {"a": "c", "sum_v": 31},
                    {"sum_v": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "sum_v"],
                "data": [
                    ["b", -1],
                    ["c", 31],
                    [NULL, 3]
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
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "sum_v": [-1, 31, 3]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_select_2(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "count", "value": "v", "aggregate": "count"},
                    {"name": "avg", "value": "v", "aggregate": "average"}
                ],
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 1, "avg": 2},
                    {"a": "c", "count": 3, "avg": 31 / 3},
                    {"count": 1, "avg": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "count", "avg"],
                "data": [
                    ["b", 1, 2],
                    ["c", 3, 31 / 3],
                    [NULL, 1, 3]
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
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "count": [1, 3, 1],
                    "avg": [2, 31 / 3, 3]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_select_3(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": structured_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "v", "value": "v", "aggregate": "sum"},
                    {"name": "d", "value": "b.d", "aggregate": "sum"}
                ],
                "edges": ["b.r"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": 6, "d": 6, "b": {"r": "a"}},
                    {"v": 15, "d": 6, "b": {"r": "b"}},
                    {"v": 24, "d": 6, "b": {"r": "c"}},
                    {"v": 33, "d": 6, "b": {"r": "d"}},
                    {"v": 13, "d": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["v", "d", "b.r"],
                "data": [
                    [6, 6, "a"],
                    [15, 6, "b"],
                    [24, 6, "c"],
                    [33, 6, "d"],
                    [13, 3, NULL]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "b.r",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "a"}, {"value": "b"}, {"value": "c"}, {"value": "d"}]
                        }
                    }
                ],
                "data": {
                    "v": [6, 15, 24, 33, 13],
                    "d": [6, 6, 6, 6, 3, NULL]
                }
            }
        }
        self.utils.execute_es_tests(test)


    def test_select_4(self):
        test = {
            "name": "count column",
            "metadata": {},
            "data": structured_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "v", "value": "v", "aggregate": "min"},
                    {"name": "d", "value": "b.d", "aggregate": "max"}
                ],
                "edges": ["b.r"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": 1, "d": 3, "b": {"r": "a"}},
                    {"v": 4, "d": 3, "b": {"r": "b"}},
                    {"v": 7, "d": 3, "b": {"r": "c"}},
                    {"v": 10, "d": 3, "b": {"r": "d"}},
                    {"v": 13, "d": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["v", "d", "b.r"],
                "data": [
                    [1, 3, "a"],
                    [4, 3, "b"],
                    [7, 3, "c"],
                    [10, 3, "d"],
                    [13, 3, NULL]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "b.r",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "a"}, {"value": "b"}, {"value": "c"}, {"value": "d"}]
                        }
                    }
                ],
                "data": {
                    "v": [1, 4, 7, 10, 13],
                    "d": [3, 3, 3, 3, 3, NULL]
                }
            }
        }
        self.utils.execute_es_tests(test)


    def test_sum_column(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "sum"},
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "v": 2},
                    {"a": "c", "v": 31},
                    {"a": NULL, "v": 3}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["b", 2],
                    ["c", 31],
                    [NULL, 3]
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
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "v": [2, 31, 3]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_where(self):
        # THE CONTAINER SHOULD RETURN THE FULL CUBE, DESPITE IT NOT BEING EXPLICIT
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "edges": ["a"],
                "where": {"term": {"a": "c"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b"},
                    {"a": "c", "v": 13},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["b", NULL],
                    ["c", 13]
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
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "v": [NULL, 13, NULL]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_where_w_dimension(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "edges": [
                    {"value": "a", "allowNulls": False, "domain": {"type": "set", "partitions": ["b", "c"]}}
                ],
                "where": {"term": {"a": "c"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b"},
                    {"a": "c", "v": 13}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["b", NULL],
                    ["c", 13]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": False,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "b"}, {"value": "c"}]
                        }
                    }
                ],
                "data": {
                    "v": [NULL, 13],
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_select_empty_list(self):
        test = {
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": [],
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b"},
                    {"a": "c"},
                    {}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [
                    ["b"],
                    ["c"],
                    [NULL]
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
                                {
                                    "dataIndex": 0,
                                    "name": "b",
                                    "value": "b"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "c",
                                    "value": "c"
                                 }
                            ]
                        }
                    }
                ],
                "data": {
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_empty_select(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {},
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b"},
                    {"a": "c"},
                    {}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [
                    ["b"],
                    ["c"],
                    [NULL]
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
                                {
                                    "dataIndex": 0,
                                    "name": "b",
                                    "value": "b"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "c",
                                    "value": "c"
                                 }
                            ]
                        }
                    }
                ],
                "data": {
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_empty_select_w_dot_edge(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {},
                "edges": {"name":".", "value":"a"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    "b",
                    "c",
                    NULL
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["."],
                "data": [
                    ["b"],
                    ["c"],
                    [NULL]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": ".",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "name": "b",
                                    "value": "b"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "c",
                                    "value": "c"
                                 }
                            ]
                        }
                    }
                ],
                "data": {
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_empty_default_domain(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "edges": ["a"],
                "where": {"term": {"a": "d"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b"},
                    {"a": "c"}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": [
                    ["b", NULL],
                    ["c", NULL]
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
                                {
                                    "dataIndex": 0,
                                    "name": "b",
                                    "value": "b"
                                },
                                {
                                    "dataIndex": 1,
                                    "name": "c",
                                    "value": "c"
                                 }
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [NULL, NULL, NULL]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_empty_default_domain_w_groupby(self):
        test = {
            "metadata": {},
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "groupby": ["a"],
                "where": {"term": {"a": "d"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": []
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "v"],
                "data": []
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "allowNulls": True,  # MUST BE FALSE, cube FORMAT CAN NOT CHANGE WHAT'S AVAILABLE
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {
                                    "dataIndex": 0,
                                    "value": "b"
                                },
                                {
                                    "dataIndex": 1,
                                    "value": "c"
                                }
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [NULL, NULL, NULL]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_default_limit(self):
        """
        TEST THAT THE DEFAULT LIMIT IS APPLIED
        """
        test = {
            "metadata": {},
            "data": long_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "edges": ["k"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"k": "a", "v": 1},
                    {"k": "b", "v": 2},
                    {"k": "c", "v": 3},
                    {"k": "d", "v": 4},
                    {"k": "e", "v": 5},
                    {"k": "f", "v": 6},
                    {"k": "g", "v": 7},
                    {"k": "h", "v": 8},
                    {"k": "i", "v": 9},
                    {"k": "j", "v": 10},
                    {"v":13}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["k", "v"],
                "data": [
                    ["a", 1],
                    ["b", 2],
                    ["c", 3],
                    ["d", 4],
                    ["e", 5],
                    ["f", 6],
                    ["g", 7],
                    ["h", 8],
                    ["i", 9],
                    ["j", 10],
                    [NULL, 13]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "k",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"value": "a"},
                                {"value": "b"},
                                {"value": "c"},
                                {"value": "d"},
                                {"value": "e"},
                                {"value": "f"},
                                {"value": "g"},
                                {"value": "h"},
                                {"value": "i"},
                                {"value": "j"}
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_edge_limit_big(self):
        test = {
            "data": long_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "edges": [{"value": "k", "domain": {"type": "default", "limit": 100}}],
                "limit": 100
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"k": "a", "v": 1},
                    {"k": "b", "v": 2},
                    {"k": "c", "v": 3},
                    {"k": "d", "v": 4},
                    {"k": "e", "v": 5},
                    {"k": "f", "v": 6},
                    {"k": "g", "v": 7},
                    {"k": "h", "v": 8},
                    {"k": "i", "v": 9},
                    {"k": "j", "v": 10},
                    {"k": "k", "v": 11},
                    {"k": "l", "v": 12},
                    {"k": "m", "v": 13}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["k", "v"],
                "data": [
                    ["a", 1],
                    ["b", 2],
                    ["c", 3],
                    ["d", 4],
                    ["e", 5],
                    ["f", 6],
                    ["g", 7],
                    ["h", 8],
                    ["i", 9],
                    ["j", 10],
                    ["k", 11],
                    ["l", 12],
                    ["m", 13]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "k",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"value": "a"},
                                {"value": "b"},
                                {"value": "c"},
                                {"value": "d"},
                                {"value": "e"},
                                {"value": "f"},
                                {"value": "g"},
                                {"value": "h"},
                                {"value": "i"},
                                {"value": "j"},
                                {"value": "k"},
                                {"value": "l"},
                                {"value": "m"}
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_edge_limit_small(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": long_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "edges": [{"value": "k", "domain": {"type": "default", "limit": 1}}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": 13},
                    {"k": "a", "v": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["k", "v"],
                "data": [
                    [NULL, 13],
                    ["a", 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "k",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"value": "a"}
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [1, 13]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_general_limit(self):
        test = {
            "name": "sum column",
            "metadata": {},
            "data": long_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "v", "aggregate": "max"},
                "edges": [{"value": "k", "domain": {"type": "default"}}],
                "limit": 5
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"k": "a", "v": 1},
                    {"k": "b", "v": 2},
                    {"k": "c", "v": 3},
                    {"k": "d", "v": 4},
                    {"k": "e", "v": 5},
                    {"v": 13}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["k", "v"],
                "data": [
                    [NULL, 13],
                    ["a", 1],
                    ["b", 2],
                    ["c", 3],
                    ["d", 4],
                    ["e", 5]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "k",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"value": "a"},
                                {"value": "b"},
                                {"value": "c"},
                                {"value": "d"},
                                {"value": "e"}
                            ]
                        }
                    }
                ],
                "data": {
                    "v": [1, 2, 3, 4, 5, 13]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_expression_on_edge(self):
        data = [
            {"s": 0, "r": 5},
            {"s": 1, "r": 2},
            {"s": 2, "r": 4},
            {"s": 3, "r": 5},
            {"s": 4, "r": 7},
            {"s": 2, "r": 5},
            {"s": 5, "r": 8}
        ]

        test = {
            "data": data,
            "query": {
                "from": TEST_TABLE,
                "select": {"aggregate": "count"},
                "edges": [{
                    "name": "start",
                    "value": {"sub": ["r", "s"]},
                    "domain": {"type": "range", "min": 0, "max": 6, "interval": 1}
                }]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"start": 0, "count": 0},
                    {"start": 1, "count": 1},
                    {"start": 2, "count": 2},
                    {"start": 3, "count": 3},
                    {"start": 4, "count": 0},
                    {"start": 5, "count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["start", "count"],
                "data": [
                    [0, 0],
                    [1, 1],
                    [2, 2],
                    [3, 3],
                    [4, 0],
                    [5, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "start",
                        "allowNulls": True,
                        "domain": {
                            "type": "range",
                            "key": "min",
                            "partitions": [
                                {"max": 1, "min": 0},
                                {"max": 2, "min": 1},
                                {"max": 3, "min": 2},
                                {"max": 4, "min": 3},
                                {"max": 5, "min": 4},
                                {"max": 6, "min": 5}
                            ]
                        }
                    }
                ],
                "data": {
                    "count": [0, 1, 2, 3, 0, 1, 0]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_float_range(self):
        data = [
            {"r": 0.5},
            {"r": 0.2},
            {"r": 0.4},
            {"r": 0.5},
            {"r": 0.7},
            {"r": 0.5},
            {"r": 0.8}
        ]

        test = {
            "data": data,
            "query": {
                "from": TEST_TABLE,
                "select": {"aggregate": "count"},
                "edges": [{
                    "name": "start",
                    "value": "r",
                    "domain": {"type": "range", "min": 0, "max": 0.6, "interval": 0.1}
                }]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"start": 0.0, "count": 0},
                    {"start": 0.1, "count": 0},
                    {"start": 0.2, "count": 1},
                    {"start": 0.3, "count": 0},
                    {"start": 0.4, "count": 1},
                    {"start": 0.5, "count": 3},
                    {"start": NULL, "count": 2}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["start", "count"],
                "data": [
                    [0.0, 0],
                    [0.1, 0],
                    [0.2, 1],
                    [0.3, 0],
                    [0.4, 1],
                    [0.5, 3],
                    [NULL, 2]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "start",
                        "allowNulls": True,
                        "domain": {
                            "type": "range",
                            "key": "min",
                            "partitions": [
                                {"max": 0.1, "min": 0.0},
                                {"max": 0.2, "min": 0.1},
                                {"max": 0.3, "min": 0.2},
                                {"max": 0.4, "min": 0.3},
                                {"max": 0.5, "min": 0.4},
                                {"max": 0.6, "min": 0.5}
                            ]
                        }
                    }
                ],
                "data": {
                    "count": [0, 0, 1, 0, 1, 3, 2]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_edge_using_expression(self):
        data = [
            {"r": "a", "s": "aa"},
            {"s": "bb"},
            {"r": "bb", "s": "bb"},
            {"r": "c", "s": "cc"},
            {"s": "dd"},
            {"r": "e", "s": "ee"},
            {"r": "e", "s": "ee"},
            {"r": "f"},
            {"r": "f"},
            {"k": 1}
        ]

        test = {
            "data": data,
            "query": {
                "from": TEST_TABLE,
                "edges": [{
                    "name": "v",
                    "value": {"coalesce": ["r", "s"]}
                }]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": "a", "count": 1},
                    {"v": "bb", "count": 2},
                    {"v": "c", "count": 1},
                    {"v": "dd", "count": 1},
                    {"v": "e", "count": 2},
                    {"v": "f", "count": 2},
                    {"v": NULL, "count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["v", "count"],
                "data": [
                    ["a", 1],
                    ["bb", 2],
                    ["c", 1],
                    ["dd", 1],
                    ["e", 2],
                    ["f", 2],
                    [NULL, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "v",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {"value": "a", "dataIndex": 0},
                                {"value": "bb", "dataIndex": 1},
                                {"value": "c", "dataIndex": 2},
                                {"value": "dd", "dataIndex": 3},
                                {"value": "e", "dataIndex": 4},
                                {"value": "f", "dataIndex": 5},
                            ]
                        }
                    }
                ],
                "data": {
                    "count": [1, 2, 1, 1, 2, 2, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_edge_using_between(self):
        test = {
            "data": [
                {"url": NULL},
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
                "groupby": {
                    "name": "subdir",
                    "value": {
                        "between": {
                            "url": [
                                "https://hg.mozilla.org/",
                                "/"
                            ]
                        }
                    }
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"subdir": NULL, "count": 5},
                    {"subdir": "a", "count": 1},
                    {"subdir": "b", "count": 4},
                    {"subdir": "c", "count": 1}
                ]}

        }
        self.utils.execute_es_tests(test)

    def test_edge_using_tuple(self):
        data = [
            {"r": "a", "s": "aa"},
            {          "s": "bb"},
            {"r": "b", "s": "bb"},
            {"r": "c", "s": "cc"},
            {          "s": "dd"},
            {"r": "e", "s": "ee"},
            {"r": "e", "s": "ee"},
            {"r": "f"},
            {"r": "f"},
            {"k": 1}
        ]

        test = {
            "data": data,
            "query": {
                "from": TEST_TABLE,
                "edges": [{
                    "name": "v",
                    "value": ["r", "s"]
                }]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": ["a", "aa"], "count": 1},
                    {"v": [NULL, "bb"], "count": 1},
                    {"v": ["b", "bb"], "count": 1},
                    {"v": ["c", "cc"], "count": 1},
                    {"v": [NULL, "dd"], "count": 1},
                    {"v": ["e", "ee"], "count": 2},
                    {"v": ["f", NULL], "count": 2},
                    {"v": [NULL, NULL], "count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["v", "count"],
                "data": [
                    [["a", "aa"], 1],
                    [[NULL, "bb"], 1],
                    [["b", "bb"], 1],
                    [["c", "cc"], 1],
                    [[NULL, "dd"], 1],
                    [["e", "ee"], 2],
                    [["f", NULL], 2],
                    [[NULL, NULL], 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "v",
                        "allowNulls": False,
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {"dataIndex": 0, "value": ["a", "aa"]},
                                {"dataIndex": 1, "value": ["b", "bb"]},
                                {"dataIndex": 2, "value": ["c", "cc"]},
                                {"dataIndex": 3, "value": ["e", "ee"]},
                                {"dataIndex": 4, "value": ["f", NULL]},
                                {"dataIndex": 5, "value": [NULL, "bb"]},
                                {"dataIndex": 6, "value": [NULL, "dd"]},
                                {"dataIndex": 7, "value": [NULL, NULL]}
                            ]
                        }
                    }
                ],
                "data": {
                    "count": [1, 1, 1, 2, 2, 1, 1, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    @skipIf(global_settings.use == "sqlite", "no median support")
    def test_percentile(self):
        test = {
            "data": [
                {"k": "a", "v": 1, "u": 5},
                {"k": "a", "v": 2, "u": 5},
                {"k": "a", "v": 3, "u": 5},
                {"k": "a", "v": 4, "u": 5},
                {"k": "a", "v": 5, "u": 5},
                {"k": "a", "v": 6, "u": 5},
                {"k": "b", "v": 7, "u": 5},
                {"k": "b", "v": 8, "u": 5},
                {"k": "b", "v": 9, "u": 5},
                {"k": "b", "v": 10, "u": 5},
                {"k": "b", "v": 11, "u": 5},
                {"k": "b", "v": 12, "u": 5},
                {"k": "b", "v": 13, "u": 5}
            ],
            "query": {
                "select": {"name": "v", "value": {"add": ["v", "u"]}, "aggregate": "percentile", "percentile": 0.70},
                "from": TEST_TABLE,
                "edges": ["k"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"k": "a", "v": 9.5},
                    {"k": "b", "v": 16.2}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["k", "v"],
                "data": [
                    ["a", 9.5],
                    ["b", 16.2]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "select": {"name": "v"},
                "edges": [
                    {"name": "k", "domain": {"type": "set", "partitions": [{"value": "a"}, {"value": "b"}]}}
                ],
                "data": {
                    "v": [9.5, 16.2]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_count_constant(self):
        test = {
            "data": [
                {"k": "a", "v": 1},
                {"k": "a", "v": 2},
                {"k": "a"},
                {"k": "b", "v": 3},
                {"k": "b"},
                {"k": "b"},
                {"v": 4}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name":"count", "value": 1, "aggregate": "count"},
                    {"value": "v", "aggregate": "count"}
                ],
                "edges": ["k"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"k": "a", "count": 3, "v": 2},
                    {"k": "b", "count": 3, "v": 1},
                    {"count": 1, "v": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["k", "count", "v"],
                "data": [
                    ["a", 3, 2],
                    ["b", 3, 1],
                    [NULL, 1, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "select": [
                    {"name": "count"},
                    {"name": "v"}
                ],
                "edges": [
                    {"name": "k", "domain": {"type": "set", "partitions": [{"value": "a"}, {"value": "b"}]}}
                ],
                "data": {
                    "v": [2, 1, 1],
                    "count": [3, 3, 1]
                }
            }
        }
        self.utils.execute_es_tests(test)

    def test_bad_edge_name(self):
        test = {
            "data": [
                {"k": "a", "v": 1},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "v",
                "edges": [""]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"k": "a", "count": 3, "v": 2},
                    {"k": "b", "count": 3, "v": 1},
                    {"count": 1, "v": 1}
                ]
            },
        }

        self.assertRaises("expression is empty", self.utils.execute_es_tests, test)

    def test_range(self):
        test = {
            "data": [
                {"k": "a", "s": 0, "e": 0.1},  # THIS RECORD HAS NO LIFESPAN, SO WE DO NOT COUNT IT
                {"k": "b", "s": 1, "e": 4},
                {"k": "c", "s": 2, "e": 5},
                {"k": "d", "s": 3, "e": 6},
                {"k": "e", "s": 4, "e": 7},
                {"k": "f", "s": 5, "e": 8},
                {"k": "g", "s": 6, "e": 9},
                {"k": "h", "s": 7, "e": 10},
                {"k": "i", "s": 8, "e": 11},
            ],
            "query": {
                "from": TEST_TABLE,
                "edges": [
                    {
                        "name": "a",
                        "range": {"min": "s", "max": "e"},
                        "domain": {"type": "range", "min": 0, "max": 10, "interval": 1}
                    }
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "count": 1},
                    {"a": 1, "count": 1},
                    {"a": 2, "count": 2},
                    {"a": 3, "count": 3},
                    {"a": 4, "count": 3},
                    {"a": 5, "count": 3},
                    {"a": 6, "count": 3},
                    {"a": 7, "count": 3},
                    {"a": 8, "count": 3},
                    {"a": 9, "count": 2}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header":["a", "count"],
                "data": [
                    [0, 1],
                    [1, 1],
                    [2, 2],
                    [3, 3],
                    [4, 3],
                    [5, 3],
                    [6, 3],
                    [7, 3],
                    [8, 3],
                    [9, 2]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "a",
                     "domain": {"partitions": [
                         {"min": 0, "max": 1},
                         {"min": 1, "max": 2},
                         {"min": 2, "max": 3},
                         {"min": 3, "max": 4},
                         {"min": 4, "max": 5},
                         {"min": 5, "max": 6},
                         {"min": 6, "max": 7},
                         {"min": 7, "max": 8},
                         {"min": 8, "max": 9},
                         {"min": 9, "max": 10}
                     ]}}
                ],
                "data": {"count": [1, 1, 2, 3, 3, 3, 3, 3, 3, 2]}  # NOT SURE HOW WE ARE COUNTING NULLS
            }
        }
        self.utils.execute_es_tests(test)

    def test_edge_w_partition_filters(self):
        test = {
            "data": structured_test_data,
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "count", "value": "v", "aggregate": "count"},
                    {"name": "sum", "value": "v", "aggregate": "sum"}
                ],
                "edges": [
                    {
                        "name": "a",
                        "domain": {
                            "type": "set",
                            "partitions": [
                                {"name": "b", "where": {"eq": {"b.r": "b"}}},
                                {"name": "3", "where": {"eq": {"b.d": 3}}}
                            ]
                        }
                    }
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b", "count": 3, "sum": 15},
                    {"a": "3", "count": 4, "sum": 37},
                    {"a": NULL, "count": 6, "sum": 39}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header":["a", "count", "sum"],
                "data": [
                    ["b", 3, 15],
                    ["3", 4, 37],
                    [NULL, 6, 39],
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [{
                    "name": "a",
                    "domain": {"partitions": [
                        {"name": "b"},
                        {"name": "3"}
                    ]}
                }],
                "data": {
                    "count": [3, 4, 6],
                    "sum": [15, 37, 39]
                }
            }
        }
        self.utils.execute_es_tests(test)

# TODO: ALLOW USE OF EDGE VARIABLES IN QUERY
# IN THIS CASE "timestamp.min" REFERS TO A PART OF THE EDGE
# {
#     "from": "jobs",
#     "select": {
#           "name": "waiting",
#           "value": {"sub": ["timestamp.min", "action.request_time"]},
#           "aggregate": "average",
#           "default": 0
#     },
#     "edges": [
#         {
#             "name": "timestamp",
#             "range": {"min": "action.request_time", "max": "action.start_time"},
#             "domain": {
#                 "type": "time",
#                 "min": date.min.unix(),
#                 "max": date.max.unix(),
#                 "interval": date.interval.seconds()
#             }
#         }
#     ]
# }

# TODO: PARENT EDGE WITH DEEP FILTER
# {
#     "from": "task.task.artifacts",
#     "where": {
#         "regex": {
#             "name": ".*jscov.*"
#         }
#     },
#     "edges": [
#         "build.revision12"
#     ]
# }



simple_test_data = [
    {"a": "c", "v": 13},
    {"a": "b", "v": 2},
    {"v": 3},
    {"a": "b"},
    {"a": "c", "v": 7},
    {"a": "c", "v": 11}
]

long_test_data = [
    {"k": "a", "v": 1},
    {"k": "b", "v": 2},
    {"k": "c", "v": 3},
    {"k": "d", "v": 4},
    {"k": "e", "v": 5},
    {"k": "f", "v": 6},
    {"k": "g", "v": 7},
    {"k": "h", "v": 8},
    {"k": "i", "v": 9},
    {"k": "j", "v": 10},
    {"k": "k", "v": 11},
    {"k": "l", "v": 12},
    {"k": "m", "v": 13}
]


structured_test_data = [
    {"b": {"r": "a", "d": 1}, "v": 1},
    {"b": {"r": "a", "d": 2}, "v": 2},
    {"b": {"r": "a", "d": 3}, "v": 3},
    {"b": {"r": "b", "d": 1}, "v": 4},
    {"b": {"r": "b", "d": 2}, "v": 5},
    {"b": {"r": "b", "d": 3}, "v": 6},
    {"b": {"r": "c", "d": 1}, "v": 7},
    {"b": {"r": "c", "d": 2}, "v": 8},
    {"b": {"r": "c", "d": 3}, "v": 9},
    {"b": {"r": "d", "d": 1}, "v": 10},
    {"b": {"r": "d", "d": 2}, "v": 11},
    {"b": {"r": "d", "d": 3}, "v": 12},
    {"b": {"r": NULL, "d": 3}, "v": 13}
]


