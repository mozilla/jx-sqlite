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

from unittest import skip

from jx_base.expressions import NULL
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestSchemaMerging(BaseTestCase):
    """
    TESTS THAT DEMONSTRATE DIFFERENT SCHEMAS
    """

    def test_select(self):
        test = {
            "data": [
                {"a": "b"},
                {"a": [{"b": 1}, {"b": 2}]},
                {"a": 3}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    "b",
                    [{"b": 1}, {"b": 2}],
                    3
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [
                        ["b"],
                        [[{"b": 1}, {"b": 2}]],
                        [3]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 3, "interval": 1}
                    }
                ],
                "data": {
                    "a": [
                        "b",
                        [{"b": 1}, {"b": 2}],
                        3
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_mixed_primitives(self):
        test = {
            "data": [
                # _id USED TO CONTROL INSERT
                {"_id": "1", "a": "b"},
                {"_id": "2", "a": 3},
                {"_id": "3", "a": "c"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    "b",
                    3,
                    "c"
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [
                    ["b"],
                    [3],
                    ["c"]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 3, "interval": 1}
                    }
                ],
                "data": {
                    "a": ["b", 3, "c"]
                }
            }
        }
        self.utils.execute_tests(test, typed=True)

    def test_dots_in_property_names(self):
        test = {
            "data": [
                {"a.html": "hello"},
                {"a": {"html": "world"}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a\\.html"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    "hello",
                    NULL
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a\\.html"],
                "data": [
                    ["hello"],
                    [NULL]
                ]
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
                    "a\\.html": ["hello", NULL]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_dots_in_property_names2(self):
        test = {
            "data": [
                {"a.html": "hello"},
                {"a": {"html": "world"}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a.html"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    NULL,
                    "world"
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.html"],
                "data": [
                    [NULL],
                    ["world"]
                ]
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
                    "a.html": [NULL, "world"]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_dots_in_property_names3(self):
        test = {
            "data": [
                {"a.html": "hello"},
                {"a": {"html": "world"}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["a\\.html", "a.html"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a.html": "hello"},
                    {"a": {"html": "world"}}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a\\.html", "a.html"],
                "data": [
                    ["hello", NULL],
                    [NULL, "world"]
                ]
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
                    "a\\.html": ["hello", NULL],
                    "a.html": [NULL, "world"]
                }
            }
        }
        self.utils.execute_tests(test)

    @skip("schema merging not working")
    def test_count(self):
        test = {
            "data": [
                {"a": "b"},
                {"a": {"b": 1}},
                {"a": {}},
                {"a": [{"b": 1}, {"b": 2}]},  # TEST THAT INNER CAN BE MAPPED TO NESTED
                {"a": {"b": 4}},  # TEST THAT INNER IS MAPPED TO NESTED, AFTER SEEING NESTED
                {"a": 3},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "a", "aggregate": "count"}
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 5
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [[5]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "data": {
                    "a": 5
                }
            }
        }
        self.utils.execute_tests(test)

    @skip("schema merging not working")
    def test_sum(self):
        test = {
            "data": [
                {"a": "b"},
                {"a": {"b": 1}},
                {"a": {}},
                {"a": [{"b": 1}, {"b": 2}]},  # TEST THAT INNER CAN BE MAPPED TO NESTED
                {"a": {"b": 4}},  # TEST THAT INNER IS MAPPED TO NESTED, AFTER SEEING NESTED
                {"a": 3},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "a.b", "aggregate": "sum"}
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 8
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b"],
                "data": [[8]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "data": {
                    "a.b": 8
                }
            }
        }
        self.utils.execute_tests(test)

    @skip("For Orange query")
    def test_edge(self):
        test = {
            "data": [
                {"v": 1, "a": "b"},
                {"v": 2, "a": {"b": 1}},
                {"v": 3, "a": {}},
                {"v": 4, "a": [{"b": 1}, {"b": 2}, {"b": 2}]},  # TEST THAT INNER CAN BE MAPPED TO NESTED
                {"v": 5, "a": {"b": 4}},  # TEST THAT INNER IS MAPPED TO NESTED, AFTER SEEING NESTED
                {"v": 6, "a": 3},
                {"v": 7}
            ],
            "query": {
                "from": TEST_TABLE + ".a",
                "edges": [{"value": "b"}],
                "select": {"value": "v", "aggregate": "sum"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"b": 1, "v": 6},
                    {"b": 2, "v": 8},
                    {"b": 4, "v": 5},
                    {"v": 14}
                ]
            },
            # "expecting_table": {
            #     "meta": {"format": "table"},
            #     "header": ["a.b"],
            #     "data": [[8]]
            # },
            # "expecting_cube": {
            #     "meta": {"format": "cube"},
            #     "data": {
            #         "a.b": 8
            #     }
            # }
        }
        self.utils.execute_tests(test)

