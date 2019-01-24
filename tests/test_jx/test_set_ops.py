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

from unittest import skip, skipIf

from jx_base.expressions import NULL
from jx_base.query import DEFAULT_LIMIT, MAX_LIMIT
from mo_dots import wrap
import mo_math
from tests.test_jx import BaseTestCase, TEST_TABLE, global_settings

lots_of_data = wrap([{"a": i} for i in range(30)])


class TestSetOps(BaseTestCase):

    def test_star(self):
        test = {
           "data": [{"a": 1}],
           "query": {
               "select": "*",
               "from": TEST_TABLE
           },
           "expecting_list": {
               "meta": {"format": "list"}, "data": [{"a": 1}]
           }
       }
        self.utils.execute_tests(test)

    def test_simplest(self):
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": ["b"]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [["b"]]
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
                    "a": ["b"]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_on_missing_field(self):
        test = {
            "data": [
                {"a": {"b": {"c": 1}}},
                {"a": {"b": {"c": 2}}},
                {"a": {"b": {"c": 3}}},
                {"a": {"b": {"c": 4}}},
                {"a": {"b": {"c": 5}}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a.b.d"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                    {},
                    {},
                    {},
                    {},
                    {}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b.d"],
                "data": [[NULL], [NULL], [NULL], [NULL], [NULL]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 5, "interval": 1}
                    }
                ],
                "data": {
                    "a.b.d": [NULL, NULL, NULL, NULL, NULL]
                }
            }
        }
        self.utils.execute_tests(test)


    def test_select_on_shallow_missing_field(self):
        test = {
            "data": [
                {"a": {"b": {"c": 1}}},
                {"a": {"b": {"c": 2}}},
                {"a": {"b": {"c": 3}}},
                {"a": {"b": {"c": 4}}},
                {"a": {"b": {"c": 5}}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "d"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {},
                {},
                {},
                {},
                {}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["d"],
                "data": [[NULL], [NULL], [NULL], [NULL], [NULL]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 5, "interval": 1}
                    }
                ],
                "data": {
                    "d": [NULL, NULL, NULL, NULL, NULL]
                }
            }
        }
        self.utils.execute_tests(test)


    def test_single_deep_select(self):

        test = {
            "data": [
                {"a": {"b": {"c": 1}}},
                {"a": {"b": {"c": 2}}},
                {"a": {"b": {"c": 3}}},
                {"a": {"b": {"c": 4}}},
                {"a": {"b": {"c": 5}}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a.b.c",
                "sort": "a.b.c"  # SO THE CUBE COMPARISON WILL PASS
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [1, 2, 3, 4, 5]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b.c"],
                "data": [[1], [2], [3], [4], [5]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 5, "interval": 1}
                    }
                ],
                "data": {
                    "a.b.c": [1, 2, 3, 4, 5]
                }
            }
        }
        self.utils.execute_tests(test)


    def test_single_select_alpha(self):
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": ["b"]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [["b"]]
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
                    "a": ["b"]
                }
            }
        }
        self.utils.execute_tests(test)


    def test_single_rename(self):
        test = {
            "name": "rename singleton alpha",
            "data": [
                {"a": "b"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "value", "value": "a"}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": ["b"]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["value"],
                "data": [["b"]]
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
                    "value": ["b"]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_single_no_select(self):
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": "b"}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["."],
                "data": [[{"a": "b"}]]
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
                    ".": [{"a": "b"}]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_id_select(self):
        """
        ALWAYS GOOD TO HAVE AN ID, CALL IT "_id"
        """
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "select": "_id",
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"_id": mo_math.is_hex}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_id"],
                "data": [[mo_math.is_hex]]
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
                    "_id": [mo_math.is_hex]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_id_value_select(self):
        """
        ALWAYS GOOD TO HAVE AN ID, CALL IT "_id"
        """
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "select": "_id",
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    mo_math.is_hex
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_id_and_value_select(self):
        """
        ALWAYS GOOD TO HAVE AN ID, CALL IT "_id"
        """
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "select": ["_id", "a"],
                "from": TEST_TABLE
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header":["_id", "a"],
                "data": [
                    [mo_math.is_hex, "b"]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_single_star_select(self):
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "select": "*",
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a": "b"}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [["b"]]
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
                    "a": ["b"]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_dot_select(self):
        test = {
            "data": [
                {"a": "b"}
            ],
            "query": {
                "select": {"name": "value", "value": "."},
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [{"a": "b"}]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["value"],
                "data": [[{"a": "b"}]]
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
                    "value": [{"a": "b"}]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_list_of_values(self):
        test = {
            "data": ["a", "b"],
            "query": {
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    "a", "b"
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["."],
                "data": [["a"], ["b"]]
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
                    ".": ["a", "b"]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_all_from_list_of_objects(self):
        test = {
            "data": [
                {"a": "b"},
                {"a": "d"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "*"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "b"},
                    {"a": "d"}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [
                    ["b"],
                    ["d"]
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
                    "a": ["b", "d"]
                }
            }
        }
        self.utils.execute_tests(test)

    @skip("Too complicated")
    def test_select_into_children(self):
        test = {
            "name": "select into children to table",
            "metadata": {
                "properties": {
                    "x": {"type": "integer"},
                    "a": {
                        "type": "nested",
                        "properties": {
                            "y": {
                                "type": "keyword"
                            },
                            "b": {
                                "type": "nested",
                                "properties": {
                                    "c": {"type": "integer"},
                                    "1": {"type": "integer"}

                                }
                            },
                            "z": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            },
            "data": [
                {"x": 5},
                {
                    "a": [
                        {
                            "b": {"c": 13},
                            "y": "m"
                        },
                        {
                            "b": [
                                {"c": 17, "1": 27},
                                {"c": 19}

                            ],
                            "y": "q"
                        },
                        {
                            "y": "r"
                        }
                    ],
                    "x": 3
                },
                {
                    "a": {"b": {"c": 23}},
                    "x": 7
                },
                {
                    "a": {"b": [
                        {"c": 29, "1": 31},
                        {"c": 37, "1": 41},
                        {"1": 47},
                        {"c": 53, "1": 59}
                    ]},
                    "x": 11
                }
            ],
            "query": {
                "from": TEST_TABLE + ".a.b",
                "select": ["...x", "c"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"x": 5, "c": NULL},
                    {"x": 3, "c": 13},
                    {"x": 3, "c": 17},
                    {"x": 3, "c": 19},
                    {"x": 7, "c": 23},
                    {"x": 11, "c": 29},
                    {"x": 11, "c": 37},
                    {"x": 11, "c": NULL},
                    {"x": 11, "c": 53}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["x", "c"],
                "data": [
                    [5, NULL],
                    [3, 13],
                    [3, 17],
                    [3, 19],
                    [7, 23],
                    [11, 29],
                    [11, 37],
                    [11, NULL],
                    [11, 53]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "index",
                        "domain": {"type": "rownum", "min": 0, "max": 9, "interval": 1}
                    }
                ],
                "data": {
                    "x": [5, 3, 3, 3, 7, 11, 11, 11, 11],
                    "c": [NULL, 13, 17, 19, 23, 29, 37, NULL, 53]
                }
            }
        }
        self.utils.execute_tests(test, typed=False)

    @skipIf(global_settings.use == "sqlite", "no need for limit when using own resources")
    def test_max_limit(self):
        test = wrap({
            "data": lots_of_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "value", "value": "a"},
                "limit": 1000000000
            }
        })

        self.utils.fill_container(test)
        result = self.utils.execute_query(test.query)
        self.assertEqual(result.meta.es_query.size, MAX_LIMIT)

    def test_default_limit(self):
        test = wrap({
            "data": lots_of_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "value", "value": "a"},
            },
        })

        self.utils.fill_container(test)
        test.query.format = "list"
        result = self.utils.execute_query(test.query)
        self.assertEqual(len(result.data), DEFAULT_LIMIT)

        test.query.format = "table"
        result = self.utils.execute_query(test.query)
        self.assertEqual(len(result.data), DEFAULT_LIMIT)

        test.query.format = "cube"
        result = self.utils.execute_query(test.query)
        self.assertEqual(len(result.data.value), DEFAULT_LIMIT)

    def test_specific_limit(self):
        test = wrap({
            "data": lots_of_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "value", "value": "a"},
                "limit": 5
            },
        })

        self.utils.fill_container(test)
        test.query.format = "list"
        result = self.utils.execute_query(test.query)
        self.assertEqual(len(result.data), 5)

        test.query.format = "table"
        result = self.utils.execute_query(test.query)
        self.assertEqual(len(result.data), 5)

        test.query.format = "cube"
        result = self.utils.execute_query(test.query)
        self.assertEqual(len(result.data.value), 5)

    def test_negative_limit(self):
        test = wrap({
            "data": lots_of_data,
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "value", "value": "a"},
                "limit": -1
            },
        })

        self.utils.fill_container(test)
        test.query.format = "list"
        self.assertRaises(Exception, self.utils.execute_query, test.query)

    def test_select_w_star(self):
        test = {
            "data": [
                {"a": {"b": 0, "c": 0}, "d": 7},
                {"a": {"b": 0, "c": 1}},
                {"a": {"b": 1, "c": 0}},
                {"a": {"b": 1, "c": 1}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "sort": ["a.b", "a.c"]
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a.b": 0, "a.c": 0, "d": 7},
                {"a.b": 0, "a.c": 1},
                {"a.b": 1, "a.c": 0},
                {"a.b": 1, "a.c": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.c", "d"],
                "data": [
                    [0, 0, 7],
                    [0, 1, NULL],
                    [1, 0, NULL],
                    [1, 1, NULL]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a.b": [0, 0, 1, 1],
                    "a.c": [0, 1, 0, 1],
                    "d": [7, NULL, NULL, NULL]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_w_deep_star(self):
        test = {
            "data": [
                {"a": {"b": 0, "c": 0}},
                {"a": {"b": 0, "c": 1}},
                {"a": {"b": 1, "c": 0}},
                {"a": {"b": 1, "c": 1}},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a.*",
                "sort": ["a.b", "a.c"]
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                    {"a.b": 0, "a.c": 0},
                    {"a.b": 0, "a.c": 1},
                    {"a.b": 1, "a.c": 0},
                    {"a.b": 1, "a.c": 1}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.c"],
                "data": [
                    [0, 0],
                    [0, 1],
                    [1, 0],
                    [1, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a.b": [0, 0, 1, 1],
                    "a.c": [0, 1, 0, 1]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_expression(self):
        test = {
            "data": [
                {"a": {"b": 0, "c": 0}},
                {"a": {"b": 0, "c": 1}},
                {"a": {"b": 1, "c": 0}},
                {"a": {"b": 1, "c": 1}},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "sum", "value": {"add": ["a.b", "a.c"]}},
                    {"name": "sub", "value": {"sub": ["a.b", "a.c"]}}
                ],
                "sort": ["a.b", "a.c"]
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"sum": 0, "sub": 0},
                {"sum": 1, "sub": -1},
                {"sum": 1, "sub": 1},
                {"sum": 2, "sub": 0}
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["sum", "sub"],
                "data": [[0, 0], [1, -1], [1, 1], [2, 0]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "sum": [0, 1, 1, 2],
                    "sub": [0, -1, 1, 0]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_object(self):
        """
        ES DOES NOT ALLOW YOU TO SELECT AN OBJECT, ONLY THE LEAVES
        THIS SHOULD USE THE SCHEMA TO SELECT-ON-OBJECT TO MANY SELECT ON LEAVES
        """
        test = {
            "data": [
                {"o": 3, "a": {"b": "x", "v": 2}},
                {"o": 1, "a": {"b": "x", "v": 5}},
                {"o": 2, "a": {"b": "x", "v": 7}},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["a"],
                "sort": "a.v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": {"b": "x", "v": 2}},
                    {"a": {"b": "x", "v": 5}},
                    {"a": {"b": "x", "v": 7}},
                    {}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [
                    [{"b": "x", "v": 2}],
                    [{"b": "x", "v": 5}],
                    [{"b": "x", "v": 7}],
                    [{}]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a": [
                        {"b": "x", "v": 2},
                        {"b": "x", "v": 5},
                        {"b": "x", "v": 7},
                        {}
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_leaves(self):
        """
        ES DOES NOT ALLOW YOU TO SELECT AN OBJECT, ONLY THE LEAVES
        THIS SHOULD USE THE SCHEMA TO SELECT-ON-OBJECT TO MANY SELECT ON LEAVES
        """
        test = {
            "data": [
                {"o": 3, "a": {"b": "x", "v": 2}},
                {"o": 1, "a": {"b": "x", "v": 5}},
                {"o": 2, "a": {"b": "x", "v": 7}},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["a.*"],
                "sort": "a.v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a.b": "x", "a.v": 2},
                    {"a.b": "x", "a.v": 5},
                    {"a.b": "x", "a.v": 7},
                    {}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.v"],
                "data": [
                    ["x", 2],
                    ["x", 5],
                    ["x", 7],
                    [NULL, NULL]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a.b": ["x", "x", "x", NULL],
                    "a.v": [2, 5, 7, NULL]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_value_object(self):
        """
        ES DOES NOT ALLOW YOU TO SELECT AN OBJECT, ONLY THE LEAVES
        THIS SHOULD USE THE SCHEMA TO SELECT-ON-OBJECT TO MANY SELECT ON LEAVES
        """
        test = {
            "data": [
                {"o": 3, "a": {"b": "x", "v": 2}},
                {"o": 1, "a": {"b": "x", "v": 5}},
                {"o": 2, "a": {"b": "x", "v": 7}},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a",
                "sort": "a.v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"b": "x", "v": 2},
                    {"b": "x", "v": 5},
                    {"b": "x", "v": 7},
                    {}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a"],
                "data": [
                    [{"b": "x", "v": 2}],
                    [{"b": "x", "v": 5}],
                    [{"b": "x", "v": 7}],
                    [{}]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a": [
                        {"b": "x", "v": 2},
                        {"b": "x", "v": 5},
                        {"b": "x", "v": 7},
                        {}
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select2_object(self):
        """
        ES DOES NOT ALLOW YOU TO SELECT AN OBJECT, ONLY THE LEAVES
        THIS SHOULD USE THE SCHEMA TO SELECT-ON-OBJECT TO MANY SELECT ON LEAVES
        """
        test = {
            "data": [
                {"o": 3, "a": {"b": "x", "v": 2}},
                {"o": 1, "a": {"b": "x", "v": 5}},
                {"o": 2, "a": {"b": "x", "v": 7}},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["o", "a"],
                "sort": "a.v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 3, "a": {"b": "x", "v": 2}},
                    {"o": 1, "a": {"b": "x", "v": 5}},
                    {"o": 2, "a": {"b": "x", "v": 7}},
                    {"o": 4}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "a"],
                "data": [
                    [3, {"b": "x", "v": 2}],
                    [1, {"b": "x", "v": 5}],
                    [2, {"b": "x", "v": 7}],
                    [4, {}]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a": [
                        {"b": "x", "v": 2},
                        {"b": "x", "v": 5},
                        {"b": "x", "v": 7},
                        {}
                    ],
                    "o": [3, 1, 2, 4]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select3_object(self):
        """
        ES DOES NOT ALLOW YOU TO SELECT AN OBJECT, ONLY THE LEAVES
        THIS SHOULD USE THE SCHEMA TO SELECT-ON-OBJECT TO MANY SELECT ON LEAVES
        """
        test = {
            "data": [
                {"o": 3, "a": {"b": "x", "v": 2}},
                {"o": 1, "a": {"b": "x", "v": 5}},
                {"o": 2, "a": {"b": "x", "v": 7}},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["o", "a.*"],
                "sort": "a.v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 3, "a.b": "x", "a.v": 2},
                    {"o": 1, "a.b": "x", "a.v": 5},
                    {"o": 2, "a.b": "x", "a.v": 7},
                    {"o": 4}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "a.b", "a.v"],
                "data": [
                    [3, "x", 2],
                    [1, "x", 5],
                    [2, "x", 7],
                    [4, NULL, NULL]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
                    }
                ],
                "data": {
                    "a.b": ["x", "x", "x", NULL],
                    "a.v": [2, 5, 7, NULL],
                    "o": [3, 1, 2, 4]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_array_as_value(self):
        test = {
            "data": [
                {"_a": [{"b": 1, "c": 1}, {"b": 2, "c": 1}]},
                {"_a": [{"b": 1, "c": 2}, {"b": 2, "c": 2}]}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "_a"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    [{"b": 1, "c": 1}, {"b": 2, "c": 1}],
                    [{"b": 1, "c": 2}, {"b": 2, "c": 2}]
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a"],
                "data": [
                   [[{"b": 1, "c": 1}, {"b": 2, "c": 1}]],
                   [[{"b": 1, "c": 2}, {"b": 2, "c": 2}]]
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
                    "_a": [
                        [{"b": 1, "c": 1}, {"b": 2, "c": 1}],
                        [{"b": 1, "c": 2}, {"b": 2, "c": 2}]
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_w_nested_values(self):
        test = {
            "data": [
                {"_a": [{"k": [{"b": 1}, {"b": 2}]}]},
                {"_a": [{"k": [{"b": 1}, {"b": 2}]}]}
            ],
            "query": {
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_a": {"k": [{"b": 1}, {"b": 2}]}},
                    {"_a": {"k": [{"b": 1}, {"b": 2}]}}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["."],
                "data": [
                    [{"_a": {"k": [{"b": 1}, {"b": 2}]}}],
                    [{"_a": {"k": [{"b": 1}, {"b": 2}]}}]
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
                    ".": [
                        {"_a": {"k": [{"b": 1}, {"b": 2}]}},
                        {"_a": {"k": [{"b": 1}, {"b": 2}]}}
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_typed_column(self):
        test = {
            "data": [
                {"a": "test"}
            ],
            "query": {
                "select": ["a.~s~"],
                "from": TEST_TABLE,
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": {"~s~": "test"}}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.~s~"],
                "data": [
                    ["test"]
                ]
            }
        }
        self.utils.execute_tests(test)

        pass

        # TODO: The timestamp.~s~ APPEARS TO RESULT IN {"":{"":{"":{"":"2018-09-26 12:41:19.575174"}}}}
        # {
        #
        # 	"select":["machine.name","template","timestamp.~s~"],
        # 	"from":"debug-etl",
        # 	"where":{"exists":"timestamp.~s~"}
        # }

    def test_union_columns(self):
        test = {
            "data": [
                {"a": [1, 2, 3], "b": [2, 3, 4], "c":1},
                {"a": [4, 4, 4], "b": [2, 3, 4], "c":2}
            ],
            "query": {
                "select": [{"name": "x", "value": {"union": ["a", "b"]}}],
                "from": TEST_TABLE,
                "sort": "c"  # USE sort TO ENSURE ORDER OF ROWS TO MATCH expecting_list
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"x": {1, 2, 3, 4}},
                    {"x": {2, 3, 4}}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_filter_on_deep_property(self):
        test = {
            "data": [
                {"v": 1, "a": "b"},
                {"v": 4, "a": [{"b": 1}, {"b": 2}, {"b": 2}]},
                {"v": 2, "a": {"b": 1}},
                {"v": 3, "a": {}},
                {"v": 5, "a": {"b": 4}},
                {"v": 6, "a": 3},
                {"v": 7}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "v",
                "where": {"exists": "a.b"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [2, 4, 5]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["v"],
                "data": [[2], [4], [5]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "data": {
                    "v": [2, 4, 5]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_id_and_source(self):
        test = {
            "data": [
                {"_id": "test_id", "v": 4, "a": [{"b": 1}, {"b": 2}, {"b": 2}]},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["_id", {"name": "_source", "value": "."}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_id": "test_id", "_source": {"v": 4, "a": [{"b": 1}, {"b": 2}, {"b": 2}]}}
                ]
            }
        }
        self.utils.execute_tests(test)
