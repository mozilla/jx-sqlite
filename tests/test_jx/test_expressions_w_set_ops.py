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
from mo_dots import wrap
from tests.test_jx import BaseTestCase, TEST_TABLE

lots_of_data = wrap([{"a": i} for i in range(30)])


class TestSetOps(BaseTestCase):

    def test_length(self):
        test = {
            "data": [
                {"v": "1"},
                {"v": "22"},
                {"v": "333"},
                {"v": "4444"},
                {"v": "55555"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "l", "value": {"length": "v"}},
                "sort": "v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [1, 2, 3, 4, 5]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["l"],
                "data": [
                    [1],
                    [2],
                    [3],
                    [4],
                    [5]
                ]
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
                    "l": [1, 2, 3, 4, 5]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_length_w_inequality(self):
        test = {
            "data": [
                {"v": "1"},
                {"v": "22"},
                {"v": "333"},
                {"v": "4444"},
                {"v": "55555"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "v",
                "where": {
                    "gt": [
                        {
                            "length": "v"
                        },
                        2
                    ]
                },
                "sort": "v"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": ["333", "4444", "55555"]
            }
        }
        self.utils.execute_tests(test)

    def test_left(self):
        test = {
            "data": [
                {},
                {"v": "1"},
                {"v": "22"},
                {"v": "333"},
                {"v": "4444"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"name": "v", "value": {"left": {"v": 2}}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [NULL, "1", "22", "33", "44"]
            }
        }
        self.utils.execute_tests(test)

    def test_eq(self):
        test = {
            "data": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ".",
                "where": {"eq": ["a", "b"]}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0},
                    {"a": 1, "b": 1},
                    {}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_ne(self):
        test = {
            "data": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ".",
                "where": {"ne": ["a", "b"]}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 1},
                    {"a": 0},
                    {"a": 1, "b": 0},
                    {"a": 1},
                    {"b": 0},
                    {"b": 1},
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_concat(self):
        test = {
            "data": [
                {"v": "hello", "w": None},
                {"v": "hello", "w": ""},
                {"v": "hello", "w": "world"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "a", "value": {"concat": []}},
                    {"name": "b", "value": {"concat": {"v": "test"}}},
                    {"name": "c", "value": {"concat": ["v", "w"]}},
                    {"name": "d", "value": {"concat": ["w", "v"]}},
                    {"name": "e", "value": {"concat": [], "separator": "-"}},
                    {"name": "f", "value": {"concat": {"v": 0}, "separator": "-"}},
                    {"name": "g", "value": {"concat": ["v", "w"], "separator": "-"}},
                    {"name": "h", "value": {"concat": ["w", "v"], "separator": "-"}},
                    {"name": "i", "value": {"concat": [{"literal": ""}, "v"], "separator": "-"}}
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {
                        "a": NULL,
                        "b": "hellotest",
                        "c": "helloworld",
                        "d": "worldhello",
                        "e": NULL,
                        "f": "hello-0",
                        "g": "hello-world",
                        "h": "world-hello",
                        "i": "hello"
                    },
                    {
                        "a": NULL,
                        "b": "hellotest",
                        "c": "hello",
                        "d": "hello",
                        "e": NULL,
                        "f": "hello-0",
                        "g": "hello",
                        "h": "hello",
                        "i": "hello"
                    },
                    {
                        "a": NULL,
                        "b": "hellotest",
                        "c": "hello",
                        "d": "hello",
                        "e": NULL,
                        "f": "hello-0",
                        "g": "hello",
                        "h": "hello",
                        "i": "hello"
                    }
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_when(self):
        test = {
            "data": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    "a",
                    "b",
                    {"name": "io", "value": {"when": {"eq": ["a", "b"]}, "then": 1, "else": 2}}
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0, "io": 1},
                    {"a": 0, "b": 1, "io": 2},
                    {"a": 0, "io": 2},
                    {"a": 1, "b": 0, "io": 2},
                    {"a": 1, "b": 1, "io": 1},
                    {"a": 1, "io": 2},
                    {"b": 0, "io": 2},
                    {"b": 1, "io": 2},
                    {"io": 1}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_when_on_multivalue(self):
        test = {
            "data": [
                {"a": "e"},
                {"a": "c"},
                {"a": ["e"]},
                {"a": ["c"]},
                {"a": ["e", "c"]},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    "a",
                    {"name": "is_e", "value": {"when": {"eq": {"a": "e"}}, "then": 1, "else": 0}},
                    {"name": "not_e", "value": {"when": {"not": {"eq": {"a": "e"}}}, "then": 1, "else": 0}},
                    {"name": "is_c", "value": {"when": {"eq": {"a": "c"}}, "then": 1, "else": 0}}
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "e", "is_e": 1, "not_e": 0, "is_c": 0},
                    {"a": "c", "is_e": 0, "not_e": 1, "is_c": 1},
                    {"a": "e", "is_e": 1, "not_e": 0, "is_c": 0},
                    {"a": "c", "is_e": 0, "not_e": 1, "is_c": 1},
                    {"a": ["e", "c"], "is_e": 1, "not_e": 0, "is_c": 1},
                    {"a": NULL, "is_e": 0, "not_e": 1, "is_c": 0}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_in_w_multivalue(self):
        test = {
            "data": [
                {"a": "e"},
                {"a": "c"},
                {"a": ["e"]},
                {"a": ["c"]},
                {"a": ["e", "c"]},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    "a",
                    {"name": "is_e", "value": {"in": [{"literal": "e"}, "a"]}},
                    {"name": "not_e", "value": {"not": {"in": [{"literal": "e"}, "a"]}}},
                    {"name": "is_c", "value": {"in": [{"literal": "c"}, "a"]}},
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "e", "is_e": True, "not_e": False, "is_c": False},
                    {"a": "c", "is_e": False, "not_e": True, "is_c": True},
                    {"a": "e", "is_e": True, "not_e": False, "is_c": False},
                    {"a": "c", "is_e": False, "not_e": True, "is_c": True},
                    {"a": ["e", "c"], "is_e": True, "not_e": False, "is_c": True},
                    {"a": NULL, "is_e": False, "not_e": True, "is_c": False}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_agg_mult_w_when(self):
        test = {
            "data": [
                {"a": 0, "b": False},                  # 0*1
                {"a": 1, "b": False},                  # 1*1 = 1
                {"a": 2, "b": True},                   # 2*0
                {"a": 3, "b": False},                  # 3*1 = 3
                {"a": 4, "b": True},                   # 4*0
                {"a": 5, "b": False},                  # 5*1 = 5
                {"a": 6, "b": True},                   # 6*0
                {"a": 7, "b": True},                   # 7*0
                {"a": 8},  # COUNTED, "b" IS NOT true  # 8*1 = 8
                {"b": True},  # NOT COUNTED              null * 0 = null
                {"b": False},  # COUNTED                 null * 1 = null
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {
                    "name": "ab",
                    "value": {
                        "mult": [
                            "a",
                            {
                                "when": "b",
                                "then": 0,
                                "else": 1
                            }
                        ]
                    },
                    "aggregate": "sum"
                }
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 17
            }
        }
        self.utils.execute_tests(test)

    # @skip("boolean in when is not using false")
    def test_select_mult_w_when(self):
        test = {
            "data": [
                {"a": 0, "b": False},                  # 0*1
                {"a": 1, "b": False},                  # 1*1 = 1
                {"a": 2, "b": True},                   # 2*0
                {"a": 3, "b": False},                  # 3*1 = 3
                {"a": 4, "b": True},                   # 4*0
                {"a": 5, "b": False},                  # 5*1 = 5
                {"a": 6, "b": True},                   # 6*0
                {"a": 7, "b": True},                   # 7*0
                {"a": 8},  # COUNTED, "b" IS NOT true  # 8*1 = 8
                {"b": True},  # NOT COUNTED              null * 0 = null
                {"b": False}   # NOT COUNTED             null * 1 = null
            ],
            "query": {
                "from": TEST_TABLE,
                "select": [
                    {"name": "b", "value": {"when": "b", "then": 0, "else": 1}},
                    {
                        "name": "ab",
                        "value": {
                            "mult": [
                                "a",
                                {"when": "b", "then": 0, "else": 1}
                            ]
                        }
                    }
                ],
                "limit": 100
            },
            "expecting_list": {
                "data": [
                    {"ab": 0, "b": 0},
                    {"ab": 0, "b": 0},
                    {"ab": 0, "b": 0},
                    {"ab": 0, "b": 0},
                    {"ab": 0, "b": 1},
                    {"ab": 1, "b": 1},
                    {"ab": 3, "b": 1},
                    {"ab": 5, "b": 1},
                    {"ab": 8, "b": 1},
                    {"ab": NULL, "b": 1},
                    {"ab": NULL, "b": 0}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_add(self):
        test = {
            "data": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["a", "b", {"name": "t", "value": {"add": ["a", "b"], "nulls":True}}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0, "t": 0},
                    {"a": 0, "b": 1, "t": 1},
                    {"a": 0, "t": 0},
                    {"a": 1, "b": 0, "t": 1},
                    {"a": 1, "b": 1, "t": 2},
                    {"a": 1, "t": 1},
                    {"b": 0, "t": 0},
                    {"b": 1, "t": 1},
                    {"t": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_add_w_default(self):
        test = {
            "data": [
                {"a": 1, "b": -1},  # DUMMY VALUE TO CREATE COLUMNS
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["a", "b", {"name": "t", "value": {"add": ["a", "b"], "default": -5}}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 1, "b": -1, "t": 0},
                    {"t": -5}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_count(self):
        test = {
            "data": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": ["a", "b", {"name": "t", "value": {"count": ["a", "b"]}}]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0, "t": 2},
                    {"a": 0, "b": 1, "t": 2},
                    {"a": 0, "t": 1},
                    {"a": 1, "b": 0, "t": 2},
                    {"a": 1, "b": 1, "t": 2},
                    {"a": 1, "t": 1},
                    {"b": 0, "t": 1},
                    {"b": 1, "t": 1},
                    {"t": 0}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_average(self):
        test = {
            "data": [{"a": {"_b": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ]}}],
            "query": {
                "from": TEST_TABLE+".a._b",
                "select": [
                    {"aggregate": "count"},
                    {"name": "t", "value": {"add": ["a", "b"], "nulls":True}, "aggregate": "average"}
                ],
                "edges": ["a", "b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0, "count": 1, "t": 0},
                    {"a": 0, "b": 1, "count": 1, "t": 1},
                    {"a": 0, "count": 1, "t": 0},
                    {"a": 1, "b": 0, "count": 1, "t": 1},
                    {"a": 1, "b": 1, "count": 1, "t": 2},
                    {"a": 1, "count": 1, "t": 1},
                    {"b": 0, "count": 1, "t": 0},
                    {"b": 1, "count": 1, "t": 1},
                    {"t": NULL, "count": 1}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_average_on_none(self):
        test = {
            "data": [{"a": {"_b": [
                {"a": 5},
                {}
            ]}}],
            "query": {
                "from": TEST_TABLE+".a._b",
                "select": [
                    {"name": "t", "value": {"add": ["a", "a"]}, "aggregate": "average"}
                ],
                "edges": ["a"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 5, "t": 10},
                    {"t": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_select_gt_on_sub(self):
        test = {
            "data": [{"a": {"_b": [
                {"a": 0, "b": 0},
                {"a": 0, "b": 1},
                {"a": 0},
                {"a": 1, "b": 0},
                {"a": 1, "b": 1},
                {"a": 1},
                {"b": 0},
                {"b": 1},
                {}
            ]}}],
            "query": {
                "from": TEST_TABLE+".a._b",
                "select": [
                    "a",
                    "b",
                    {"name": "diff", "value": {"sub": ["a", "b"]}}
                ],
                "where": {"gt": [{"sub": ["a", "b"]}, 0]},
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 1, "b": 0, "diff": 1}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_find(self):
        test = {
            "data": [
                {"v": "test"},
                {"v": "not test"},
                {"v": NULL},
                {},
                {"v": "a"}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"find": {"v": "test"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": "test"},
                    {"v": "not test"}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_or_find(self):
        test = {
            "data": [
                {"v": "test"},
                {"v": "not test"},
                {"v": NULL},
                {},
                {"v": "a"}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"or": [{"find": {"v": "test"}}]}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": "test"},
                    {"v": "not test"}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_and_find(self):
        test = {
            "data": [
                {"v": "test"},
                {"v": "not test"},
                {"v": NULL},
                {},
                {"v": "a"}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"and": [{"find": {"v": "test"}}]}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": "test"},
                    {"v": "not test"}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_left_in_edge(self):
        test = {
            "data": [
                {"v": "test"},
                {"v": "not test"},
                {"v": NULL},
                {},
                {"v": "a"}
            ],
            "query": {
                "edges": [{"name": "a", "value": {"left": {"v": 1}}}],
                "from": TEST_TABLE
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [{"name": "a", "domain": {"type": "set", "partitions": [
                    {"value": "a"},
                    {"value": "n"},
                    {"value": "t"},
                ]}}],
                "data": {
                    "count": [1, 1, 1, 2]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_left_and_right(self):
        test = {
            "data": [
                {"i": 0, "t": -1, "v": NULL},
                {"i": 1, "t": -1, "v": ""},
                {"i": 2, "t": -1, "v": "a"},
                {"i": 3, "t": -1, "v": "abcdefg"},
                {"i": 4, "t": 0, "v": NULL},
                {"i": 5, "t": 0, "v": ""},
                {"i": 6, "t": 0, "v": "a"},
                {"i": 7, "t": 0, "v": "abcdefg"},
                {"i": 8, "t": 3, "v": NULL},
                {"i": 9, "t": 3, "v": ""},
                {"i": 10, "t": 3, "v": "a"},
                {"i": 11, "t": 3, "v": "abcdefg"},
                {"i": 12, "t": 7, "v": NULL},
                {"i": 13, "t": 7, "v": ""},
                {"i": 14, "t": 7, "v": "a"},
                {"i": 15, "t": 7, "v": "abcdefg"}
            ],
            "query": {
                "select": [
                    "i",
                    {"name": "a", "value": {"left": ["v", "t"]}},
                    {"name": "b", "value": {"not_left": ["v", "t"]}},
                    {"name": "c", "value": {"right": ["v", "t"]}},
                    {"name": "d", "value": {"not_right": ["v", "t"]}}
                ],
                "from": TEST_TABLE,
                "limit": 100
            },
            "expecting_list": {
                "data": [
                    {"i": 0, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 1, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 2, "a": NULL, "b": "a", "c": NULL, "d": "a"},
                    {"i": 3, "a": NULL, "b": "abcdefg", "c": NULL, "d": "abcdefg"},
                    {"i": 4, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 5, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 6, "a": NULL, "b": "a", "c": NULL, "d": "a"},
                    {"i": 7, "a": NULL, "b": "abcdefg", "c": NULL, "d": "abcdefg"},
                    {"i": 8, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 9, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 10, "a": "a", "b": NULL, "c": "a", "d": NULL},
                    {"i": 11, "a": "abc", "b": "defg", "c": "efg", "d": "abcd"},
                    {"i": 12, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 13, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 14, "a": "a", "b": NULL, "c": "a", "d": NULL},
                    {"i": 15, "a": "abcdefg", "b": NULL, "c": "abcdefg", "d": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_string(self):
        test = {
            "data": [
                {"v": 1},
                {"v": "2"},
                {"v": 3},
                {"v": "4"},
                {"v": "100"},
                {}
            ],
            "query": {
                "select": {"name": "v", "value": {"string": "v"}},
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": ["1", "2", "3", "4", "100", NULL]
            }
        }
        self.utils.execute_tests(test)

    def test_number(self):
        test = {
            "data": [
                {"v": 1},
                {"v": "2"},
                {"v": 3},
                {"v": "4"},
                {}
            ],
            "query": {
                "select": {"name": "v", "value": {"number": "v"}},
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [1, 2, 3, 4, NULL]
            }
        }
        self.utils.execute_tests(test)

    def test_div_with_default(self):
        test = {
            "data": [
                {"v": 0},
                {"v": 1},
                {"v": 2},
                {}
            ],
            "query": {
                "select": {"name": "v", "value": {"div": {"v": 2}, "default": 10}},
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [0, 0.5, 1, 10]
            }
        }
        self.utils.execute_tests(test)

    def test_div_wo_default(self):
        test = {
            "data": [
                {"v": 0},
                {"v": 1},
                {"v": 2},
                {}
            ],
            "query": {
                "select": {"name": "v", "value": {"div": {"v": 2}}},
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [0, 0.5, 1, NULL]
            }
        }
        self.utils.execute_tests(test)

    def test_between(self):
        test = {
            "data": [
                {"v": "/this/is/a/directory"},
                {"v": "/"}
            ],
            "query": {
                "select": [
                    {"name": "a", "value": {"between": {"v": ["/this/", "/"]}}},
                    {"name": "c", "value": {"between": ["v", {"literal": "/this/"}, {"literal": "/"}]}},
                    {"name": "d", "value": {"between": {"v": [-1, 5]}}},
                    {"name": "e", "value": {"between": {"v": [None, "/is"]}}},
                    {"name": "f", "value": {"between": {"v": ["/is", None]}}}
                ],
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "is", "c": "is", "d": "/this", "e": "/this", "f": "/a/directory"},
                    {"d": "/"}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_between_missing(self):
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
                "select": [
                    "url",
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
                "limit": 100
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    NULL,
                    {"url": "/", "filename": "/", "subdir": NULL},
                    {"url": "https://hg.mozilla.org/", "filename": "https://hg.mozilla.org/", "subdir": NULL},
                    {"url": "https://hg.mozilla.org/a/", "filename": NULL, "subdir": "a"},
                    {"url": "https://hg.mozilla.org/b/", "filename": NULL, "subdir": "b"},
                    {"url": "https://hg.mozilla.org/b/1", "filename": NULL, "subdir": "b"},
                    {"url": "https://hg.mozilla.org/b/2", "filename": NULL, "subdir": "b"},
                    {"url": "https://hg.mozilla.org/b/3", "filename": NULL, "subdir": "b"},
                    {"url": "https://hg.mozilla.org/c/", "filename": NULL, "subdir": "c"},
                    {"url": "https://hg.mozilla.org/d", "filename": "https://hg.mozilla.org/d", "subdir": NULL},
                    {"url": "https://hg.mozilla.org/e", "filename": "https://hg.mozilla.org/e", "subdir": NULL}
                ]}

        }
        self.utils.execute_tests(test)



    def test_lack_of_eval(self):
        test = {
            "data": [
                {"v": "/this/is/a/directory"},
                {"v": "/"}
            ],
            "query": {
                "select": [
                    {"name": "b", "value": {"case": [
                        {"when": {"missing": {"literal": "/this/"}}, "then": 0},
                        {"find": {"v": "/this/"}, "start": 0}
                    ]}}
                ],
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"b": 0},
                    {"b": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_right(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "select": [{"name": "v", "value": {"right": {"v": 4}}}],
                "from": TEST_TABLE
            },
            "expecting_list": {
                "meta": {
                    "format": "list"},
                "data": [
                    {"v": "test"},
                    {"v": "vest"},
                    {"v": "test"},
                    {"v": NULL},
                    {"v": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_param_left(self):
        test = {
            "data": [
                {},
                {"url": "/"},
                #        012345678901234567890123456789
                {"url": "https://hg.mozilla.org/"},
                {"url": "https://hg.mozilla.org/a"},
                {"url": "https://hg.mozilla.org/b"},
                {"url": "https://hg.mozilla.org/b/1"},
                {"url": "https://hg.mozilla.org/b/2"},
                {"url": "https://hg.mozilla.org/b/3"},
                {"url": "https://hg.mozilla.org/c"},
                {"url": "https://hg.mozilla.org/d"},
                {"url": "https://hg.mozilla.org/e"}
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": {
                    "name": "f",
                    "value": {
                        "left": [
                            "url",
                            {
                                "find": {"url": "/"},
                                "start": 23,
                                "default": {"length": "url"}
                            }

                        ]
                    }
                }
            },
            "expecting_list":{
                "meta": {"format": "list"},
                "data": [
                    {"f": NULL, "count": 1},
                    {"f": "/", "count": 1},
                    {"f": "https://hg.mozilla.org/", "count": 1},
                    {"f": "https://hg.mozilla.org/a", "count": 1},
                    {"f": "https://hg.mozilla.org/b", "count": 4},
                    {"f": "https://hg.mozilla.org/c", "count": 1},
                    {"f": "https://hg.mozilla.org/d", "count": 1},
                    {"f": "https://hg.mozilla.org/e", "count": 1}
                ]
            }
        }

        self.utils.execute_tests(test)

    def test_not_left(self):
        test = {
            "data": [
                {"url": NULL},
                {"url": "/"},
                #        012345678901234567890123456789
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
                "where": {"and": [
                    {"prefix": {"url": "https://hg.mozilla.org/"}},
                    {"not": {"find": [{"not_left": {"url": 23}}, {"literal": "/"}]}}
                ]}
            },
            "expecting_list":{
                "meta": {"format": "list"},
                "data": [
                    {"url": "https://hg.mozilla.org/"},
                    {"url": "https://hg.mozilla.org/d"},
                    {"url": "https://hg.mozilla.org/e"}
                ]
            }
        }

        self.utils.execute_tests(test)

    def test_date_on_duration(self):
        test = {
            "data": [
                {"data": 0},
                {"data": 1}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {
                    "name": "test",
                    "value": {"date": "day"}
                }

            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    86400,
                    86400
                ]
            }
        }

        self.utils.execute_tests(test)

    def test_prefix_w_when(self):
        test = {
            "data": [
                {"a": "test"},
                {"a": "testkyle"},
                {"a": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {
                    "name": "test",
                    "value": {"when": {"prefix": {"a": "test"}}, "then": 1}
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [1, 1, NULL]
            }
        }

        self.utils.execute_tests(test)

    def test_boolean_in_expression(self):
        test = {
            "data": [
                {"result": {"ok": True}},
                {"result": {"ok": True}},
                {"result": {"ok": True}},
                {"result": {"ok": True}},
                {"result": {"ok": False}},
                {"result": {"ok": False}},
                {"result": {"ok": False}},
                {"result": {"ok": False}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {
                    "name": "failures",
                    "aggregate": "sum",
                    "value": {
                        "when": {
                            "eq": {
                                "result.ok": "F"
                            }
                        },
                        "then": 1,
                        "else": 0
                    }
                }
            },
            "expecting_list":{
                "meta": {"format": "value"},
                "data": 4
            }
        }

        self.utils.execute_tests(test)

    def test_boolean_in_where_clause1(self):
        test = {
            "data": [
                {"result": {"ok": True}},
                {"result": {"ok": True}},
                {"result": {"ok": True}},
                {"result": {"ok": True}},
                {"result": {"ok": False}},
                {"result": {"ok": False}},
                {"result": {"ok": False}},
                {"result": {"ok": False}}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {
                    "in": {
                        "result.ok": [
                            "F"
                        ]
                    }
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"result": {"ok": False}},
                    {"result": {"ok": False}},
                    {"result": {"ok": False}},
                    {"result": {"ok": False}}
                ]
            }
        }

        self.utils.execute_tests(test)

    def test_boolean_in_where_clause2(self):
        test = {
            "data": [
                {"result": {"ok": True}},
                {"result": {"ok": True}},
                {"result": {"ok": True}},
                {"result": {}},
                {"result": {}},
                {"result": {"ok": False}},
                {"result": {"ok": False}},
                {"result": {"ok": False}}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"not": "result.ok"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"result": {}},
                    {"result": {}},
                    {"result": {"ok": False}},
                    {"result": {"ok": False}},
                    {"result": {"ok": False}}
                ]
            }
        }

        self.utils.execute_tests(test)

    def test_in_with_singlton(self):
        test = {
            "data": [
                {"a": "b"},
                {"a": "b"},
                {"a": "b"},
                {"a": "c"},
                {"a": "c"},
                {"a": "d"},
                {"a": "d"},
                {"a": "d"},
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"in": {"a": "b"}}
            },
            "expecting_list":{
                "meta": {"format": "list"},
                "data": [
                    {"a": "b"},
                    {"a": "b"},
                    {"a": "b"}
                ]
            }
        }

        self.utils.execute_tests(test)

    def test_floor_on_float(self):
        test = {
            "data": [
                {"a": -0.1},
                {"a": -0.0},
                {"a": 0.1},
                {"a": 10.9},
                {"a": 11.0},
                {"a": 11.1},
                {"a": 11.9},
                {"a": 0.1},
            ],
            "query": {
                "from": TEST_TABLE,
                "groupby": {"name": "a", "value": {"floor": {"a": 2}}}
            },
            "expecting_list":{
                "meta": {"format": "list"},
                "data": [
                    {"a": -2, "count": 1},
                    {"a": 0, "count": 3},
                    {"a": 10, "count": 4}
                ]
            }
        }

        self.utils.execute_tests(test)


# TODO: {"left": {variable: sentinel}}
# TODO: {"find": {variable: subtring}, "default": -1}


