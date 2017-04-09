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

from tests.test_jx import BaseTestCase, TEST_TABLE, NULL

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
                    {"a": 1, "b": 0}
                ]
            }
        }
        self.utils.execute_es_tests(test)

    def test_concat(self):
        test = {
            "data": [
                {"v": "hello", "w": NULL},
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
        self.utils.execute_es_tests(test)

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
                "select": ["a", "b", {"name": "io", "value": {"when": {"eq": ["a", "b"]}, "then": 1, "else": 2}}]
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
        self.utils.execute_es_tests(test)

    def test_select_mult_w_when(self):
        test = {
            "data": [
                {"a": 0, "b": False},
                {"a": 1, "b": False},
                {"a": 2, "b": True},
                {"a": 3, "b": False},
                {"a": 4, "b": True},
                {"a": 5, "b": False},
                {"a": 6, "b": True},
                {"a": 7, "b": True},
                {"a": 8},  # COUNTED, "b" IS NOT true
                {"b": True},  # NOT COUNTED
                {"b": False},  # NOT COUNTED
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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
                    {"i": 0},
                    {"i": 1, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 2, "a": NULL, "b": "a", "c": NULL, "d": "a"},
                    {"i": 3, "a": NULL, "b": "abcdefg", "c": NULL, "d": "abcdefg"},
                    {"i": 4},
                    {"i": 5, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 6, "a": NULL, "b": "a", "c": NULL, "d": "a"},
                    {"i": 7, "a": NULL, "b": "abcdefg", "c": NULL, "d": "abcdefg"},
                    {"i": 8},
                    {"i": 9, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 10, "a": "a", "b": NULL, "c": "a", "d": NULL},
                    {"i": 11, "a": "abc", "b": "defg", "c": "efg", "d": "abcd"},
                    {"i": 12},
                    {"i": 13, "a": NULL, "b": NULL, "c": NULL, "d": NULL},
                    {"i": 14, "a": "a", "b": NULL, "c": "a", "d": NULL},
                    {"i": 15, "a": "abcdefg", "b": NULL, "c": "abcdefg", "d": NULL}
                ]
            }
        }
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
        self.utils.execute_es_tests(test)

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
                    {"name": "e", "value": {"between": {"v": [NULL, "/is"]}}},
                    {"name": "f", "value": {"between": {"v": ["/is", NULL]}}}
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
        self.utils.execute_es_tests(test)

    def test_param_left(self):
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

        self.utils.execute_es_tests(test)



# TODO: {"left": {variable: sentinel}}
# TODO: {"find": {variable: subtring}, "default": -1}


