# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from unittest import skipIf

from mo_dots import wrap
from mo_math import Math
from tests.test_jx import BaseTestCase, TEST_TABLE, global_settings, NULL

lots_of_data = wrap([{"a": i} for i in range(30)])


class TestDeepOps(BaseTestCase):
    def test_deep_select_column(self):
        test = {
            "data": [
                {"_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"_a": {"b": "x", "v": 5}},
                {"_a": [
                    {"b": "x", "v": 7},
                ]},
                {"c": "x"}
            ],
            "query": {
                "from": TEST_TABLE + "._a",
                "select": {"value": "_a.v", "aggregate": "sum"},
                "edges": ["_a.b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_a": {"b": "x", "v": 14}},
                    {"_a": {"b": "y", "v": 3}},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a.b", "_a.v"],
                "data": [
                    ["x", 14],
                    ["y", 3]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "_a.b",
                        "allowNulls": True,
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [{"value": "x"}, {"value": "y"}]
                        }
                    }
                ],
                "data": {
                    "_a.v": [14, 3]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_deep_select_column_w_groupby(self):
        test = {
            "data": [
                {"_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"_a": {"b": "x", "v": 5}},
                {"_a": [
                    {"b": "x", "v": 7},
                ]},
                {"c": "x"}
            ],
            "query": {
                "from": TEST_TABLE + "._a",
                "select": {"value": "_a.v", "aggregate": "sum"},
                "groupby": ["_a.b"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_a": {"b": "x", "v": 14}},
                    {"_a": {"b": "y", "v": 3}},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a.b", "_a.v"],
                "data": [
                    ["x", 14],
                    ["y", 3]
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_bad_deep_select_column_w_groupby(self):
        test = {
            "data": [  # WE NEED SOME DATA TO MAKE A NESTED COLUMN
                {"_a": {"b": "x"}}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": {"value": "a.v", "aggregate": "sum"},
                "groupby": ["a.b"]
            },
            "expecting_list": {  # DUMMY: SO AN QUERY ATTEMPT IS MADE
                "meta": {"format": "list"},
                "data": []
            }
        }
        self.assertRaises(Exception, self.utils.execute_tests, test)

    def test_abs_shallow_select(self):
        # TEST THAT ABSOLUTE COLUMN NAMES WORK (WHEN THEY DO NOT CONFLICT WITH RELATIVE PROPERTY NAME)
        test = {
            "data": [
                {"o": 3, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 1, "_a": {"b": "x", "v": 5}},
                {"o": 2, "_a": [
                    {"b": "x", "v": 7},
                ]},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": TEST_TABLE + "._a",
                "select": ["b", "o"],
                "where": {"eq": {"b": "x"}},
                "sort": ["o"]
            },
            "es_query": {  # FOR REFERENCE
                           "query": {"nested": {
                               "path": "_a",
                               "inner_hits": {"size": 100000},
                               "filter": {"term": {"_a.b": "x"}}
                           }},
                           "fields": ["o"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "b": "x"},
                    {"o": 2, "b": "x"},
                    {"o": 3, "b": "x"}
                ]},
            # "expecting_table": {
            # "meta": {"format": "table"},
            #     "header": ["o", "a", "c"],
            #     "data": [
            #         [1, {"b": "x", "v": 5}, NULL],
            #         [2, {"b": "x", "v": 7}, NULL],
            #         [3,
            #             [
            #                 {"b": "x", "v": 2},
            #                 {"b": "y", "v": 3}
            #             ],
            #             NULL
            #         ],
            #         [4, NULL, "x"]
            #     ]
            # },
            # "expecting_cube": {
            #     "meta": {"format": "cube"},
            #     "edges": [
            #         {
            #             "name": "rownum",
            #             "domain": {"type": "rownum", "min": 0, "max": 4, "interval": 1}
            #         }
            #     ],
            #     "data": {
            #         "a": [
            #             {"b": "x", "v": 5},
            #             {"b": "x", "v": 7},
            #             [
            #                 {"b": "x", "v": 2},
            #                 {"b": "y", "v": 3}
            #             ],
            #             NULL
            #         ],
            #         "c": [NULL, NULL, NULL, "x"],
            #         "o": [1, 2, 3, 4]
            #     }
            # }
        }

        self.utils.execute_tests(test)


    def test_select_whole_document(self):
        test = {
            "data": [
                {"o": 3, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 1, "_a": {"b": "x", "v": 5}},
                {"o": 2, "_a": [
                    {"b": "x", "v": 7},
                ]},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "sort": ["o"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "_a": {"b": "x", "v": 5}},
                    {"o": 2, "_a": {"b": "x", "v": 7}},
                    {"o": 3, "_a": [
                        {"b": "x", "v": 2},
                        {"b": "y", "v": 3}
                    ]},
                    {"o": 4, "c": "x"}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "_a", "c"],
                "data": [
                    [1, {"b": "x", "v": 5}, NULL],
                    [2, {"b": "x", "v": 7}, NULL],
                    [3,
                     [
                         {"b": "x", "v": 2},
                         {"b": "y", "v": 3}
                     ],
                     NULL
                    ],
                    [4, NULL, "x"]
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
                    "_a": [
                        {"b": "x", "v": 5},
                        {"b": "x", "v": 7},
                        [
                            {"b": "x", "v": 2},
                            {"b": "y", "v": 3}
                        ],
                        NULL
                    ],
                    "c": [NULL, NULL, NULL, "x"],
                    "o": [1, 2, 3, 4]
                }
            }
        }
        self.utils.execute_tests(test)


    def test_select_whole_nested_document(self):
        test = {
            "data": [
                {"o": 3, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 1, "_a": {"b": "x", "v": 5}},
                {"o": 2, "_a": [
                    {"b": "x", "v": 7},
                ]},
                {"o": 4, "c": "x"}
            ],
            "query": {
                "from": TEST_TABLE + "._a",
                "select": "*"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "b": "x", "v": 5},
                    {"o": 2, "b": "x", "v": 7},
                    {"o": 3, "b": "x", "v": 2},
                    {"o": 3, "b": "y", "v": 3},
                    {"o": 4, "c": "x"}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "b", "v", "c"],
                "data": [
                    [1, "x", 5, NULL],
                    [2, "x", 7, NULL],
                    [3, "x", 2, NULL],
                    [3, "y", 3, NULL],
                    [4, NULL, NULL, "x"]
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
                    "b": ["x", "x", "y", "x", NULL],
                    "v": [5, 7, 3, 2, NULL],
                    "c": [NULL, NULL, NULL, NULL, "x"],
                    "o": [1, 2, 3, 3, 4]
                }
            }
        }

        self.utils.execute_tests(test)

    def test_deep_names_w_star(self):
        test = {
            "data": [
                # LETTERS FROM action, timing, builder, harness, step
                {"a": {"_t": [
                    {"b": {"s": 1}, "h": {"s": "a-a"}},
                    {"b": {"s": 2}, "h": {"s": "a-b"}},
                    {"b": {"s": 3}, "h": {"s": "a-c"}},
                    {"b": {"s": 4}, "h": {"s": "b-d"}},
                    {"b": {"s": 5}, "h": {"s": "b-e"}},
                    {"b": {"s": 6}, "h": {"s": "b-f"}},
                   ]}}
            ],
            "query": {
                "select": "a._t.*",
                "from": TEST_TABLE + ".a._t",
                "where": {
                    "prefix": {
                        "h.s": "a-"
                    }
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a._t.b.s": 1, "a._t.h.s": "a-a"},
                    {"a._t.b.s": 2, "a._t.h.s": "a-b"},
                    {"a._t.b.s": 3, "a._t.h.s": "a-c"}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a._t.b.s", "a._t.h.s"],
                "data": [
                    [1, "a-a"],
                    [2, "a-b"],
                    [3, "a-c"]
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
                    "a._t.b.s": [1, 2, 3],
                    "a._t.h.s": ["a-a", "a-b", "a-c"]
                }
            }
        }

        self.utils.execute_tests(test)


    def test_deep_names_select_value(self):
        test = {
            "data": [
                # LETTERS FROM action, timing, builder, harness, step
                {"a": {"_t": [
                    {"b": {"s": 1}, "h": {"s": "a-a"}},
                    {"b": {"s": 2}, "h": {"s": "a-b"}},
                    {"b": {"s": 3}, "h": {"s": "a-c"}},
                    {"b": {"s": 4}, "h": {"s": "b-d"}},
                    {"b": {"s": 5}, "h": {"s": "b-e"}},
                    {"b": {"s": 6}, "h": {"s": "b-f"}},
                ]}}
            ],
            "query": {
                "select": "a._t",
                "from": TEST_TABLE + ".a._t",
                "where": {
                    "prefix": {
                        "h.s": "a-"
                    }
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"b": {"s": 1}, "h": {"s": "a-a"}},
                    {"b": {"s": 2}, "h": {"s": "a-b"}},
                    {"b": {"s": 3}, "h": {"s": "a-c"}}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a._t"],
                "data": [
                    [{"b": {"s": 1}, "h": {"s": "a-a"}}],
                    [{"b": {"s": 2}, "h": {"s": "a-b"}}],
                    [{"b": {"s": 3}, "h": {"s": "a-c"}}]
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
                    "a._t": [
                        {"b": {"s": 1}, "h": {"s": "a-a"}},
                        {"b": {"s": 2}, "h": {"s": "a-b"}},
                        {"b": {"s": 3}, "h": {"s": "a-c"}}
                    ],
                }
            }
        }

        self.utils.execute_tests(test)

    def test_deep_names(self):
        test = {
            "data": [
                # LETTERS FROM action, timing, builder, harness, step
                {"a": {"_t": [
                    {"b": {"s": 1}, "h": {"s": "a-a"}},
                    {"b": {"s": 2}, "h": {"s": "a-b"}},
                    {"b": {"s": 3}, "h": {"s": "a-c"}},
                    {"b": {"s": 4}, "h": {"s": "b-d"}},
                    {"b": {"s": 5}, "h": {"s": "b-e"}},
                    {"b": {"s": 6}, "h": {"s": "b-f"}},
                       ]}}
            ],
            "query": {
                "select": ["a._t"],
                "from": TEST_TABLE + ".a._t",
                "where": {
                    "prefix": {
                        "h.s": "a-"
                    }
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": {"_t": {"b": {"s": 1}, "h": {"s": "a-a"}}}},
                    {"a": {"_t": {"b": {"s": 2}, "h": {"s": "a-b"}}}},
                    {"a": {"_t": {"b": {"s": 3}, "h": {"s": "a-c"}}}}
                ]}
        }

        self.utils.execute_tests(test)

    def test_deep_agg_on_expression(self):
        # TEST WE CAN PERFORM AGGREGATES ON EXPRESSIONS OF DEEP VARIABLES
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string"},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {"v": "still more"}}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!"},
                ]}},
                {"o": 4, "a": {}}
            ],
            "query": {
                "from": TEST_TABLE + ".a._a",
                "select": {"name": "l", "value": {"length": "v"}, "aggregate": "max"}
            },
            "es_query": {
                # FOR REFERENCE
                "fields": [],
                "aggs": {"_nested": {
                    "nested": {"path": "a._a"},
                    "aggs": {"max_length": {"max": {"script": "(doc[\"v\"].value).length()"}}}
                }},
                "size": 10,
                "sort": []
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 14
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header":["l"],
                "data": [[14]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "select": {
                    "name": "l"
                },
                "data": {"l": 14}
            }
        }

        self.utils.execute_tests(test)

    def test_deep_agg_on_expression_w_shallow_where(self):
        # TEST WE CAN PERFORM AGGREGATES ON EXPRESSIONS OF DEEP VARIABLES
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string"},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {"v": "still more"}}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!"},
                ]}},
                {"o": 4, "a": {}}
            ],
            "query": {
                "from": TEST_TABLE + ".a._a",
                "select": {"name": "l", "value": {"length": "v"}, "aggregate": "max"},
                "where": {"lt": {"o": 3}}
            },
            "es_query": {  # FOR REFERENCE
                           "fields": [],
                           "aggs": {"_nested": {
                               "nested": {"path": "a._a"},
                               "aggs": {"max_length": {"max": {"script": "(doc[\"v\"].value).length()"}}}
                           }},
                           "size": 10,
                           "sort": []
            },
            "expecting_list": {
                "meta": {"format": "value"},
                "data": 10
            }
        }

        self.utils.execute_tests(test)

    def test_agg_w_complicated_where(self):
        # TEST WE CAN PERFORM AGGREGATES ON EXPRESSIONS OF DEEP VARIABLES
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string"},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {"v": "still more"}}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!"},
                ]}},
                {"o": 4, "a": {}}
            ],
            "query": {
                "from": TEST_TABLE + ".a._a",
                "select": "*",
                "where": {
                    "eq": [{"length": "v"}, 10]
                }
            },
            "es_query": {  # FOR REFERENCE
                "fields": [],
                "aggs": {"_nested": {
                    "nested": {"path": "a._a"},
                    "aggs": {"max_length": {"max": {"script": "(doc[\"v\"].value).length()"}}}
                }},
                "size": 10,
                "sort": []
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [{"o": 1, "v": "still more"}]
            }
        }

        self.utils.execute_tests(test)

    def test_setop_w_complicated_where(self):
        # TEST WE CAN PERFORM AGGREGATES ON EXPRESSIONS OF DEEP VARIABLES
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string", "s": False},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "still more",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!", "s": True},
                ]}},
                {"o": 4, "a": {"_a": {"s": False}}}
            ],
            "query": {
                "from": TEST_TABLE + ".a._a",
                "select": ["o", "v"],
                "where": {"and": [
                    {"gte": {"o": 2}},
                    {"eq": {"s": False}}
                ]}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 3, "v": "a string"},
                    {"o": 4}
                ]
            }
        }

        self.utils.execute_tests(test)

    def test_id_select(self):
        """
        ALWAYS GOOD TO HAVE AN ID, CALL IT "_id"
        """
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string", "s": False},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "still more",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!", "s": True},
                ]}},
                {"o": 4, "a": {"_a": {"s": False}}}
            ],
            "query": {
                "select": ["_id"],
                "from": TEST_TABLE + ".a._a",
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"_id": Math.is_hex},
                    {"_id": Math.is_hex},
                    {"_id": Math.is_hex},
                    {"_id": Math.is_hex},
                    {"_id": Math.is_hex}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_id"],
                "data": [
                    [Math.is_hex],
                    [Math.is_hex],
                    [Math.is_hex],
                    [Math.is_hex],
                    [Math.is_hex]
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
                    "_id": [Math.is_hex, Math.is_hex, Math.is_hex, Math.is_hex, Math.is_hex]
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
                {"o": 3, "a": {"_a": [
                    {"v": "a string", "s": False},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "still more",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!", "s": True},
                ]}},
                {"o": 4, "a": {"_a": {"s": False}}}
            ],
            "query": {
                "select": "_id",
                "from": TEST_TABLE + ".a._a",
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    Math.is_hex,  # DUE TO NATURE OF THE _id AUTO-ASSIGN LOGIC IN pyLibrary.env.elasticsearch.Index, WE KNOW _id WILL BE HEX
                    Math.is_hex,
                    Math.is_hex,
                    Math.is_hex,
                    Math.is_hex
                ]
            }
        }
        self.utils.execute_tests(test)


    def test_aggs_on_parent(self):
        test = {
            "data": [
                {"o": 3, "a": {"_a": [
                    {"v": "a string", "s": False},
                    {"v": "another string"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "still more",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "string!", "s": True},
                ]}},
                {"o": 4, "a": {"_a": {"s": False}}}
            ],
            "query": {
                "from": TEST_TABLE + ".a._a",
                "edges": ["o"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "count": 1},
                    {"o": 2, "count": 1},
                    {"o": 3, "count": 2},
                    {"o": 4, "count": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "count"],
                "data": [
                    [1, 1],
                    [2, 1],
                    [3, 2],
                    [4, 1]
                ]

            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "select": {"name": "count"},
                "edges": [
                    {"name": "o", "domain": {"type": "set", "partitions": [
                        {"value": 1, "dataIndex": 0},
                        {"value": 2, "dataIndex": 1},
                        {"value": 3, "dataIndex": 2},
                        {"value": 4, "dataIndex": 3}
                    ]}}
                ],
                "data": {
                    "count": [1, 1, 2, 1, 0]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_aggs_on_parent_and_child(self):
        test = {
            "data": [
                {"o": 1, "a": {"_a": [
                    {"v": "b", "s": False},
                    {"v": "c"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "b",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "b", "s": True},
                ]}},
                {"o": 2, "a": {"_a": {"s": False}}}
            ],
            "query": {
                "from": TEST_TABLE + ".a._a",
                "edges": ["o", "v"],
                "select": {"aggregate": "count", "value": "s"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "v": "b", "s": 2},
                    {"o": 1, "v": "c", "s": 0},
                    {"o": 1, "v": NULL, "s": 0},
                    {"o": 2, "v": "b", "s": 1},
                    {"o": 2, "v": "c", "s": 0},
                    {"o": 2, "v": NULL, "s": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "v", "s"],
                "data": [
                    [1, "b", 2],
                    [1, "c", 0],
                    [1, NULL, 0],
                    [2, "b", 1],
                    [2, "c", 0],
                    [2, NULL, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "o", "domain": {"type": "set", "partitions": [
                        {"value": 1, "dataIndex": 0},
                        {"value": 2, "dataIndex": 1},
                    ]}},
                    {"name": "v", "domain": {"type": "set", "partitions": [
                        {"value": "b", "dataIndex": 0},
                        {"value": "c", "dataIndex": 1},
                    ]}}
                ],
                "data": {
                    "s": [
                        [2, 0, 0],
                        [1, 0, 1],
                        [0, 0, 0]
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_aggs_on_parent_and_child2(self):
        # ADDED DOCUMENT WHERE o IS MISSING
        test = {
            "data": [
                {"o": 1, "a": {"_a": [
                    {"v": "b", "s": False},
                    {"v": "c"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "b",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "b", "s": True},
                ]}},
                {"o": 2, "a": {"_a": {"s": False}}},
                {"a": {"_a": {"s": False}}}
            ],
            "query": {
                "from": TEST_TABLE + ".a._a",
                "edges": ["o", "v"],
                "select": {"aggregate": "count", "value": "s"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "v": "b", "s": 2},
                    {"o": 1, "v": "c", "s": 0},
                    {"o": 1, "v": NULL, "s": 0},
                    {"o": 2, "v": "b", "s": 1},
                    {"o": 2, "v": "c", "s": 0},
                    {"o": 2, "v": NULL, "s": 1},
                    {"o": NULL, "v":"b", "s": 0},
                    {"o": NULL, "v":"c", "s": 0},
                    {"o": NULL, "v":NULL, "s": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["o", "v", "s"],
                "data": [
                    [1, "b", 2],
                    [1, "c", 0],
                    [1, NULL, 0],
                    [2, "b", 1],
                    [2, "c", 0],
                    [2, NULL, 1],
                    [NULL, "b", 0],
                    [NULL, "c", 0],
                    [NULL, NULL, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "o", "domain": {"type": "set", "partitions": [
                        {"value": 1, "dataIndex": 0},
                        {"value": 2, "dataIndex": 1},
                    ]}},
                    {"name": "v", "domain": {"type": "set", "partitions": [
                        {"value": "b", "dataIndex": 0},
                        {"value": "c", "dataIndex": 1},
                    ]}}
                ],
                "data": {
                    "s": [
                        [2, 0, 0],
                        [1, 0, 1],
                        [0, 0, 1]
                    ]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_aggs_on_parent_and_child3(self):
        # NO DOCUEMNT WHERE v IS MISSING
        test = {
            "data": [
                {"o": 1, "a": {"_a": [
                    {"v": "b", "s": False},
                    {"v": "c"}
                ]}},
                {"o": 1, "a": {"_a": {
                    "v": "b",
                    "s": False
                }}},
                {"o": 2, "a": {"_a": [
                    {"v": "b", "s": True},
                ]}}
            ],
            "query": {
                "from": TEST_TABLE + ".a._a",
                "edges": ["o", "v"],
                "select": {"aggregate": "count", "value": "s"}
            },
            "expecting_list": {  # TODO: THIS MAY NOT BE CORRECT
                "meta": {"format": "list"},
                "data": [
                    {"o": 1, "v": "b", "s": 2},
                    {"o": 1, "v": "c", "s": 0},
                    {"o": 1, "v": NULL, "s": 0},
                    {"o": 2, "v": "b", "s": 1},
                    {"o": 2, "v": "c", "s": 0},
                    {"o": 2, "v": NULL, "s": 0}
                ]
            },
            "expecting_table": {  # TODO: THIS MAY NOT BE CORRECT
                "meta": {"format": "table"},
                "header": ["o", "v", "s"],
                "data": [
                    [1, "b", 2],
                    [1, "c", 0],
                    [1, NULL, 0],
                    [2, "b", 1],
                    [2, "c", 0],
                    [2, NULL, 0]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "o", "domain": {"type": "set", "partitions": [
                        {"value": 1, "dataIndex": 0},
                        {"value": 2, "dataIndex": 1},
                    ]}},
                    {"name": "v", "domain": {"type": "set", "partitions": [
                        {"value": "b", "dataIndex": 0},
                        {"value": "c", "dataIndex": 1},
                    ]}}
                ],
                "data": {
                    "s": [
                        [2, 0, 0],
                        [1, 0, 0],
                        [0, 0, 0]
                    ]
                }
            }
        }
        self.utils.execute_tests(test)




    def test_deep_edge_using_list(self):
        data = [{"a": {"_b": [
            {"r": "a",  "s": "aa"},
            {           "s": "bb"},
            {"r": "bb", "s": "bb"},
            {"r": "c",  "s": "cc"},
            {           "s": "dd"},
            {"r": "e",  "s": "ee"},
            {"r": "e",  "s": "ee"},
            {"r": "f"},
            {"r": "f"},
            {                     "k": 1}
        ]}}]

        test = {
            "data": data,
            "query": {
                "from": TEST_TABLE + ".a._b",
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
                    {"v": ["bb", "bb"], "count": 1},
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
                    [["bb", "bb"], 1],
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
                                {"dataIndex": 1, "value": ["bb", "bb"]},
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
        self.utils.execute_tests(test)

    def test_deep_agg_w_deeper_select_relative_name_neop(self):
        data = [{"a": {"_b": [
            {"r": {"s": "a"}, "v": {"u": 1}},
            {"r": {"s": "a"}, "v": {"u": 2}},
            {"r": {"s": "b"}, "v": {"u": 3}},
            {"r": {"s": "b"}, "v": {"u": 4}},
            {"r": {"s": "c"}, "v": {"u": 5}},
            {"v": {"u": 6}}
        ]}}]

        test = {
            "data": data,
            "query": {
                "select": {"value": "v.u", "aggregate": "sum"},  # TEST RELATIVE NAME IN select
                "from": TEST_TABLE + ".a._b",
                "edges": ["r.s"],  # TEST RELATIVE NAME IN edges
                "where": {"ne": {"r.s": "b"}} # TEST RELATIVE NAME IN where
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"r": {"s": "a"}, "v": {"u": 3}},
                    {"r": {"s": "b"}, "v": NULL},
                    {"r": {"s": "c"}, "v": {"u": 5}},
                    {"r": NULL, "v": {"u": 6}}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["r.s", "v.u"],
                "data": [
                    ["a", 3],
                    ["b", NULL],
                    ["c", 5],
                    [NULL, 6]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "r.s", "domain": {"type": "set", "partitions": [
                        {"value": "a"},
                        {"value": "b"},
                        {"value": "c"}
                    ]}}
                ],
                "data": {
                    "v.u": [3, NULL, 5, 6]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_setop_w_deep_select_value_neop(self):
        data = [{"a": {"_b": [
            {"r": {"s": "a"}, "v": {"u": 1}},
            {"r": {"s": "a"}, "v": {"u": 2}},
            {"r": {"s": "b"}, "v": {"u": 3}},
            {"r": {"s": "b"}, "v": {"u": 4}},
            {"r": {"s": "c"}, "v": {"u": 5}},
            {"v": {"u": 6}}
        ]}}]

        test = {
            "data": data,
            "query": {
                "select": ["r.s", "v.u"],
                "from": TEST_TABLE + ".a._b",
                "where": {"ne": {"r.s": "b"}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"r": {"s": "a"}, "v": {"u": 1}},
                    {"r": {"s": "a"}, "v": {"u": 2}},
                    {"r": {"s": "c"}, "v": {"u": 5}},
                    {"v": {"u": 6}}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["r.s", "v.u"],
                "data": [
                    ["a", 1],
                    ["a", 2],
                    ["c", 5],
                    [NULL, 6]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "rownum", "domain": {
                        "type": "rownum",
                        "min": 0,
                        "max": 4,
                        "interval": 1
                    }}
                ],
                "data": {
                    "v.u": [1, 2, 5, 6],
                    "r.s": ["a", "a", "c", NULL]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_deep_agg_w_deeper_select_relative_name(self):
        data = [{"a": {"_b": [
            {"r": {"s": "a"}, "v": {"u": 1}},
            {"r": {"s": "a"}, "v": {"u": 2}},
            {"r": {"s": "b"}, "v": {"u": 3}},
            {"r": {"s": "b"}, "v": {"u": 4}},
            {"r": {"s": "c"}, "v": {"u": 5}},
            {"v": {"u": 6}}
        ]}}]

        test = {
            "data": data,
            "query": {
                "select": {"value": "v.u", "aggregate": "sum"},  # TEST RELATIVE NAME IN select
                "from": TEST_TABLE + ".a._b",
                "edges": ["r.s"],  # TEST RELATIVE NAME IN edges
                "where": {"not": {"eq": {"r.s": "b"}}}  # TEST RELATIVE NAME IN where
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"r": {"s": "a"}, "v": {"u": 3}},
                    {"r": {"s": "b"}, "v": NULL},
                    {"r": {"s": "c"}, "v": {"u": 5}},
                    {"r": NULL, "v": {"u": 6}}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["r.s", "v.u"],
                "data": [
                    ["a", 3],
                    ["b", NULL],
                    ["c", 5],
                    [NULL, 6]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "r.s", "domain": {"type": "set", "partitions": [
                        {"value": "a"},
                        {"value": "b"},
                        {"value": "c"}
                    ]}}
                ],
                "data": {
                    "v.u": [3, NULL, 5, 6]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_setop_w_deep_select_value(self):
        data = [{"a": {"_b": [
            {"r": {"s": "a"}, "v": {"u": 1}},
            {"r": {"s": "a"}, "v": {"u": 2}},
            {"r": {"s": "b"}, "v": {"u": 3}},
            {"r": {"s": "b"}, "v": {"u": 4}},
            {"r": {"s": "c"}, "v": {"u": 5}},
            {"v": {"u": 6}}
        ]}}]

        test = {
            "data": data,
            "query": {
                "select": ["r.s", "v.u"],
                "from": TEST_TABLE + ".a._b",
                "where": {"not": {"eq": {"r.s": "b"}}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"r": {"s": "a"}, "v": {"u": 1}},
                    {"r": {"s": "a"}, "v": {"u": 2}},
                    {"r": {"s": "c"}, "v": {"u": 5}},
                    {"v": {"u": 6}}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["r.s", "v.u"],
                "data": [
                    ["a", 1],
                    ["a", 2],
                    ["c", 5],
                    [NULL, 6]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {"name": "rownum", "domain": {
                        "type": "rownum",
                        "min": 0,
                        "max": 4,
                        "interval": 1
                    }}
                ],
                "data": {
                    "v.u": [1, 2, 5, 6],
                    "r.s": ["a", "a", "c", NULL]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_select_average_on_none(self):
        test = {
            "data": [{"a": {"_b": [
                {"a": 0},
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
                    {"a": 0, "t": 0},
                    {"t": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_missing(self):
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
                    "b"
                ],
                "where": {"missing": "a"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"b": 0},
                    {"b": 1},
                    {}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_missing_on_not_exists(self):
        # CHECKING FOR A MISSING COLUMN THAT DOES NOT EXIST SHOULD NOT THROW AN ERROR, RATHER RETURN true
        test = {
            "data": [
                {
                    "a": {"_b": [{"a": 1}, {"a": 2}]},
                    "b": 0
                }
            ],
            "query": {
                "from": TEST_TABLE+".a._b",
                "select": [
                    "a"
                ],
                "where": {"missing": "c"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 1},
                    {"a": 2}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_exists(self):
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
                    "b"
                ],
                "where": {"exists": "a"}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": 0, "b": 0},
                    {"a": 0, "b": 1},
                    {"a": 0},
                    {"a": 1, "b": 0},
                    {"a": 1, "b": 1},
                    {"a": 1}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_deep_or(self):
        test = {
            "data": [{"a": {"_b": [
                {"q": {"a": 0, "b": 0}},
                {"q": {"a": 0, "b": 1}},
                {"q": {"a": 0}},
                {"q": {"a": 1, "b": 0}},
                {"q": {"a": 1, "b": 1}},
                {"q": {"a": 1}},
                {"q": {"b": 0}},
                {"q": {"b": 1}},
                {"q": {}}
            ]}}],
            "query": {
                "from": TEST_TABLE+".a._b",
                "select": [
                    "q.a",
                    "q.b"
                ],
                "where": {"or": [
                    {"exists": "q.b"},
                    {"exists": "q.a"}
                ]}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"q": {"a": 0, "b": 0}},
                    {"q": {"a": 0, "b": 1}},
                    {"q": {"a": 0}},
                    {"q": {"a": 1, "b": 0}},
                    {"q": {"a": 1, "b": 1}},
                    {"q": {"a": 1}},
                    {"q": {"b": 0}},
                    {"q": {"b": 1}}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_sibling_nested_column(self):
        test = {
            "data": [{"a": {
                "_b": [{"f": "a"}, {"f": "b"}],
                "_c": [{"f": "c"}, {"f": "d"}]
            }
            }],
            "query": {
                "from": TEST_TABLE + ".a._b",
                "select": ["f"]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"f": "a"},
                    {"f": "b"}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_deep_star(self):
        test = {
            "data": [{"x": 1, "a": {"_b": [
                {"q": {"a": 0, "b": 1}},
                {"q": {"a": 0}},
                {"q": {}}
            ]}}],
            "query": {
                "from": TEST_TABLE+".a._b",
                "select": ["*"],
                "format": "list"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"q.a": 0, "q.b": 1, "x": 1},
                    {"q.a": 0, "x": 1},
                    {"x": 1}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_from_shallow_select_deep_column(self):
        test = {
            "data": [
                {"_a": [{"b": 1, "c": 2}, {"b": 3, "c": 4}]},
                {"_a": [{"b": 5, "c": 6}, {"b": 7, "c": 8}]}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "_a.b"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    [1, 3],
                    [5, 7]
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["_a.b"],
                "data": [
                    [[1, 3]],
                    [[5, 7]]
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
                    "_a.b": [
                        [1, 3],
                        [5, 7]
                    ]
                }
            }
        }
        self.utils.execute_tests(test)


# TODO: using "find" as a filter should be legitimate:
todo = {
    "from": "task.task.artifacts",
    "where": {"and": [
        {"gt": {"action.start_time": {"date": "today-3day"}}},
        {"find": {"name": "gcda"}}
    ]}
}

# TODO: WHAT DOES * MEAN IN THE CONTEXT OF A DEEP QUERY?
# THIS SHOULD RETURN SOMETHING, NOT FAIL
todo = {
    "select": [
        "*",
        "action.*"
    ],
    "from": "jobs.action.timings",
    "format": "list"
}





