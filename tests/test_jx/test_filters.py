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
        self.utils.execute_tests(test)

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
        self.utils.execute_tests(test)

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
        self.utils.execute_tests(test)
        # No regexp() user function is defined by default and so use of the
        #REGEXP operator will normally result in an error message.
        #If an application-defined SQL function named "regexp" is added at run-time,
        #then the "X REGEXP Y" operator will be implemented

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
        self.utils.execute_tests(test)


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
        self.utils.execute_tests(test)

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
        self.utils.execute_tests(test)

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
        self.utils.execute_tests(test)

    def test_empty_prefix(self):
        test = {
            "data": [{"v": "test"}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"prefix": {"v": ""}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"v": "test"}]
            }
        }
        self.utils.execute_tests(test)

    def test_null_prefix(self):
        test = {
            "data": [{"v": "test"}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"prefix": {"v": None}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"v": "test"}]
            }
        }
        self.utils.execute_tests(test)

    def test_edges_and_empty_prefix(self):
        test = {
            "data": [{"v": "test"}],
            "query": {
                "from": TEST_TABLE,
                "edges": "v",
                "where": {"prefix": {"v": ""}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"v": "test", "count": 1}]
            }
        }
        self.utils.execute_tests(test)


    def test_edges_and_null_prefix(self):
        test = {
            "data": [{"v": "test"}],
            "query": {
                "from": TEST_TABLE,
                "edges": "v",
                "where": {"prefix": {"v": None}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"v": "test", "count": 1}]
            }
        }
        self.utils.execute_tests(test)

    def test_suffix(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"suffix": {"v": "test"}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list"},
                "data": [
                    {"v": "this-is-a-test"},
                    {"v": "test"}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_null_suffix(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"postfix": {"v": None}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list"},
                "data": [
                    {"v": "this-is-a-test"},
                    {"v": "this-is-a-vest"},
                    {"v": "test"},
                    {"v": NULL},
                    {"v": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_empty_suffix(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"postfix": {"v": ""}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list"},
                "data": [
                    {"v": "this-is-a-test"},
                    {"v": "this-is-a-vest"},
                    {"v": "test"},
                    {"v": NULL},
                    {"v": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_eq_with_boolean(self):
        test = {
            "data": [
                {"v": True},
                {"v": True},
                {"v": True},
                {"v": False},
                {"v": False},
                {"v": False},
                {"v": None},
                {"v": None},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"eq": {"v": "T"}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": [
                    {"v": True},
                    {"v": True},
                    {"v": True}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_big_integers_in_script(self):
        bigger_than_int32 = 1547 * 1000 * 1000 * 1000
        test = {
            "data": [
                {"v": 42}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"lt": [0, {"mul": ["v", bigger_than_int32]}]}  # SOMETHING COMPLICATED ENOUGH TO FORCE SCRIPTING
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": [
                    {"v": 42}
                ]
            }
        }
        self.utils.execute_tests(test)
