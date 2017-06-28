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


class TestSchemaMerging(BaseTestCase):
    """
    TESTS THAT DEMONSTRATE DIFFERENT SCHEMAS
    """

    @skipIf(global_settings.use == "elasticsearch", "require dynamic typing before overloading objects and primitives")
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
                {"a": "b"},
                {"a": 3},
                {"a": "c"}
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
        self.utils.execute_tests(test)

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

