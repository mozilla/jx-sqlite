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

from mo_dots import wrap
from mo_future import text_type
from mo_logs import Log
from mo_logs.exceptions import extract_stack
from pyLibrary.meta import extenstion_method
from tests.test_jx import BaseTestCase, TEST_TABLE


class TestMetadata(BaseTestCase):

    def test_meta_tables(self):
        pre_test = {
            "data": [{"a": "b"}],
            "query": {"from": TEST_TABLE},  # DUMMY QUERY
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"a": "b"}]
            }
        }
        self.utils.execute_tests(pre_test)

        test = {
            "query": {
                "from": "meta.tables"
            },
            "expecting_list": {
                "meta": {"format": "list"}
            }
        }
        self.utils.send_queries(test)

    def test_meta(self):
        test = wrap({
            "query": {"from": TEST_TABLE},
            "data": [
                {"a": "b"}
            ]
        })

        settings = self.utils.fill_container(test, typed=False)

        table_name = settings.alias

        # WE REQUIRE A QUERY TO FORCE LOADING OF METADATA
        pre_test = {
            "query": {
                "from": table_name
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"a": "b"}]
            }
        }
        self.utils.send_queries(pre_test)

        test = {
            "query": {
                "select": ["name", "table", "type", "nested_path"],
                "from": "meta.columns",
                "where": {"eq": {"table": table_name}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"table": table_name, "name": "_id", "type": "string", "nested_path": "."},
                    {"table": table_name, "name": "a", "type": "string", "nested_path": "."}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["table", "name", "type", "nested_path"],
                "data": [
                    [table_name, "_id", "string", "."],
                    [table_name, "a", "string", "."]
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
                    "table": [table_name, table_name],
                    "name": ["_id", "a"],
                    "type": ["string", "string"],
                    "nested_path": [".", "."]
                }
            }
        }
        self.utils.send_queries(test)

    def test_get_nested_columns(self):
        settings = self.utils.fill_container({
            "query": {"from": TEST_TABLE},  # DUMMY QUERY
            "data": [
                {"o": 1, "_a": [
                    {"b": "x", "v": 2},
                    {"b": "y", "v": 3}
                ]},
                {"o": 2, "_a": {"b": "x", "v": 5}},
                {"o": 3, "_a": [
                    {"b": "x", "v": 7}
                ]},
                {"o": 4, "c": "x"}
            ]})

        table_name = settings.alias

        # WE REQUIRE A QUERY TO FORCE LOADING OF METADATA
        pre_test = {
            "query": {
                "from": table_name,
                "sort": "o"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                    {"o": 1, "_a": [
                        {"b": "x", "v": 2},
                        {"b": "y", "v": 3}
                    ]},
                    {"o": 2, "_a": {"b": "x", "v": 5}},
                    {"o": 3, "_a": {"b": "x", "v": 7}},
                    {"o": 4, "c": "x"}
                ]}
        }
        self.utils.send_queries(pre_test)

        test = {
            "query": {
                "select": ["name", "table", "type", "nested_path"],
                "from": "meta.columns",
                "where": {"eq": {"table": table_name}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"table": table_name, "name": "_id", "type": "string", "nested_path": "."},
                    {"table": table_name, "name": "_a.b", "type": "string", "nested_path": ["_a", "."]},
                    {"table": table_name, "name": "_a.v", "type": "number", "nested_path": ["_a", "."]},
                    {"table": table_name, "name": "c", "type": "string", "nested_path": "."},
                    {"table": table_name, "name": "o", "type": "number", "nested_path": "."},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["table", "name", "nested_path", "type"],
                "data": [
                    [table_name, "_id", ".", "string"],
                    [table_name, "_a.b", ["_a", "."], "string"],
                    [table_name, "_a.v", ["_a", "."], "number"],
                    [table_name, "c", ".", "string"],
                    [table_name, "o", ".", "number"]
                ]
            }
        }

        self.utils.send_queries(test)

    def test_assign(self):
        class TestClass(object):
            def __init__(self, value):
                self.value=value

        a = TestClass("test_value")

        @extenstion_method(TestClass)
        def my_func(self, print_me):
            return print_me, self.value

        self.assertEqual(a.my_func("testing"), ("testing", "test_value"), "Expecting method to be run")

    def test_cardinality(self):
        pre_test = wrap({
            "data": [{"a": "b"}, {"a": "c"}],
            "query": {"from": TEST_TABLE},  # DUMMY QUERY
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"a": "b"}, {"a": "c"}]
            }
        })
        settings = self.utils.fill_container(pre_test)
        self.utils.send_queries(pre_test)

        test = {
            "query": {
                "from": "meta.columns",
                "select": "cardinality",
                "where": {
                    "and": [
                        {
                            "eq": {
                                "table": settings.alias
                            }
                        },
                        {
                            "eq": {
                                "name": "a"
                            }
                        }
                    ]
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    2
                ]
            }
        }
        Log.note("table = {{table}}", table=pre_test.query['from'])
        subtest = wrap(test)
        self.utils.send_queries(subtest)
