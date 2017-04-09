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

from mo_dots import set_default, wrap

from pyLibrary.meta import extenstion_method
from tests.test_jx import BaseTestCase


class TestMetadata(BaseTestCase):


    def test_meta(self):
        test = wrap({
            "query": {"from": "meta.columns"},
            "data": [
                {"a": "b"}
            ]
        })

        settings = self.utils.fill_container(test, tjson=False)

        table_name = settings.index

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

        test = set_default(test, {
            "query": {
                "select": ["name", "table", "type", "nested_path"],
                "from": "meta.columns",
                "where": {"eq": {"table": table_name}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
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
                    "table": [table_name],
                    "name": ["_id", "a"],
                    "type": ["string", "string"],
                    "nested_path": [".", "."]
                }
            }
        })
        self.utils.send_queries(test)

    def test_get_nested_columns(self):
        settings = self.utils.fill_container({
            "query": {"from": "meta.columns"},  # DUMMY QUERY
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

        table_name = settings.index

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
                    {"o": 3, "_a": [{"b": "x", "v": 7}]},
                    {"o": 4, "c": "x"}
                ]}
        }
        self.utils.send_queries(pre_test)

        test = {
            "query": {
                "select": ["name", "table", "type", "nested_path"],
                "from": "meta.columns",
                "where": {"term": {"table": table_name}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"table": table_name, "name": "_id", "type": "string", "nested_path": "."},
                    {"table": table_name, "name": "_a", "type": "nested", "nested_path": "."},
                    {"table": table_name, "name": "_a.b", "type": "string", "nested_path": ["_a", "."]},
                    {"table": table_name, "name": "_a.v", "type": "double", "nested_path": ["_a", "."]},
                    {"table": table_name, "name": "c", "type": "string", "nested_path": "."},
                    {"table": table_name, "name": "o", "type": "double", "nested_path": "."},
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["table", "name", "nested_path", "type"],
                "data": [
                    [table_name, "_id", ".", "string"],
                    [table_name, "_a", ".", "nested"],
                    [table_name, "_a.b", ["_a", "."], "string"],
                    [table_name, "_a.v", ["_a", "."], "double"],
                    [table_name, "c", ".", "string"],
                    [table_name, "o", ".", "double"]
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
            print(print_me)
            return print_me, self.value

        self.assertEqual(a.my_func("testing"), ("testing", "test_value"), "Expecting method to be run")
