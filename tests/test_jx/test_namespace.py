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

from unittest import skip

from pyLibrary.queries.jx_usingES import FromES
from pyLibrary.queries.namespace.rename import Rename
from pyLibrary.queries.namespace.typed import Typed
from tests.test_jx import BaseTestCase, TEST_TABLE, NULL


class Namespace(BaseTestCase):
    """
    TEST A VARIETY OF RE-NAMINGS
    """

    @skip("not working yet")
    def test_rename_select(self):
        self._run_test(
            query={
                "from": TEST_TABLE,
                "select": ["o", "w"],
                "format": "table"
            },
            data=deep_test_data,
            dimensions={"w": "a.v"},
            expect={
                "header": ["o", "w"],
                "data": [
                    [3, 2],
                    [1, 5],
                    [2, 7],
                    [4, NULL]
                ]
            }
        )

    @skip("not working yet")
    def test_rename_select_to_struct(self):
        self._run_test(
            query={
                "from": TEST_TABLE,
                "select": ["o", "w"],
                "format": "table"
            },
            dimensions={"w": {"a": "a.v", "b": "a.b"}},
            data=deep_test_data,
            expect={
                "header": ["o", "w.a", "w.b"],
                "data": [
                    [3, 2, "x"],
                    [1, 5, "x"],
                    [2, 7, "x"],
                    [4, NULL, NULL]
                ]
            }
        )

    @skip("not working yet")
    def test_rename_select_to_list(self):
        self._run_test(
            query={
                "from": TEST_TABLE,
                "select": ["o", "w"],
                "format": "table"
            },
            dimensions={"w": ["a.v", "a.b"]},
            data=deep_test_data,
            expect={
                "header": ["o", "w"],
                "data": [
                    [3, [2, "x"]],
                    [1, [5, "x"]],
                    [2, [7, "x"]],
                    [4, [NULL, NULL]]
                ]
            }
        )

    @skip("not working yet")
    def test_rename_edge(self):
        self._run_test(
            query={
                "from": TEST_TABLE,
                "edges": ["w"],
                "format": "table"
            },
            dimensions={"w": "a.b"},
            data=deep_test_data,
            expect={
                "header": ["w", "count"],
                "data": [
                    ["x", 3],
                    [NULL, 1]
                ]
            }
        )

    @skip("not working yet")
    def test_rename_edge_to_struct(self):
        query = {
            "from": TEST_TABLE,
            "edges": ["w"],
            "format": "table"
        }

        self.utils.fill_container({"query":query, "data": deep_test_data})
        db = FromES(kwargs=base_test_case.settings.backend_es)
        db.namespaces += [Rename(dimensions={"name": "w", "fields": {"a": "a.v", "b": "a.b"}}), Typed()]
        result = db.query(query)
        self.compare_to_expected(query, result, {
            "header": ["w.a", "w.b", "count"],
            "data": [
                [2, "x", 1],
                [5, "x", 1],
                [7, "x", 1],
                [NULL, NULL, 1]
            ]
        })

    @skip("not working yet")
    def test_rename_edge_to_list(self):
        """
        EXPAND DIMENSION
        """
        self._run_test(
            query={
                "from": TEST_TABLE,
                "edges": ["w"],
                "format": "cube"
            },
            dimensions={"w": ["a.v", "a.b"]},
            data=deep_test_data,
            expect={
                "edges": [{
                    "name": "w",
                    "domain": {"type": "set", "partitions": [
                        {"value": [2, "x"]},
                        {"value": [5, "x"]},
                        {"value": [7, "x"]},
                        {"value": NULL}
                    ]}
                }],
                "data": {"count": [1, 1, 1, 1]}
            }
        )

    def _run_test(self, data, query, expect, dimensions):
        new_settings = self.utils.fill_container({"query": query, "data": data}, tjson=True)
        db = FromES(kwargs=new_settings)
        db.namespaces += [Rename(dimensions=dimensions, source=db)]
        result = db.query(query)
        self.compare_to_expected(query, result, expect)



deep_test_data = [
    {"o": 3, "a": {"b": "x", "v": 2}},
    {"o": 1, "a": {"b": "x", "v": 5}},
    {"o": 2, "a": {"b": "x", "v": 7}},
    {"o": 4, "c": "x"}
]


