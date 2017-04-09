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

from tests.test_jx import BaseTestCase, TEST_TABLE, global_settings


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
                    [
                        ["b"],
                        [[{"b": 1}, {"b": 2}]],
                        [3]
                    ]
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
                        ["b"],
                        [[{"b": 1}, {"b": 2}]],
                        [3]
                    ]
                }
            }
        }
        self.utils.execute_es_tests(test)




