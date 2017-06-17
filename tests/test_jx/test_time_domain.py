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
from mo_times.dates import Date
from mo_times.durations import WEEK, DAY
from tests.test_jx import BaseTestCase, TEST_TABLE, global_settings, NULL

TODAY = Date.today()

test_data_1 = [
    {"a": "x", "t": Date("today").unix, "v": 2},
    {"a": "x", "t": Date("today-day").unix, "v": 2},
    {"a": "x", "t": Date("today-2day").unix, "v": 3},
    {"a": "x", "t": Date("today-3day").unix, "v": 5},
    {"a": "x", "t": Date("today-4day").unix, "v": 7},
    {"a": "x", "t": Date("today-5day").unix, "v": 11},
    {"a": "x", "t": NULL, "v": 27},
    {"a": "y", "t": Date("today-day").unix, "v": 13},
    {"a": "y", "t": Date("today-2day").unix, "v": 17},
    {"a": "y", "t": Date("today-4day").unix, "v": 19},
    {"a": "y", "t": Date("today-5day").unix, "v": 23}
]

expected_list_1 = wrap([
    {"t": (TODAY - WEEK).unix, "v": NULL},
    {"t": (TODAY - 6 * DAY).unix, "v": NULL},
    {"t": (TODAY - 5 * DAY).unix, "v": 34},
    {"t": (TODAY - 4 * DAY).unix, "v": 26},
    {"t": (TODAY - 3 * DAY).unix, "v": 5},
    {"t": (TODAY - 2 * DAY).unix, "v": 20},
    {"t": (TODAY - 1 * DAY).unix, "v": 15},
    {"v": 29}
])

expected2 = wrap([
    {"a": "x", "t": (TODAY - WEEK).unix,    "v": NULL},
    {"a": "x", "t": (TODAY - 6 * DAY).unix, "v": NULL},
    {"a": "x", "t": (TODAY - 5 * DAY).unix, "v": 11},
    {"a": "x", "t": (TODAY - 4 * DAY).unix, "v": 7},
    {"a": "x", "t": (TODAY - 3 * DAY).unix, "v": 5},
    {"a": "x", "t": (TODAY - 2 * DAY).unix, "v": 3},
    {"a": "x", "t": (TODAY - 1 * DAY).unix, "v": 2},
    {"a": "x",                              "v": 29},
    {"a": "y", "t": (TODAY - WEEK).unix,    "v": NULL},
    {"a": "y", "t": (TODAY - 6 * DAY).unix, "v": NULL},
    {"a": "y", "t": (TODAY - 5 * DAY).unix, "v": 23},
    {"a": "y", "t": (TODAY - 4 * DAY).unix, "v": 19},
    {"a": "y", "t": (TODAY - 3 * DAY).unix, "v": NULL},
    {"a": "y", "t": (TODAY - 2 * DAY).unix, "v": 17},
    {"a": "y", "t": (TODAY - 1 * DAY).unix, "v": 13},
    {"a": "y",                              "v": NULL}
])

test_data_3 = [
    {"a": TODAY, "t": Date("today").unix, "v": 2},
    {"a": TODAY, "t": Date("today-day").unix, "v": 2},
    {"a": TODAY, "t": Date("today-2day").unix, "v": 3},
    {"a": TODAY, "t": Date("today-3day").unix, "v": 5},
    {"a": TODAY, "t": Date("today-4day").unix, "v": 7},
    {"a": TODAY, "t": Date("today-5day").unix, "v": 11},
    {"a": TODAY, "t": NULL, "v": 27},
    {"a": TODAY, "t": Date("today-day").unix, "v": 13},
    {"a": TODAY, "t": Date("today-2day").unix, "v": 17},
    {"a": TODAY, "t": Date("today-4day").unix, "v": 19},
    {"a": TODAY, "t": Date("today-5day").unix, "v": 23}
]

expected3 = wrap([
    {"since": -7 * DAY.seconds, "v": NULL},
    {"since": -6 * DAY.seconds, "v": NULL},
    {"since": -5 * DAY.seconds, "v": 34},
    {"since": -4 * DAY.seconds, "v": 26},
    {"since": -3 * DAY.seconds, "v": 5},
    {"since": -2 * DAY.seconds, "v": 20},
    {"since": -1 * DAY.seconds, "v": 15},
    {"since": 0, "v": 2},
    {"since": NULL, "v": 27}
])


class TestTime(BaseTestCase):
    def test_time_variables(self):
        test = {
            "metadata": {},
            "data": test_data_1,
            "query": {
                "from": TEST_TABLE,
                "edges": [
                    {
                        "value": "t",
                        "domain": {
                            "type": "time",
                            "min": "today-week",
                            "max": "today",
                            "interval": "day"
                        }
                    }
                ],
                "select": {
                    "value": "v", "aggregate": "sum"
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [r for r in expected_list_1]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["t", "v"],
                "data": [[r.t, r.v] for r in expected_list_1]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "t",
                        "domain": {
                            "type": "time",
                            "key": "min",
                            "min": Date("today-week").unix,
                            "max": TODAY.unix,
                            "interval": DAY.seconds,
                            "partitions": [{"min": r.t, "max": (Date(r.t) + DAY).unix} for r in expected_list_1 if r.t != None]
                        }
                    }
                ],
                "data": {"v": [r.v for r in expected_list_1]}
            }
        }
        self.utils.execute_tests(test)

    def test_time2_variables(self):
        test = {
            "metadata": {},
            "data": test_data_1,
            "query": {
                "from": TEST_TABLE,
                "edges": [
                    "a",
                    {
                        "value": "t",
                        "domain": {
                            "type": "time",
                            "min": "today-week",
                            "max": "today",
                            "interval": "day"
                        }
                    }
                ],
                "select": {
                    "value": "v", "aggregate": "sum"
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [r for r in expected2]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a", "t", "v"],
                "data": [[r.a, r.t, r.v] for r in expected2]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "a",
                        "domain": {
                            "type": "set",
                            "key": "value",
                            "partitions": [
                                {"name": "x", "value": "x", "dataIndex": 0},
                                {"name": "y", "value": "y", "dataIndex": 1}
                            ]
                        }
                    }, {
                        "name": "t",
                        "domain": {
                            "type": "time",
                            "key": "min",
                            "min": Date("today-week").unix,
                            "max": TODAY.unix,
                            "interval": DAY.seconds,
                            "partitions": [{"min": r.t, "max": (Date(r.t) + DAY).unix} for r in expected2 if r.t != None and r.a == "x"]
                        }
                    }
                ],
                "data": {"v": [
                    [r.v for r in expected2 if r.a == "x"],
                    [r.v for r in expected2 if r.a == "y"],
                    [NULL for r in expected2 if r.a == "x"]
                ]}
            }
        }
        self.utils.execute_tests(test)

    def test_time_expression(self):
        test = {
            "data": test_data_3,
            "query": {
                "from": TEST_TABLE,
                "edges": [
                    {
                        "name": "since",
                        "value": {"sub": ["t", "a"]},
                        "domain": {
                            "type": "duration",
                            "min": "-week",
                            "max": "day",
                            "interval": "day"
                        }
                    }
                ],
                "select": {
                    "value": "v", "aggregate": "sum"
                }
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [r for r in expected3]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["since", "v"],
                "data": [[r.since, r.v] for r in expected3]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "since",
                        "domain": {
                            "type": "duration",
                            "key": "min",
                            "partitions": [
                                {"min": e.since, "max": expected3[i + 1].since}
                                for i, e in enumerate(expected3[0:8:])
                            ]
                        }
                    }
                ],
                "data": {"v": [e.v for e in expected3]}
            }
        }
        self.utils.execute_tests(test)
