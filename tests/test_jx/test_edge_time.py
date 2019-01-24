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

from mo_times.dates import Date
from mo_times.durations import DAY
from tests.test_jx import BaseTestCase, TEST_TABLE

FROM_DATE = Date.today()-7*DAY
TO_DATE = Date.today()

simple_test_data =[
    {"run":{"timestamp": Date("now-4day"), "value": 1}},
    {"run":{"timestamp": Date("now-4day"), "value": 2}},
    {"run":{"timestamp": Date("now-4day"), "value": 3}},
    {"run":{"timestamp": Date("now-4day"), "value": 4}},
    {"run":{"timestamp": Date("now-3day"), "value": 5}},
    {"run":{"timestamp": Date("now-3day"), "value": 6}},
    {"run":{"timestamp": Date("now-3day"), "value": 7}},
    {"run":{"timestamp": Date("now-2day"), "value": 8}},
    {"run":{"timestamp": Date("now-2day"), "value": 9}},
    {"run":{"timestamp": Date("now-1day"), "value": 0}},
    {"run":{"timestamp": Date("now-5day"), "value": 1}},
    {"run":{"timestamp": Date("now-5day"), "value": 2}},
    {"run":{"timestamp": Date("now-5day"), "value": 3}},
    {"run":{"timestamp": Date("now-5day"), "value": 4}},
    {"run":{"timestamp": Date("now-5day"), "value": 5}},
    {"run":{"timestamp": Date("now-6day"), "value": 6}},
    {"run":{"timestamp": Date("now-6day"), "value": 7}},
    {"run":{"timestamp": Date("now-6day"), "value": 8}},
    {"run":{"timestamp": Date("now-6day"), "value": 9}},
    {"run":{"timestamp": Date("now-6day"), "value": 0}},
    {"run":{"timestamp": Date("now-6day"), "value": 1}},
    {"run":{"timestamp": Date("now-0day"), "value": 2}},
    {"run":{"timestamp": Date("now-0day"), "value": 3}},
    {"run":{"timestamp": Date("now-0day"), "value": 4}},
    {"run":{"timestamp": Date("now-0day"), "value": 5}}
]


class TestEdge1(BaseTestCase):

    def test_count_over_time(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "edges": [
                    {
                        "value": "run.timestamp",
                        "allowNulls": False,
                        "domain": {
                            "type": "time",
                            "min": "today-week",
                            "max": "today",
                            "interval": "day"
                        }
                    }
                ]
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"run": {"timestamp": (FROM_DATE + 0 * DAY).unix}, "count": 0},
                    {"run": {"timestamp": (FROM_DATE + 1 * DAY).unix}, "count": 6},
                    {"run": {"timestamp": (FROM_DATE + 2 * DAY).unix}, "count": 5},
                    {"run": {"timestamp": (FROM_DATE + 3 * DAY).unix}, "count": 4},
                    {"run": {"timestamp": (FROM_DATE + 4 * DAY).unix}, "count": 3},
                    {"run": {"timestamp": (FROM_DATE + 5 * DAY).unix}, "count": 2},
                    {"run": {"timestamp": (FROM_DATE + 6 * DAY).unix}, "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["run.timestamp", "count"],
                "data": [
                    [(FROM_DATE + 0 * DAY).unix, 0],
                    [(FROM_DATE + 1 * DAY).unix, 6],
                    [(FROM_DATE + 2 * DAY).unix, 5],
                    [(FROM_DATE + 3 * DAY).unix, 4],
                    [(FROM_DATE + 4 * DAY).unix, 3],
                    [(FROM_DATE + 5 * DAY).unix, 2],
                    [(FROM_DATE + 6 * DAY).unix, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "run.timestamp",
                        "domain": {
                            "type": "time",
                            "key": "min",
                            "partitions": [{"dataIndex": i, "min": m.unix, "max": (m + DAY).unix} for i, m in enumerate(Date.range(FROM_DATE, TO_DATE, DAY))],
                            "min": FROM_DATE.unix,
                            "max": TO_DATE.unix,
                            "interval": DAY.seconds
                        }
                    }
                ],
                "data": {
                    "count": [0, 6, 5, 4, 3, 2, 1]
                }
            }
        }
        self.utils.execute_tests(test)

    def test_count_over_time_w_sort(self):
        test = {
            "data": simple_test_data,
            "query": {
                "from": TEST_TABLE,
                "edges": [
                    {
                        "value": "run.timestamp",
                        "allowNulls": False,
                        "domain": {
                            "type": "time",
                            "min": "today-week",
                            "max": "today",
                            "interval": "day"
                        }
                    }
                ],
                "sort": "run.timestamp"
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"run": {"timestamp": (FROM_DATE + 0 * DAY).unix}, "count": 0},
                    {"run": {"timestamp": (FROM_DATE + 1 * DAY).unix}, "count": 6},
                    {"run": {"timestamp": (FROM_DATE + 2 * DAY).unix}, "count": 5},
                    {"run": {"timestamp": (FROM_DATE + 3 * DAY).unix}, "count": 4},
                    {"run": {"timestamp": (FROM_DATE + 4 * DAY).unix}, "count": 3},
                    {"run": {"timestamp": (FROM_DATE + 5 * DAY).unix}, "count": 2},
                    {"run": {"timestamp": (FROM_DATE + 6 * DAY).unix}, "count": 1}
                ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["run.timestamp", "count"],
                "data": [
                    [(FROM_DATE + 0 * DAY).unix, 0],
                    [(FROM_DATE + 1 * DAY).unix, 6],
                    [(FROM_DATE + 2 * DAY).unix, 5],
                    [(FROM_DATE + 3 * DAY).unix, 4],
                    [(FROM_DATE + 4 * DAY).unix, 3],
                    [(FROM_DATE + 5 * DAY).unix, 2],
                    [(FROM_DATE + 6 * DAY).unix, 1]
                ]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "run.timestamp",
                        "domain": {
                            "type": "time",
                            "key": "min",
                            "partitions": [{"dataIndex": i, "min": m.unix, "max": (m + DAY).unix} for i, m in enumerate(Date.range(FROM_DATE, TO_DATE, DAY))],
                            "min": FROM_DATE.unix,
                            "max": TO_DATE.unix,
                            "interval": DAY.seconds
                        }
                    }
                ],
                "data": {
                    "count": [0, 6, 5, 4, 3, 2, 1]
                }
            }
        }
        self.utils.execute_tests(test)

