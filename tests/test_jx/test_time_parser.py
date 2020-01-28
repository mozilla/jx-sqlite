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

from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times.dates import Date, parse
from mo_times.durations import DAY, MONTH, WEEK


class TestTimeParser(FuzzyTestCase):
    def test_now(self):
        self.assertAlmostEqual(parse("now").unix, Date.now().unix, places=9)  # IGNORE THE LEAST SIGNIFICANT MILLISECOND

    def test_today(self):
        self.assertAlmostEqual(parse("today").unix, Date.today().unix)

    def test_yesterday(self):
        self.assertAlmostEqual(parse("today-day").unix, (Date.today() - DAY).unix)

    def test_last_week(self):
        self.assertAlmostEqual(parse("today-7day").unix, (Date.today() - DAY * 7).unix)

    def test_next_week(self):
        self.assertAlmostEqual(parse("today+7day").unix, (Date.today() + DAY * 7).unix)

    def test_week_before(self):
        self.assertAlmostEqual(parse("today-2week").unix, (Date.today() - WEEK * 2).unix)

    def test_last_year(self):
        self.assertAlmostEqual(parse("today-12month").unix, (Date.today() - MONTH * 12).unix)

    def test_beginning_of_month(self):
        self.assertAlmostEqual(parse("today|month").unix, Date.today().floor(MONTH).unix)

    def test_end_of_month(self):
        self.assertAlmostEqual(parse("today|month+month").unix, Date.today().floor(MONTH).add(MONTH).unix)

    def test_13_weeks(self):
        self.assertAlmostEqual(parse("13week").seconds, (WEEK * 13).seconds)

    def test_bad_floor(self):
        self.assertRaises(Exception, parse, "today - week|week")
