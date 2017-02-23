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

from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times.dates import Date
from mo_times.durations import WEEK


class TestQueryVariables(FuzzyTestCase):

    def test_simple(self):
        today = Date.today()

        result = replace_vars('"{{today|week}}" "{{var}}"', {"var": 20})
        expect = '"'+unicode(today.floor(WEEK).unix)+'" "20"'
        self.assertEqual(result, expect)

    def test_two_simple(self):
        today = Date.today()

        result = replace_vars('"{{today|week}}" "{{today}}d"')
        expect = '"'+unicode(today.floor(WEEK).unix)+'" "'+unicode(today.unix)+'d"'
        self.assertEqual(result, expect)

    def test_overload(self):
        today = Date.today()

        result = replace_vars('"{{today|week}}" "{{var}}"', {"today": 1000, "var": 20})
        expect = '"'+unicode(today.floor(WEEK).unix)+'" "20"'
        self.assertEqual(result, expect)

