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

from mo_logs import Log
from mo_testing.fuzzytestcase import FuzzyTestCase

from pyLibrary.queries.expressions import NullOp

TEST_TABLE = "testdata"
NULL = NullOp

global_settings = None


class BaseTestCase(FuzzyTestCase):

    utils = None

    def __init__(self, utils, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)
        if not utils:
            Log.error("Something wrong with test setup")
        BaseTestCase.utils = utils


    @classmethod
    def setUpClass(cls):
        BaseTestCase.utils.setUpClass()

    @classmethod
    def tearDownClass(cls):
        BaseTestCase.utils.tearDownClass()

    def setUp(self):
        BaseTestCase.utils.setUp()

    def tearDown(self):
        BaseTestCase.utils.tearDown()
