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
NULL = NullOp()

global_settings = None
utils = None


class BaseTestCase(FuzzyTestCase):

    def __init__(self, *args, **kwargs):
        FuzzyTestCase.__init__(self, *args, **kwargs)
        if not utils:
            try:
                import tests
            except Exception:
                Log.error("Expecting ./tests/__init__.py to set `global_settings` and `utils` so tests can be run")
        self.utils = utils

    @classmethod
    def setUpClass(cls):
        utils.setUpClass()

    @classmethod
    def tearDownClass(cls):
        utils.tearDownClass()

    def setUp(self):
        utils.setUp()

    def tearDown(self):
        utils.tearDown()
