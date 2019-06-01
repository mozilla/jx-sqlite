# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

import jx_base
from jx_sqlite.schema import Schema
from mo_logs import Log


class Table(jx_base.Table):

    def __init__(self, nested_path, snowflake):
        if not isinstance(nested_path, list):
            Log.error("Expecting list of paths")
        self.nested_path = nested_path
        self.schema = Schema(nested_path, snowflake)
        # self.columns = []  # PLAIN DATABASE COLUMNS

    @property
    def name(self):
        """
        :return: THE TABLE NAME RELATIVE TO THE FACT TABLE
        """
        return self.nested_path[0]

    def map(self, mapping):
        return self

