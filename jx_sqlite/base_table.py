# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import, division, unicode_literals

import jx_base
from mo_kwargs import override


class BaseTable(jx_base.Table):
    @override
    def __init__(self, name, container):
        """
        :param name: NAME FOR THIS TABLE
        :param db: THE DB TO USE
        :param uid: THE UNIQUE INDEX FOR THIS TABLE
        :return: HANDLE FOR TABLE IN db
        """
        self.name = name
        self.db = container.db
        self.container = container

    @property
    def sf(self):
        return self.schema.snowflake

    @property
    def namespace(self):
        return self.container.ns

    @property
    def schema(self):
        return self.container.ns.get_schema(self.name)

