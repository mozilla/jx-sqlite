# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_kwargs import override

from jx_sqlite import GUID
from jx_sqlite.snowflake import Snowflake
from pyLibrary.queries import jx
from pyLibrary.queries.containers import Container
from pyLibrary.sql.sqlite import Sqlite

_config=None


class BaseTable(Container):
    @override
    def __init__(self, name, db=None, uid=GUID, kwargs=None):
        """
        :param name: NAME FOR THIS TABLE
        :param db: THE DB TO USE
        :param uid: THE UNIQUE INDEX FOR THIS TABLE
        :return: HANDLE FOR TABLE IN db
        """
        global _config
        Container.__init__(self, frum=None)
        if db:
            self.db = db
        else:
            self.db = db = Sqlite()

        if not _config:
            # REGISTER sqlite AS THE DEFAULT CONTAINER TYPE
            from pyLibrary.queries.containers import config as _config
            if not _config.default:
                _config.default = {
                    "type": "sqlite",
                    "settings": {"db": db}
                }

        self.sf = Snowflake(fact=name, uid=uid, db=db)

        self._next_uid = 1
        self._make_digits_table()
        self.uid_accessor = jx.get(self.sf.uid)


    def _make_digits_table(self):
        existence = self.db.query("PRAGMA table_info(__digits__)")
        if not existence.data:
            self.db.execute("CREATE TABLE __digits__(value INTEGER)")
            self.db.execute("INSERT INTO __digits__ " + "\nUNION ALL ".join("SELECT " + unicode(i) for i in range(10)))


