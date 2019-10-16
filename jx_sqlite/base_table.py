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
from jx_base import generateGuid
from jx_python import jx
from jx_sqlite import UID
from jx_sqlite.namespace import Namespace
from mo_kwargs import override
from mo_logs import Log
from pyLibrary.sql import SQL_INSERT, sql_list, sql_iso
from pyLibrary.sql.sqlite import Sqlite, quote_column, quote_value

_config = None


class BaseTable(jx_base.Table):
    @override
    def __init__(self, name, db=None, uid=UID, kwargs=None):
        """
        :param name: NAME FOR THIS TABLE
        :param db: THE DB TO USE
        :param uid: THE UNIQUE INDEX FOR THIS TABLE
        :return: HANDLE FOR TABLE IN db
        """
        global _config
        if isinstance(db, Sqlite):
            self.db = db
        else:
            self.db = db = Sqlite(db)

        if not _config:
            # REGISTER sqlite AS THE DEFAULT CONTAINER TYPE
            from jx_base.container import config as _config

            if not _config.default:
                _config.default = {"type": "sqlite", "settings": {"db": db}}

        ns = Namespace(db=db)
        self.facts = ns.create_or_replace_facts(fact_name=name)

        self._next_guid = generateGuid()
        self._next_uid = 1
        self._make_digits_table()
        self.uid_accessor = jx.get(uid)

    def _make_digits_table(self):
        existence = self.db.query("PRAGMA table_info(__digits__)")
        if not existence.data:
            with self.db.transaction() as t:
                t.execute(
                    "CREATE TABLE" + quote_column(DIGITS_TABLE) + "(value INTEGER)"
                )
                t.execute(
                    SQL_INSERT
                    + quote_column(DIGITS_TABLE)
                    + sql_list(
                        sql_iso(quote_value(i))
                        for i in range(10)
                    )
                )

    @property
    def sf(self):
        return self.facts.snowflake

    @property
    def namespace(self):
        return self.facts.snowflake.namespace

    @property
    def schema(self):
        return self.facts.snowflake.column  # THIS IS A LOOKUP TOOL

    @property
    def name(self):
        return self.facts.snowflake.fact_name

    def get_table(self, table_name):
        if self.name != table_name:
            Log.error("expecting to match name")
        return self


DIGITS_TABLE = "__digits__"
