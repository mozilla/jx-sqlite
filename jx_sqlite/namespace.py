# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#

from __future__ import absolute_import, division, unicode_literals

from copy import copy, deepcopy

import jx_base
from jx_base import Column, Facts
from jx_sqlite import UID, quoted_GUID, quoted_UID, typed_column
from jx_sqlite.expressions import json_type_to_sql_type
from jx_sqlite.meta_columns import ColumnList
from jx_sqlite.snowflake import Snowflake
from mo_dots import Data, concat_field, listwrap
import mo_json
from mo_times import Date
from pyLibrary.sql import sql_iso, sql_list
from pyLibrary.sql.sqlite import json_type_to_sqlite_type, quote_column


class Namespace(jx_base.Namespace):
    """
    MANAGE SQLITE DATABASE
    """
    def __init__(self, db):
        self.db = db
        self._snowflakes = Data()  # MAP FROM BASE TABLE TO LIST OF NESTED PATH TUPLES
        self.columns = ColumnList(db)

    def __copy__(self):
        output = object.__new__(Namespace)
        output.db = None
        output._snowflakes = deepcopy(self._snowflakes)
        output.columns = copy(self.columns)
        return output

    def remove_snowflake(self, fact_name):
        paths = self.columns._snowflakes[fact_name]
        if paths:
            with self.db.transaction() as t:
                for p in paths:
                    full_name = concat_field(fact_name, p[0])
                    t.execute("DROP TABLE "+quote_column(full_name))
            self.columns.remove_table(fact_name)
        self._snowflakes[fact_name] = None

    def create_or_replace_facts(self, fact_name, uid=UID):
        """
        MAKE NEW TABLE WITH GIVEN guid
        :param fact_name:  NAME FOR THE CENTRAL FACTS
        :param uid: name, or list of names, for the GUID
        :return: Facts
        """
        self.remove_snowflake(fact_name)
        self._snowflakes[fact_name] = ["."]

        uid = listwrap(uid)
        new_columns = []
        for u in uid:
            if u == UID:
                pass
            else:
                c = Column(
                    name=u,
                    jx_type=mo_json.STRING,
                    es_column=typed_column(u, json_type_to_sql_type[mo_json.STRING]),
                    es_type=json_type_to_sqlite_type[mo_json.STRING],
                    es_index=fact_name,
                    last_updated=Date.now()
                )
                self.add_column_to_schema(c)
                new_columns.append(c)

        command = (
            "CREATE TABLE " + quote_column(fact_name) + sql_iso(sql_list(
                [quoted_GUID + " TEXT "] +
                [quoted_UID + " INTEGER"] +
                [quote_column(c.es_column) + " " + c.es_type for c in new_columns] +
                ["PRIMARY KEY " + sql_iso(sql_list(
                    [quoted_GUID] +
                    [quoted_UID] +
                    [quote_column(c.es_column) for c in new_columns]
                ))]
            ))
        )

        with self.db.transaction() as t:
            t.execute(command)

        snowflake = Snowflake(fact_name, self)
        return Facts(self, snowflake)

    def add_column_to_schema(self, column):
        self.columns.add(column)

