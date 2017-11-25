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

import json
import time
from collections import Mapping
from datetime import datetime, date, timedelta
from decimal import Decimal

from mo_future import text_type, binary_type

from jx_base import python_type_to_json_type, INTEGER, NUMBER
from mo_dots import Data, FlatList, NullType, unwrap
from mo_json import ESCAPE_DCT, float2json, json2value
from mo_json.encoder import pretty_json, problem_serializing, UnicodeBuilder, COMMA, COLON
from mo_json.typed_encoder import untype_path, encode_property, BOOLEAN_TYPE, NESTED_TYPE, EXISTS_TYPE, STRING_TYPE, NUMBER_TYPE, TYPE_PREFIX
from mo_logs import Log
from mo_logs.strings import utf82unicode
from mo_times.dates import Date
from mo_times.durations import Duration
from pyLibrary.env.elasticsearch import parse_properties, random_id

append = UnicodeBuilder.append


class TypedInserter(object):
    def __init__(self, es, id_column="_id"):
        columns = parse_properties(es.settings.alias, ".", es.get_properties()).columns
        _schema = Data()
        for c in columns:
            untyped_path = untype_path(c.names["."])
            type = c.type
            _schema[untyped_path][TYPE_PREFIX + type] = c
        self.schema = unwrap(_schema)
        self.es = es
        self.id_column = id_column
        self.remove_id = True if id_column == "_id" else False

    def typed_encode(self, r):
        """
        :param record:  expecting id and value properties
        :return:  dict with id and json properties
        """
        try:
            value = r['value']
            if "json" in r:
                value = json2value(r["json"])
            elif isinstance(value, Mapping) or value != None:
                pass
            else:
                from mo_logs import Log
                raise Log.error("Expecting every record given to have \"value\" or \"json\" property")

            _buffer = UnicodeBuilder(1024)
            net_new_properties = []
            path = []
            if isinstance(value, Mapping):
                given_id = value.get(self.id_column)
                if self.remove_id:
                    value[self.id_column] = None
            else:
                given_id = None

            if given_id:
                record_id = r.get('id')
                if record_id and record_id != given_id:
                    from mo_logs import Log

                    raise Log.error(
                        "expecting {{property}} of record ({{record_id|quote}}) to match one given ({{given|quote}})",
                        property=self.id_column,
                        record_id=record_id,
                        given=given_id
                    )
            else:
                record_id = r.get('id')
                if record_id:
                    given_id = record_id
                else:
                    given_id = random_id()

            self._typed_encode(value, self.schema, path, net_new_properties, _buffer)
            json = _buffer.build()

            for props in net_new_properties:
                path, type = props[:-1], props[-1][1:]
                # self.es.add_column(join_field(path), type)

            return {"id": given_id, "json": json}
        except Exception as e:
            # THE PRETTY JSON WILL PROVIDE MORE DETAIL ABOUT THE SERIALIZATION CONCERNS
            from mo_logs import Log

            Log.warning("Serialization of JSON problems", e)
            try:
                return pretty_json(r)
            except Exception as f:
                Log.error("problem serializing object", f)

    def _typed_encode(self, value, sub_schema, path, net_new_properties, _buffer):
        try:
            if value is None:
                append(_buffer, u'{}')
                return
            elif value is True:
                if BOOLEAN_TYPE not in sub_schema:
                    sub_schema[BOOLEAN_TYPE] = {}
                    net_new_properties.append(path+[BOOLEAN_TYPE])
                append(_buffer, u'{"'+BOOLEAN_TYPE+'": true}')
                return
            elif value is False:
                if BOOLEAN_TYPE not in sub_schema:
                    sub_schema[BOOLEAN_TYPE] = {}
                    net_new_properties.append(path+[BOOLEAN_TYPE])
                append(_buffer, u'{"'+BOOLEAN_TYPE+'": false}')
                return

            _type = value.__class__
            if _type in (dict, Data):
                if NESTED_TYPE in sub_schema:
                    # PREFER NESTED, WHEN SEEN BEFORE
                    if value:
                        append(_buffer, u'{"'+NESTED_TYPE+'": [')
                        self._dict2json(value, sub_schema[NESTED_TYPE], path + [NESTED_TYPE], net_new_properties, _buffer)
                        append(_buffer, '], "' + EXISTS_TYPE + '":' + text_type(len(value)) + '}')
                    else:
                        # SINGLETON LISTS OF null SHOULD NOT EXIST
                        pass
                else:
                    if EXISTS_TYPE not in sub_schema:
                        sub_schema[EXISTS_TYPE] = {}
                        net_new_properties.append(path+[EXISTS_TYPE])

                    if value:
                        self._dict2json(value, sub_schema, path, net_new_properties, _buffer)
                    else:
                        append(_buffer, u'{"'+EXISTS_TYPE+'": 0}')
            elif _type is binary_type:
                if STRING_TYPE not in sub_schema:
                    sub_schema[STRING_TYPE] = True
                    net_new_properties.append(path + [STRING_TYPE])
                append(_buffer, u'{"'+STRING_TYPE+'": "')
                try:
                    v = utf82unicode(value)
                except Exception as e:
                    raise problem_serializing(value, e)

                for c in v:
                    append(_buffer, ESCAPE_DCT.get(c, c))
                append(_buffer, u'"}')
            elif _type is text_type:
                if STRING_TYPE not in sub_schema:
                    sub_schema[STRING_TYPE] = True
                    net_new_properties.append(path + [STRING_TYPE])

                append(_buffer, u'{"'+STRING_TYPE+'": "')
                for c in value:
                    append(_buffer, ESCAPE_DCT.get(c, c))
                append(_buffer, u'"}')
            elif _type in (int, long, Decimal):
                if NUMBER_TYPE not in sub_schema:
                    sub_schema[NUMBER_TYPE] = True
                    net_new_properties.append(path + [NUMBER_TYPE])

                append(_buffer, u'{"'+NUMBER_TYPE+'": ')
                append(_buffer, float2json(value))
                append(_buffer, u'}')
            elif _type is float:
                if NUMBER_TYPE not in sub_schema:
                    sub_schema[NUMBER_TYPE] = True
                    net_new_properties.append(path + [NUMBER_TYPE])
                append(_buffer, u'{"'+NUMBER_TYPE+'": ')
                append(_buffer, float2json(value))
                append(_buffer, u'}')
            elif _type in (set, list, tuple, FlatList):
                if len(value) == 0:
                    append(_buffer, u'{"'+NESTED_TYPE+'": []}')
                elif any(isinstance(v, (Mapping, set, list, tuple, FlatList)) for v in value):
                    if NESTED_TYPE not in sub_schema:
                        sub_schema[NESTED_TYPE] = {}
                        net_new_properties.append(path + [NESTED_TYPE])
                    append(_buffer, u'{"'+NESTED_TYPE+'": ')
                    self._list2json(value, sub_schema[NESTED_TYPE], path+[NESTED_TYPE], net_new_properties, _buffer)
                    append(_buffer, u'}')
                else:
                    # ALLOW PRIMITIVE MULTIVALUES
                    types = list(set(python_type_to_json_type[v.__class__] for v in value))
                    if len(types) > 1:
                        from mo_logs import Log
                        Log.error("Can not handle multi-typed multivalues")
                    if types[0] == INTEGER:
                        # TODO: MAYBE ALLOW INTEGER TYPES?
                        element_type = TYPE_PREFIX + NUMBER
                    else:
                        element_type = TYPE_PREFIX + types[0]
                    if element_type not in sub_schema:
                        sub_schema[element_type] = True
                        net_new_properties.append(path + [element_type])
                    append(_buffer, u'{"'+element_type+u'": ')
                    self._multivalue2json(value, sub_schema[element_type], path+[element_type], net_new_properties, _buffer)
                    append(_buffer, u'}')
            elif _type is date:
                if NUMBER_TYPE not in sub_schema:
                    sub_schema[NUMBER_TYPE] = True
                    net_new_properties.append(path + [NUMBER_TYPE])
                append(_buffer, u'{"'+NUMBER_TYPE+'": ')
                append(_buffer, float2json(time.mktime(value.timetuple())))
                append(_buffer, u'}')
            elif _type is datetime:
                if NUMBER_TYPE not in sub_schema:
                    sub_schema[NUMBER_TYPE] = True
                    net_new_properties.append(path + [NUMBER_TYPE])
                append(_buffer, u'{"'+NUMBER_TYPE+'": ')
                append(_buffer, float2json(time.mktime(value.timetuple())))
                append(_buffer, u'}')
            elif _type is Date:
                if NUMBER_TYPE not in sub_schema:
                    sub_schema[NUMBER_TYPE] = True
                    net_new_properties.append(path + [NUMBER_TYPE])
                append(_buffer, u'{"'+NUMBER_TYPE+'": ')
                append(_buffer, float2json(value.unix))
                append(_buffer, u'}')
            elif _type is timedelta:
                if NUMBER_TYPE not in sub_schema:
                    sub_schema[NUMBER_TYPE] = True
                    net_new_properties.append(path + [NUMBER_TYPE])
                append(_buffer, u'{"'+NUMBER_TYPE+'": ')
                append(_buffer, float2json(value.total_seconds()))
                append(_buffer, u'}')
            elif _type is Duration:
                if NUMBER_TYPE not in sub_schema:
                    sub_schema[NUMBER_TYPE] = True
                    net_new_properties.append(path + [NUMBER_TYPE])
                append(_buffer, u'{"'+NUMBER_TYPE+'": ')
                append(_buffer, float2json(value.seconds))
                append(_buffer, u'}')
            elif _type is NullType:
                append(_buffer, u"null")
            elif hasattr(value, '__json__'):
                from mo_logs import Log
                Log.error("do not know how to handle")
            elif hasattr(value, '__iter__'):
                if NESTED_TYPE not in sub_schema:
                    sub_schema[NESTED_TYPE] = {}
                    net_new_properties.append(path + [NESTED_TYPE])

                append(_buffer, u'{"'+NESTED_TYPE+'": ')
                self._iter2json(value, sub_schema[NESTED_TYPE], path+[NESTED_TYPE], net_new_properties, _buffer)
                append(_buffer, u'}')
            else:
                from mo_logs import Log

                Log.error(repr(value) + " is not JSON serializable")
        except Exception as e:
            from mo_logs import Log

            Log.error(repr(value) + " is not JSON serializable", cause=e)

    def _list2json(self, value, sub_schema, path, net_new_properties, _buffer):
        if not value:
            append(_buffer, u"[]")
        else:
            sep = u"["
            for v in value:
                append(_buffer, sep)
                sep = COMMA
                self._typed_encode(v, sub_schema, path, net_new_properties, _buffer)
            append(_buffer, u'], "'+EXISTS_TYPE+'":'+text_type(len(value)))

    def _multivalue2json(self, value, sub_schema, path, net_new_properties, _buffer):
        if not value:
            append(_buffer, u"[]")
        else:
            sep = u"["
            for v in value:
                append(_buffer, sep)
                sep = COMMA
                append(_buffer, json_encoder(v))
            append(_buffer, u"]")

    def _iter2json(self, value, sub_schema, path, net_new_properties, _buffer):
        append(_buffer, u"[")
        sep = u""
        count = 0
        for v in value:
            append(_buffer, sep)
            sep = COMMA
            self._typed_encode(v, sub_schema, path, net_new_properties, _buffer)
            count += 1
        append(_buffer, u'], "'+EXISTS_TYPE+'":' + text_type(count))

    def _dict2json(self, value, sub_schema, path, net_new_properties, _buffer):
        prefix = u'{'
        for k, v in value.iteritems():
            if v == None or v == "":
                continue
            append(_buffer, prefix)
            prefix = u", "
            if isinstance(k, binary_type):
                k = utf82unicode(k)
            if not isinstance(k, text_type):
                Log.error("Expecting property name to be a string")
            if k not in sub_schema:
                sub_schema[k] = {}
                net_new_properties.append(path+[k])
            append(_buffer, json.dumps(encode_property(k)))
            append(_buffer, u": ")
            self._typed_encode(v, sub_schema[k], path+[k], net_new_properties, _buffer)
        if prefix == u", ":
            append(_buffer, u', "'+EXISTS_TYPE+'": 1}')
        else:
            append(_buffer, u'{"'+EXISTS_TYPE+'": 0}')


json_encoder = json.JSONEncoder(
    skipkeys=False,
    ensure_ascii=False,  # DIFF FROM DEFAULTS
    check_circular=True,
    allow_nan=True,
    indent=None,
    separators=(COMMA, COLON),
    encoding='utf8',
    default=None,
    sort_keys=True
).encode
