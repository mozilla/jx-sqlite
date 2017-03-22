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

import re

from pyLibrary import convert
from mo_logs import Log
from mo_dots import coalesce, Data
from mo_times.dates import Date

true = True
false = False
null = None
EMPTY_DICT = {}


def compile_expression(source):
    """
    THIS FUNCTION IS ON ITS OWN FOR MINIMAL GLOBAL NAMESPACE

    :param source:  PYTHON SOURCE CODE
    :return:  PYTHON FUNCTION
    """

    # FORCE MODULES TO BE IN NAMESPACE
    _ = coalesce
    _ = Date
    _ = convert
    _ = Log
    _ = Data
    _ = EMPTY_DICT
    _ = re

    output = None
    exec """
def output(row, rownum=None, rows=None):
    try:
        return """ + source + """
    except Exception as e:
        Log.error("Problem with dynamic function {{func|quote}}",  func= """ + convert.value2quote(source) + """, cause=e)
"""
    return output
