# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#

import re

from mo_logs import Log

keyword_pattern = re.compile(r"(\$|\w|\\\.)+(?:\.(\$|\w|\\\.)+)*")



def is_variable_name(value):
    if value.__class__.__name__ == "Variable":
        Log.warning("not expected")
        return True

    if not value or not isinstance(value, basestring):
        return False  # _a._b
    if value == ".":
        return True
    match = keyword_pattern.match(value)
    if not match:
        return False
    return match.group(0) == value
