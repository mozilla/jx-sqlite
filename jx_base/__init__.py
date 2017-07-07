# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with self file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from uuid import uuid4

def generateGuid():
    """Gets a random GUID.
    Note: python's UUID generation library is used here. 
    Basically UUID is the same as GUID when represented as a string.
    :Returns:
        str, the generated random GUID.
        
    a=GenerateGuid()
    import uuid
    print a
    print uuid.UUID(a).hex

    """
    return str(uuid4())
