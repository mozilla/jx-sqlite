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

from collections import Mapping
from uuid import uuid4

from mo_dots import NullType, Data, FlatList, wrap, coalesce, listwrap
from mo_future import text_type, none_type, PY2, long
from mo_json import value2json
from mo_logs import Log
from mo_logs.strings import quote, expand_template
from mo_times import Date

IS_NULL = '0'
BOOLEAN = 'boolean'
INTEGER = 'integer'
NUMBER = 'number'
STRING = 'string'
OBJECT = 'object'
NESTED = "nested"
EXISTS = "exists"

JSON_TYPES = [BOOLEAN, INTEGER, NUMBER, STRING, OBJECT]
PRIMITIVE = [BOOLEAN, INTEGER, NUMBER, STRING]
STRUCT = [EXISTS, OBJECT, NESTED]


python_type_to_json_type = {
    int: INTEGER,
    text_type: STRING,
    float: NUMBER,
    None: OBJECT,
    bool: BOOLEAN,
    NullType: OBJECT,
    none_type: OBJECT,
    Data: OBJECT,
    dict: OBJECT,
    object: OBJECT,
    Mapping: OBJECT,
    list: NESTED,
    FlatList: NESTED,
    Date: NUMBER
}

if PY2:
    python_type_to_json_type[str] = STRING
    python_type_to_json_type[long] = NUMBER


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
    return text_type(uuid4())


def _exec(code, name):
    try:
        globs = globals()
        fake_locals = {}
        exec(code, globs, fake_locals)
        temp = globs[name] = fake_locals[name]
        return temp
    except Exception as e:
        Log.error("Can not make class\n{{code}}", code=code, cause=e)


_ = listwrap


def DataClass(name, columns, constraint=None):
    """
    Use the DataClass to define a class, but with some extra features:
    1. restrict the datatype of property
    2. restrict if `required`, or if `nulls` are allowed
    3. generic constraints on object properties

    It is expected that this class become a real class (or be removed) in the
    long term because it is expensive to use and should only be good for
    verifying program correctness, not user input.

    :param name: Name of the class we are creating
    :param columns: Each columns[i] has properties {
            "name",     - (required) name of the property
            "required", - False if it must be defined (even if None)
            "nulls",    - True if property can be None, or missing
            "default",  - A default value, if none is provided
            "type"      - a Python datatype
        }
    :param constraint: a JSON query Expression for extra constraints (return true if all constraints are met)
    :return: The class that has been created
    """

    from jx_python.expressions import jx_expression

    columns = wrap([{"name": c, "required": True, "nulls": False, "type": object} if isinstance(c, text_type) else c for c in columns])
    slots = columns.name
    required = wrap(filter(lambda c: c.required and not c.nulls and not c.default, columns)).name
    nulls = wrap(filter(lambda c: c.nulls, columns)).name
    defaults = {c.name: coalesce(c.default, None) for c in columns}
    types = {c.name: coalesce(c.type, object) for c in columns}

    code = expand_template(
"""
from __future__ import unicode_literals
from collections import Mapping

meta = None
types_ = {{types}}
defaults_ = {{defaults}}

class {{class_name}}(Mapping):
    __slots__ = {{slots}}


    def _constraint(row, rownum, rows):
        try:
            return {{constraint_expr}}
        except Exception as e:
            return False

    def __init__(self, **kwargs):
        if not kwargs:
            return

        for s in {{slots}}:
            object.__setattr__(self, s, kwargs.get(s, {{defaults}}.get(s, None)))

        missed = {{required}}-set(kwargs.keys())
        if missed:
            Log.error("Expecting properties {"+"{missed}}", missed=missed)

        illegal = set(kwargs.keys())-set({{slots}})
        if illegal:
            Log.error("{"+"{names}} are not a valid properties", names=illegal)

        if not self._constraint(0, [self]):
            Log.error("constraint not satisfied {"+"{expect}}\\n{"+"{value|indent}}", expect={{constraint}}, value=self)

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item, value):
        setattr(self, item, value)
        return self

    def __setattr__(self, item, value):
        if item not in {{slots}}:
            Log.error("{"+"{item|quote}} not valid attribute", item=item)
        object.__setattr__(self, item, value)
        if not self._constraint(0, [self]):
            Log.error("constraint not satisfied {"+"{expect}}\\n{"+"{value|indent}}", expect={{constraint}}, value=self)

    def __getattr__(self, item):
        Log.error("{"+"{item|quote}} not valid attribute", item=item)

    def __hash__(self):
        return object.__hash__(self)

    def __eq__(self, other):
        if isinstance(other, {{class_name}}) and dict(self)==dict(other) and self is not other:
            Log.error("expecting to be same object")
        return self is other

    def __dict__(self):
        return {k: getattr(self, k) for k in {{slots}}}

    def items(self):
        return ((k, getattr(self, k)) for k in {{slots}})

    def __copy__(self):
        _set = object.__setattr__
        output = object.__new__({{class_name}})
        {{assign}}
        return output

    def __iter__(self):
        return {{slots}}.__iter__()

    def __len__(self):
        return {{len_slots}}

    def __str__(self):
        return str({{dict}})

""",
        {
            "class_name": name,
            "slots": "(" + (", ".join(quote(s) for s in slots)) + ")",
            "required": "{" + (", ".join(quote(s) for s in required)) + "}",
            "nulls": "{" + (", ".join(quote(s) for s in nulls)) + "}",
            "defaults": jx_expression({"literal": defaults}).to_python(),
            "len_slots": len(slots),
            "dict": "{" + (", ".join(quote(s) + ": self." + s for s in slots)) + "}",
            "assign": "; ".join("_set(output, "+quote(s)+", self."+s+")" for s in slots),
            "types": "{" + (",".join(quote(k) + ": " + v.__name__ for k, v in types.items())) + "}",
            "constraint_expr": jx_expression(constraint).to_python(),
            "constraint": value2json(constraint)
        }
    )

    return _exec(code, name)


class Table(DataClass(
    "Table",
    [
        "name",
        "url",
        "query_path",
        "timestamp"
    ],
    constraint={"and": [
        {"eq": [{"last": "query_path"}, {"literal": "."}]}
    ]}
)):
    @property
    def columns(self):
        Log.error("not implemented")
        # return singlton.get_columns(table_name=self.name)


Column = DataClass(
    "Column",
    [
        # "table",
        "names",  # MAP FROM TABLE NAME TO COLUMN NAME (ONE COLUMN CAN HAVE MULTIPLE NAMES)
        "es_column",
        "es_index",
        # "es_type",
        "type",
        {"name": "useSource", "default": False},
        {"name": "nested_path", "nulls": True},  # AN ARRAY OF PATHS (FROM DEEPEST TO SHALLOWEST) INDICATING THE JSON SUB-ARRAYS
        {"name": "count", "nulls": True},
        {"name": "cardinality", "nulls": True},
        {"name": "multi", "nulls": True},
        {"name": "partitions", "nulls": True},
        {"name": "last_updated", "nulls": True}
    ],
    constraint={"and": [
        {"eq": [{"last": "nested_path"}, {"literal": "."}]}
    ]}
)

