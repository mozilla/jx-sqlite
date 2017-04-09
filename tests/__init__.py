# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

import itertools
import os
import signal
import subprocess
from copy import deepcopy

import mo_json_config
from mo_dots import wrap, coalesce, unwrap, listwrap, Data
from mo_kwargs import override
from mo_logs import Log, Except, constants
from mo_logs.exceptions import extract_stack
from mo_testing.fuzzytestcase import assertAlmostEqual

from jx_sqlite.query_table import QueryTable
from pyLibrary import convert
from pyLibrary.queries import jx
from pyLibrary.queries.query import QueryOp

from tests import test_jx


class SQLiteUtils(object):
    @override
    def __init__(
        self,
        kwargs=None
    ):
        self._index = None

    def setUp(self):
        self._index = QueryTable("testing")

    def tearDown(self):
        pass

    def setUpClass(self):
        pass

    def tearDownClass(self):
        pass

    def not_real_service(self):
        return True

    def execute_es_tests(self, subtest, tjson=False):
        subtest = wrap(subtest)
        subtest.name = extract_stack()[1]['method']

        if subtest.disable:
            return

        if "sqlite" in subtest["not"]:
            return

        self.fill_container(subtest, tjson=tjson)
        self.send_queries(subtest)

    def fill_container(self, subtest, tjson=False):
        """
        RETURN SETTINGS THAT CAN BE USED TO POINT TO THE INDEX THAT'S FILLED
        """
        subtest = wrap(subtest)

        try:
            # INSERT DATA
            self._index.insert(subtest.data)
        except Exception, e:
            Log.error("can not load {{data}} into container", {"data":subtest.data}, e)

        frum = subtest.query['from']
        if isinstance(frum, basestring):
            subtest.query["from"] = frum.replace(test_jx.TEST_TABLE, self._index.sf.fact)
        else:
            Log.error("Do not know how to handle")


        return Data()

    def send_queries(self, subtest):
        subtest = wrap(subtest)

        try:
            # EXECUTE QUERY
            num_expectations = 0
            for k, v in subtest.items():
                if k.startswith("expecting_"):  # WHAT FORMAT ARE WE REQUESTING
                    format = k[len("expecting_"):]
                elif k == "expecting":  # NO FORMAT REQUESTED (TO TEST DEFAULT FORMATS)
                    format = None
                else:
                    continue

                num_expectations += 1
                expected = v

                subtest.query.format = format
                subtest.query.meta.testing = True  # MARK ALL QUERIES FOR TESTING SO FULL METADATA IS AVAILABLE BEFORE QUERY EXECUTION
                result = self.execute_query(subtest.query)

                compare_to_expected(subtest.query, result, expected)
            if num_expectations == 0:
                Log.error("Expecting test {{name|quote}} to have property named 'expecting_*' for testing the various format clauses", {
                    "name": subtest.name
                })
        except Exception, e:
            Log.error("Failed test {{name|quote}}", {"name": subtest.name}, e)

    def execute_query(self, query):
        try:
            return self._index.query(deepcopy(query))
        except Exception, e:
            Log.error("Failed query", e)

    def try_till_response(self, *args, **kwargs):
        self.execute_query(convert.json2value(convert.utf82unicode(kwargs["data"])))


def compare_to_expected(query, result, expect):
    query = wrap(query)
    expect = wrap(expect)

    if result.meta.format == "table":
        assertAlmostEqual(set(result.header), set(expect.header))

        # MAP FROM expected COLUMN TO result COLUMN
        mapping = zip(*zip(*filter(
            lambda v: v[0][1] == v[1][1],
            itertools.product(enumerate(expect.header), enumerate(result.header))
        ))[1])[0]
        result.header = [result.header[m] for m in mapping]

        if result.data:
            columns = zip(*unwrap(result.data))
            result.data = zip(*[columns[m] for m in mapping])

        if not query.sort:
            sort_table(result)
            sort_table(expect)
    elif result.meta.format == "list":
        if query["from"].startswith("meta."):
            pass
        else:
            query = QueryOp.wrap(query)

        if not query.sort:
            try:
                #result.data MAY BE A LIST OF VALUES, NOT OBJECTS
                data_columns = jx.sort(set(jx.get_columns(result.data, leaves=True)) | set(jx.get_columns(expect.data, leaves=True)), "name")
            except Exception:
                data_columns = [{"name":"."}]

            sort_order = listwrap(coalesce(query.edges, query.groupby)) + data_columns

            if isinstance(expect.data, list):
                try:
                    expect.data = jx.sort(expect.data, sort_order.name)
                except Exception, _:
                    pass

            if isinstance(result.data, list):
                try:
                    result.data = jx.sort(result.data, sort_order.name)
                except Exception, _:
                    pass

    elif result.meta.format == "cube" and len(result.edges) == 1 and result.edges[0].name == "rownum" and not query.sort:
        result_data, result_header = cube2list(result.data)
        result_data = unwrap(jx.sort(result_data, result_header))
        result.data = list2cube(result_data, result_header)

        expect_data, expect_header = cube2list(expect.data)
        expect_data = jx.sort(expect_data, expect_header)
        expect.data = list2cube(expect_data, expect_header)

    # CONFIRM MATCH
    assertAlmostEqual(result, expect, places=6)


def cube2list(cube):
    """
    RETURNS header SO THAT THE ORIGINAL CUBE CAN BE RECREATED
    :param cube: A dict WITH VALUES BEING A MULTIDIMENSIONAL ARRAY OF UNIFORM VALUES
    :return: (rows, header) TUPLE
    """
    header = list(unwrap(cube).keys())
    rows = []
    for r in zip(*[[(k, v) for v in a] for k, a in cube.items()]):
        row = Data()
        for k, v in r:
            row[k]=v
        rows.append(unwrap(row))
    return rows, header


def list2cube(rows, header):
    output = {h: [] for h in header}
    for r in rows:
        for h in header:
            if h==".":
                output[h].append(r)
            else:
                r = wrap(r)
                output[h].append(r[h])
    return output


def sort_table(result):
    """
    SORT ROWS IN TABLE, EVEN IF ELEMENTS ARE JSON
    """
    data = wrap([{unicode(i): v for i, v in enumerate(row)} for row in result.data])
    sort_columns = jx.sort(set(jx.get_columns(data, leaves=True).name))
    data = jx.sort(data, sort_columns)
    result.data = [tuple(row[unicode(i)] for i in range(len(result.header))) for row in data]


def error(response):
    response = convert.utf82unicode(response.content)

    try:
        e = Except.new_instance(convert.json2value(response))
    except Exception:
        e = None

    if e:
        Log.error("Failed request", e)
    else:
        Log.error("Failed request\n {{response}}", {"response": response})


def run_app(please_stop, server_is_ready):
    proc = subprocess.Popen(
        ["python", "active_data\\app.py", "--settings", "tests/config/elasticsearch.json"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1
        #creationflags=CREATE_NEW_PROCESS_GROUP
    )

    while not please_stop:
        line = proc.stdout.readline()
        if not line:
            continue
        if line.find(" * Running on") >= 0:
            server_is_ready.go()
        Log.note("SERVER: {{line}}", {"line": line.strip()})

    proc.send_signal(signal.CTRL_C_EVENT)


# read_alternate_settings
try:
    filename = os.environ.get("TEST_CONFIG")
    if filename:
        test_jx.global_settings = mo_json_config.get("file://"+filename)
        constants.set(test_jx.global_settings.constants)
    else:
        Log.alert("No TEST_CONFIG environment variable to point to config file.  Using ./tests/config/sqlite.json")
        test_jx.global_settings = mo_json_config.get("file://tests/config/sqlite.json")
        constants.set(test_jx.global_settings.constants)

    if not test_jx.global_settings.use:
        Log.error('Must have a {"use": type} set in the config file')

    Log.start(test_jx.global_settings.debug)
    test_jx.utils = SQLiteUtils(test_jx.global_settings)
except Exception, e:
    Log.warning("problem", e)

