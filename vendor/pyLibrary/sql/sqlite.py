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

import os
import re
import sys
from collections import Mapping

from mo_future import allocate_lock as _allocate_lock, text_type, zip_longest
from mo_dots import Data, coalesce
from mo_files import File
from mo_kwargs import override
from mo_logs import Log
from mo_logs.exceptions import Except, extract_stack, ERROR
from mo_logs.strings import quote
from mo_math.stats import percentile
from mo_threads import Queue, Signal, Thread
from mo_threads.signal import DONE
from mo_times import Date, Duration
from mo_times.timer import Timer

from pyLibrary import convert
from pyLibrary.sql import DB, SQL, SQL_TRUE, SQL_FALSE, SQL_NULL, SQL_SELECT, sql_iso

DEBUG = False
TRACE = True
DEBUG_EXECUTE = False
DEBUG_INSERT = False

sqlite3 = None

_load_extension_warning_sent = False
_upgraded = False


def _upgrade():
    try:
        Log.error("no upgrade")
        import sys
        import platform
        if "windows" in platform.system().lower():
            original_dll = File.new_instance(sys.exec_prefix, "dlls/sqlite3.dll")
            if platform.architecture()[0]=='32bit':
                source_dll = File("vendor/pyLibrary/vendor/sqlite/sqlite3_32.dll")
            else:
                source_dll = File("vendor/pyLibrary/vendor/sqlite/sqlite3_64.dll")

            if not all(a == b for a, b in zip_longest(source_dll.read_bytes(), original_dll.read_bytes())):
                original_dll.backup()
                File.copy(source_dll, original_dll)
        else:
            pass
    except Exception as e:
        Log.warning("could not upgrade python's sqlite", cause=e)


class Sqlite(DB):
    """
    Allows multi-threaded access
    Loads extension functions (like SQRT)
    """

    canonical = None

    @override
    def __init__(self, filename=None, db=None, upgrade=True, kwargs=None):
        """
        :param db:  Optional, wrap a sqlite db in a thread
        :return: Multithread-safe database
        """
        global _upgraded
        global sqlite3

        if not _upgraded:
            if upgrade:
                _upgrade()
            _upgraded = True
            import sqlite3
            _ = sqlite3

        self.filename = File(filename).abspath if filename else None
        self.db = db
        self.queue = Queue("sql commands")   # HOLD (command, result, signal) PAIRS
        self.worker = Thread.run("sqlite db thread", self._worker)
        self.get_trace = TRACE
        self.upgrade = upgrade
        self.closed = False

    def _enhancements(self):
        def regex(pattern, value):
            return 1 if re.match(pattern+"$", value) else 0
        con = self.db.create_function("regex", 2, regex)

        class Percentile(object):
            def __init__(self, percentile):
                self.percentile=percentile
                self.acc=[]

            def step(self, value):
                self.acc.append(value)

            def finalize(self):
                return percentile(self.acc, self.percentile)

        con.create_aggregate("percentile", 2, Percentile)

    def execute(self, command):
        """
        COMMANDS WILL BE EXECUTED IN THE ORDER THEY ARE GIVEN
        BUT CAN INTERLEAVE WITH OTHER TREAD COMMANDS
        :param command: COMMAND FOR SQLITE
        :return: Signal FOR IF YOU WANT TO BE NOTIFIED WHEN DONE
        """
        if self.closed:
            Log.error("database is closed")
        if DEBUG_EXECUTE:  # EXECUTE IMMEDIATELY FOR BETTER STACK TRACE
            self.query(command)
            return DONE

        if self.get_trace:
            trace = extract_stack(1)
        else:
            trace = None

        is_done = Signal()
        self.queue.add((command, None, is_done, trace))
        return is_done

    def commit(self):
        """
        WILL BLOCK CALLING THREAD UNTIL ALL PREVIOUS execute() CALLS ARE COMPLETED
        :return:
        """
        if self.closed:
            Log.error("database is closed")
        signal = _allocate_lock()
        signal.acquire()
        self.queue.add((COMMIT, None, signal, None))
        signal.acquire()
        return

    def query(self, command):
        """
        WILL BLOCK CALLING THREAD UNTIL THE command IS COMPLETED
        :param command: COMMAND FOR SQLITE
        :return: list OF RESULTS
        """
        if self.closed:
            Log.error("database is closed")
        if not self.worker:
            self.worker = Thread.run("sqlite db thread", self._worker)

        signal = _allocate_lock()
        signal.acquire()
        result = Data()
        self.queue.add((command, result, signal, None))
        signal.acquire()
        if result.exception:
            Log.error("Problem with Sqlite call", cause=result.exception)
        return result

    def close(self):
        """
        OPTIONAL COMMIT-AND-CLOSE
        IF THIS IS NOT DONE, THEN THE THREAD THAT SPAWNED THIS INSTANCE
        :return:
        """
        self.closed = True
        signal = _allocate_lock()
        signal.acquire()
        self.queue.add((COMMIT, None, signal, None))
        signal.acquire()
        self.worker.please_stop.go()
        return

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _worker(self, please_stop):
        global _load_extension_warning_sent

        try:
            if DEBUG:
                Log.note("Sqlite version {{version}}", version=sqlite3.sqlite_version)
            if Sqlite.canonical:
                self.db = Sqlite.canonical
            else:
                self.db = sqlite3.connect(coalesce(self.filename, ':memory:'), check_same_thread=False)

                library_loc = File.new_instance(sys.modules[__name__].__file__, "../..")
                full_path = File.new_instance(library_loc, "vendor/sqlite/libsqlitefunctions.so").abspath
                try:
                    trace = extract_stack(0)[0]
                    if self.upgrade:
                        if os.name == 'nt':
                            file = File.new_instance(trace["file"], "../../vendor/sqlite/libsqlitefunctions.so")
                        else:
                            file = File.new_instance(trace["file"], "../../vendor/sqlite/libsqlitefunctions")

                        full_path = file.abspath
                        self.db.enable_load_extension(True)
                        self.db.execute(SQL_SELECT + "load_extension" + sql_iso(self.quote_value(full_path)))
                except Exception as e:
                    if not _load_extension_warning_sent:
                        _load_extension_warning_sent = True
                        Log.warning("Could not load {{file}}}, doing without. (no SQRT for you!)", file=full_path, cause=e)

            while not please_stop:
                quad = self.queue.pop(till=please_stop)
                if quad is None:
                    break
                command, result, signal, trace = quad

                show_timing = False
                if DEBUG_INSERT and command.strip().lower().startswith("insert"):
                    Log.note("Running command\n{{command|limit(100)|indent}}", command=command)
                    show_timing = True
                if DEBUG and not command.strip().lower().startswith("insert"):
                    Log.note("Running command\n{{command|limit(100)|indent}}", command=command)
                    show_timing = True
                with Timer("SQL Timing", silent=not show_timing):
                    if command is COMMIT:
                        self.db.commit()
                        signal.release()
                    elif signal is not None:
                        try:
                            curr = self.db.execute(command)
                            if result is not None:
                                result.meta.format = "table"
                                result.header = [d[0] for d in curr.description] if curr.description else None
                                result.data = curr.fetchall()
                                if DEBUG and result.data:
                                    text = convert.table2csv(list(result.data))
                                    Log.note("Result:\n{{data}}", data=text)
                        except Exception as e:
                            e = Except.wrap(e)
                            e.cause = Except(
                                type=ERROR,
                                template="Bad call to Sqlite",
                                trace=trace
                            )
                            if result is None:
                                Log.error("Problem with\n{{command|indent}}", command=command, cause=e)
                            else:
                                result.exception = Except(ERROR, "Problem with\n{{command|indent}}", command=command, cause=e)
                        finally:
                            if isinstance(signal, Signal):
                                signal.go()
                            else:
                                signal.release()
                    else:
                        try:
                            self.db.execute(command)
                        except Exception as e:
                            e = Except.wrap(e)
                            e.cause = Except(
                                type=ERROR,
                                template="Bad call to Sqlite",
                                trace=trace
                            )
                            Log.warning("Failure to execute", cause=e)

        except Exception as e:
            if not please_stop:
                Log.warning("Problem with sql thread", cause=e)
        finally:
            self.closed = True
            if DEBUG:
                Log.note("Database is closed")
            self.db.close()

    def quote_column(self, column_name, table=None):
        return quote_column(column_name, table)

    def quote_value(self, value):
        return quote_value(value)

    def create_new_functions(self):

        def regexp(pattern, item):
            reg = re.compile(pattern)
            return reg.search(item) is not None

        self.db.create_function("REGEXP", 2, regexp)

_no_need_to_quote = re.compile(r"^\w+$", re.UNICODE)


def quote_column(column_name, table=None):
    if isinstance(column_name, SQL):
        return column_name

    if not isinstance(column_name, text_type):
        Log.error("expecting a name")
    if table != None:
        return SQL(" d" + quote(table) + "." + quote(column_name) + " ")
    else:
        if _no_need_to_quote.match(column_name):
            return SQL(" " + column_name + " ")
        return SQL(" " + quote(column_name) + " ")


def quote_value(value):
    if isinstance(value, (Mapping, list)):
        return SQL(".")
    elif isinstance(value, Date):
        return SQL(text_type(value.unix))
    elif isinstance(value, Duration):
        return SQL(text_type(value.seconds))
    elif isinstance(value, text_type):
        return SQL("'" + value.replace("'", "''") + "'")
    elif value == None:
        return SQL_NULL
    elif value is True:
        return SQL_TRUE
    elif value is False:
        return SQL_FALSE
    else:
        return SQL(text_type(value))


def join_column(a, b):
    a = quote_column(a)
    b = quote_column(b)
    return SQL(a.template.rstrip() + "." + b.template.lstrip())


COMMIT = "commit"
