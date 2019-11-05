# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import os
import re
import sys
from collections import Mapping, namedtuple

from mo_dots import Data, coalesce, unwraplist, listwrap, wrap
from mo_files import File
from mo_future import allocate_lock as _allocate_lock, text_type, first
from mo_future import is_text
from mo_future import zip_longest
from mo_json import BOOLEAN, INTEGER, NESTED, NUMBER, OBJECT, STRING
from mo_kwargs import override
from mo_logs import Log
from mo_logs.exceptions import ERROR, Except, extract_stack, format_trace
from mo_logs.strings import quote
from mo_math.stats import percentile
from mo_threads import Lock, Queue, Thread, Till
from mo_times import Date, Duration, Timer
from pyLibrary import convert
from pyLibrary.sql import (
    DB,
    SQL,
    SQL_FALSE,
    SQL_NULL,
    SQL_SELECT,
    SQL_TRUE,
    sql_iso,
    sql_list,
    SQL_AND,
    ConcatSQL,
    SQL_EQ,
    SQL_IS_NULL,
    SQL_COMMA,
    JoinSQL,
    SQL_FROM,
    SQL_WHERE,
    SQL_ORDERBY,
    SQL_STAR,
    SQL_CREATE,
    SQL_VALUES,
    SQL_INSERT,
    SQL_OP,
    SQL_CP,
    SQL_DOT,
    SQL_LT, SQL_SPACE, SQL_AS)

DEBUG = True
TRACE = True

FORMAT_COMMAND = "Running command\n{{command|limit(1000)|indent}}"
DOUBLE_TRANSACTION_ERROR = (
    "You can not query outside a transaction you have open already"
)
TOO_LONG_TO_HOLD_TRANSACTION = 10

_sqlite3 = None
_load_extension_warning_sent = False
_upgraded = False
known_databases = {None: None}


def _upgrade():
    try:
        Log.note("sqlite not upgraded")
        # return
        #
        # import sys
        # import platform
        # if "windows" in platform.system().lower():
        #     original_dll = File.new_instance(sys.exec_prefix, "dlls/sqlite3.dll")
        #     if platform.architecture()[0]=='32bit':
        #         source_dll = File("vendor/pyLibrary/vendor/sqlite/sqlite3_32.dll")
        #     else:
        #         source_dll = File("vendor/pyLibrary/vendor/sqlite/sqlite3_64.dll")
        #
        #     if not all(a == b for a, b in zip_longest(source_dll.read_bytes(), original_dll.read_bytes())):
        #         original_dll.backup()
        #         File.copy(source_dll, original_dll)
        # else:
        #     pass
    except Exception as e:
        Log.warning("could not upgrade python's sqlite", cause=e)


class Sqlite(DB):
    """
    Allows multi-threaded access
    Loads extension functions (like SQRT)
    """

    @override
    def __init__(
        self,
        filename=None,
        db=None,
        get_trace=None,
        upgrade=True,
        load_functions=False,
        debug=False,
        kwargs=None,
    ):
        """
        :param filename:  FILE TO USE FOR DATABASE
        :param db: AN EXISTING sqlite3 DB YOU WOULD LIKE TO USE (INSTEAD OF USING filename)
        :param get_trace: GET THE STACK TRACE AND THREAD FOR EVERY DB COMMAND (GOOD FOR DEBUGGING)
        :param upgrade: REPLACE PYTHON sqlite3 DLL WITH MORE RECENT ONE, WITH MORE FUNCTIONS (NOT WORKING)
        :param load_functions: LOAD EXTENDED MATH FUNCTIONS (MAY REQUIRE upgrade)
        :param kwargs:
        """
        global _upgraded
        global _sqlite3

        self.settings = kwargs
        if not _upgraded:
            if upgrade:
                _upgrade()
            _upgraded = True
            import sqlite3 as _sqlite3

            _ = _sqlite3

        self.filename = File(filename).abspath if filename else None
        if known_databases.get(self.filename):
            Log.error(
                "Not allowed to create more than one Sqlite instance for {{file}}",
                file=self.filename,
            )
        self.debug = debug | DEBUG

        # SETUP DATABASE
        self.debug and Log.note(
            "Sqlite version {{version}}", version=_sqlite3.sqlite_version
        )
        try:
            if db == None:
                self.db = _sqlite3.connect(
                    database=coalesce(self.filename, ":memory:"),
                    check_same_thread=False,
                    isolation_level=None,
                )
            else:
                self.db = db
        except Exception as e:
            Log.error(
                "could not open file {{filename}}", filename=self.filename, cause=e
            )
        self.upgrade = upgrade
        load_functions and self._load_functions()

        self.locker = Lock()
        self.available_transactions = []  # LIST OF ALL THE TRANSACTIONS BEING MANAGED
        self.queue = Queue(
            "sql commands"
        )  # HOLD (command, result, signal, stacktrace) TUPLES

        self.get_trace = coalesce(get_trace, TRACE)
        self.closed = False

        # WORKER VARIABLES
        self.transaction_stack = []  # THE TRANSACTION OBJECT WE HAVE PARTIALLY RUN
        self.last_command_item = (
            None
        )  # USE THIS TO HELP BLAME current_transaction FOR HANGING ON TOO LONG
        self.too_long = None
        self.delayed_queries = []
        self.delayed_transactions = []
        self.worker = Thread.run("sqlite db thread", self._worker)

        self.debug and Log.note(
            "Sqlite version {{version}}",
            version=self.query("select sqlite_version()").data[0][0],
        )

    def _enhancements(self):
        def regex(pattern, value):
            return 1 if re.match(pattern + "$", value) else 0

        con = self.db.create_function("regex", 2, regex)

        class Percentile(object):
            def __init__(self, percentile):
                self.percentile = percentile
                self.acc = []

            def step(self, value):
                self.acc.append(value)

            def finalize(self):
                return percentile(self.acc, self.percentile)

        con.create_aggregate("percentile", 2, Percentile)

    def transaction(self):
        thread = Thread.current()
        parent = None
        with self.locker:
            for t in self.available_transactions:
                if t.thread is thread:
                    parent = t

        output = Transaction(self, parent=parent)
        self.available_transactions.append(output)
        return output

    def about(self, table_name):
        """
        :param table_name: TABLE IF INTEREST
        :return: SOME INFORMATION ABOUT THE TABLE
            (cid, name, dtype, notnull, dfft_value, pk) tuples
        """
        details = self.query("PRAGMA table_info" + sql_iso(quote_column(table_name)))
        return details.data

    def query(self, command):
        """
        WILL BLOCK CALLING THREAD UNTIL THE command IS COMPLETED
        :param command: COMMAND FOR SQLITE
        :return: list OF RESULTS
        """
        if self.closed:
            Log.error("database is closed")

        signal = _allocate_lock()
        signal.acquire()
        result = Data()
        trace = extract_stack(1) if self.get_trace else None

        if self.get_trace:
            current_thread = Thread.current()
            with self.locker:
                for t in self.available_transactions:
                    if t.thread is current_thread:
                        Log.error(DOUBLE_TRANSACTION_ERROR)

        self.queue.add(CommandItem(command, result, signal, trace, None))
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
        self.queue.add(CommandItem(COMMIT, None, signal, None, None))
        signal.acquire()
        self.worker.please_stop.go()
        return

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _load_functions(self):
        global _load_extension_warning_sent
        library_loc = File.new_instance(sys.modules[__name__].__file__, "../..")
        full_path = File.new_instance(
            library_loc, "vendor/sqlite/libsqlitefunctions.so"
        ).abspath
        try:
            trace = extract_stack(0)[0]
            if self.upgrade:
                if os.name == "nt":
                    file = File.new_instance(
                        trace["file"], "../../vendor/sqlite/libsqlitefunctions.so"
                    )
                else:
                    file = File.new_instance(
                        trace["file"], "../../vendor/sqlite/libsqlitefunctions"
                    )

                full_path = file.abspath
                self.db.enable_load_extension(True)
                self.db.execute(text_type(
                    SQL_SELECT + "load_extension" + sql_iso(quote_value(full_path))
                ))
        except Exception as e:
            if not _load_extension_warning_sent:
                _load_extension_warning_sent = True
                Log.warning(
                    "Could not load {{file}}, doing without. (no SQRT for you!)",
                    file=full_path,
                    cause=e,
                )

    def create_new_functions(self):
        def regexp(pattern, item):
            reg = re.compile(pattern)
            return reg.search(item) is not None

        self.db.create_function("REGEXP", 2, regexp)

    def show_transactions_blocked_warning(self):
        blocker = self.last_command_item
        blocked = (self.delayed_queries + self.delayed_transactions)[0]

        Log.warning(
            "Query on thread {{blocked_thread|json}} at\n"
            "{{blocked_trace|indent}}"
            "is blocked by {{blocker_thread|json}} at\n"
            "{{blocker_trace|indent}}"
            "this message brought to you by....",
            blocker_trace=format_trace(blocker.trace),
            blocked_trace=format_trace(blocked.trace),
            blocker_thread=blocker.transaction.thread.name
            if blocker.transaction is not None
            else None,
            blocked_thread=blocked.transaction.thread.name
            if blocked.transaction is not None
            else None,
        )

    def _close_transaction(self, command_item):
        query, result, signal, trace, transaction = command_item

        transaction.end_of_life = True
        with self.locker:
            self.available_transactions.remove(transaction)
            assert transaction not in self.available_transactions

            old_length = len(self.transaction_stack)
            old_trans = self.transaction_stack[-1]
            del self.transaction_stack[-1]

            assert old_length - 1 == len(self.transaction_stack)
            assert old_trans
            assert old_trans not in self.transaction_stack
        if not self.transaction_stack:
            # NESTED TRANSACTIONS NOT ALLOWED IN sqlite3
            self.debug and Log.note(FORMAT_COMMAND, command=query)
            self.db.execute(query)

        has_been_too_long = False
        with self.locker:
            if self.too_long is not None:
                self.too_long, too_long = None, self.too_long
                # WE ARE CHEATING HERE: WE REACH INTO THE Signal MEMBERS AND REMOVE WHAT WE ADDED TO THE INTERNAL job_queue
                with too_long.lock:
                    has_been_too_long = bool(too_long)
                    too_long.job_queue = None

            # PUT delayed BACK ON THE QUEUE, IN THE ORDER FOUND, BUT WITH QUERIES FIRST
            if self.delayed_transactions:
                for c in reversed(self.delayed_transactions):
                    self.queue.push(c)
                del self.delayed_transactions[:]
            if self.delayed_queries:
                for c in reversed(self.delayed_queries):
                    self.queue.push(c)
                del self.delayed_queries[:]
        if has_been_too_long:
            Log.note("Transaction blockage cleared")

    def _worker(self, please_stop):
        try:
            # MAIN EXECUTION LOOP
            while not please_stop:
                command_item = self.queue.pop(till=please_stop)
                if command_item is None:
                    break
                try:
                    self._process_command_item(command_item)
                except Exception as e:
                    Log.warning("worker can not execute command", cause=e)
        except Exception as e:
            e = Except.wrap(e)
            if not please_stop:
                Log.warning("Problem with sql", cause=e)
        finally:
            self.closed = True
            self.debug and Log.note("Database is closed")
            self.db.close()

    def _process_command_item(self, command_item):
        query, result, signal, trace, transaction = command_item

        with Timer("SQL Timing", silent=not self.debug):
            if transaction is None:
                # THIS IS A TRANSACTIONLESS QUERY, DELAY IT IF THERE IS A CURRENT TRANSACTION
                if self.transaction_stack:
                    with self.locker:
                        if self.too_long is None:
                            self.too_long = Till(seconds=TOO_LONG_TO_HOLD_TRANSACTION)
                            self.too_long.then(self.show_transactions_blocked_warning)
                        self.delayed_queries.append(command_item)
                    return
            elif self.transaction_stack and self.transaction_stack[-1] not in [
                transaction,
                transaction.parent,
            ]:
                # THIS TRANSACTION IS NOT THE CURRENT TRANSACTION, DELAY IT
                with self.locker:
                    if self.too_long is None:
                        self.too_long = Till(seconds=TOO_LONG_TO_HOLD_TRANSACTION)
                        self.too_long.then(self.show_transactions_blocked_warning)
                    self.delayed_transactions.append(command_item)
                return
            else:
                # ENSURE THE CURRENT TRANSACTION IS UP TO DATE FOR THIS query
                if not self.transaction_stack:
                    # sqlite3 ALLOWS ONLY ONE TRANSACTION AT A TIME
                    self.debug and Log.note(FORMAT_COMMAND, command=BEGIN)
                    self.db.execute(BEGIN)
                    self.transaction_stack.append(transaction)
                elif transaction is not self.transaction_stack[-1]:
                    self.transaction_stack.append(transaction)
                elif transaction.exception and query is not ROLLBACK:
                    result.exception = Except(
                        context=ERROR,
                        template="Not allowed to continue using a transaction that failed",
                        cause=transaction.exception,
                        trace=trace,
                    )
                    signal.release()
                    return

                try:
                    transaction.do_all()
                except Exception as e:
                    # DEAL WITH ERRORS IN QUEUED COMMANDS
                    # WE WILL UNWRAP THE OUTER EXCEPTION TO GET THE CAUSE
                    err = Except(
                        context=ERROR,
                        template="Bad call to Sqlite3 while " + FORMAT_COMMAND,
                        params={"command": e.params.current.command},
                        cause=e.cause,
                        trace=e.params.current.trace,
                    )
                    transaction.exception = result.exception = err

                    if query in [COMMIT, ROLLBACK]:
                        self._close_transaction(
                            CommandItem(ROLLBACK, result, signal, trace, transaction)
                        )

                    signal.release()
                    return

            try:
                # DEAL WITH END-OF-TRANSACTION MESSAGES
                if query in [COMMIT, ROLLBACK]:
                    self._close_transaction(command_item)
                    return

                # EXECUTE QUERY
                self.last_command_item = command_item
                self.debug and Log.note(FORMAT_COMMAND, command=query)
                curr = self.db.execute(text_type(query))
                result.meta.format = "table"
                result.header = (
                    [d[0] for d in curr.description] if curr.description else None
                )
                result.data = curr.fetchall()
                if self.debug and result.data:
                    text = convert.table2csv(list(result.data))
                    Log.note("Result:\n{{data|limit(100)|indent}}", data=text)
            except Exception as e:
                e = Except.wrap(e)
                err = Except(
                    context=ERROR,
                    template="Bad call to Sqlite while " + FORMAT_COMMAND,
                    params={"command": query},
                    trace=trace,
                    cause=e,
                )
                result.exception = err
                if transaction:
                    transaction.exception = err
            finally:
                signal.release()


class Transaction(object):
    def __init__(self, db, parent=None):
        self.db = db
        self.locker = Lock("transaction " + text_type(id(self)) + " todo lock")
        self.todo = []
        self.complete = 0
        self.end_of_life = False
        self.exception = None
        self.parent = parent
        self.thread = parent.thread if parent else Thread.current()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        causes = []
        try:
            if isinstance(exc_val, Exception):
                causes.append(Except.wrap(exc_val))
                self.rollback()
            else:
                self.commit()
        except Exception as e:
            causes.append(Except.wrap(e))
            Log.error("Transaction failed", cause=unwraplist(causes))

    def transaction(self):
        with self.db.locker:
            output = Transaction(self.db, parent=self)
            self.db.available_transactions.append(output)
        return output

    def execute(self, command):
        if self.end_of_life:
            Log.error("Transaction is dead")
        trace = extract_stack(1) if self.db.get_trace else None
        with self.locker:
            self.todo.append(CommandItem(command, None, None, trace, self))

    def do_all(self):
        # ENSURE PARENT TRANSACTION IS UP TO DATE
        c = None
        try:
            if self.parent == self:
                Log.warning("Transactions parent is equal to itself.")
            if self.parent:
                self.parent.do_all()
            # GET THE REMAINING COMMANDS
            with self.locker:
                todo = self.todo[self.complete :]
                self.complete = len(self.todo)

            # RUN THEM
            for c in todo:
                self.db.debug and Log.note(FORMAT_COMMAND, command=c.command)
                self.db.db.execute(text_type(c.command))
        except Exception as e:
            Log.error("problem running commands", current=c, cause=e)

    def query(self, query):
        if self.db.closed:
            Log.error("database is closed")

        signal = _allocate_lock()
        signal.acquire()
        result = Data()
        trace = extract_stack(1) if self.db.get_trace else None
        self.db.queue.add(CommandItem(query, result, signal, trace, self))
        signal.acquire()
        if result.exception:
            Log.error("Problem with Sqlite call", cause=result.exception)
        return result

    def rollback(self):
        self.query(ROLLBACK)

    def commit(self):
        self.query(COMMIT)


CommandItem = namedtuple(
    "CommandItem", ("command", "result", "is_done", "trace", "transaction")
)

_simple_word = re.compile(r"^\w+$", re.UNICODE)


def quote_column(*path):
    if not path:
        Log.error("expecting a name")
    if any(not is_text(p) for p in path):
        Log.error("expecting strings, not SQL")
    try:
        return ConcatSQL((SQL_SPACE, JoinSQL(SQL_DOT, [SQL(quote(p)) for p in path]), SQL_SPACE))
    except Exception as e:
        Log.error("Not expacted", cause=e)


def sql_alias(value, alias):
    if not isinstance(value, SQL) or not is_text(alias):
        Log.error("Expecting (SQL, text) parameters")
    return ConcatSQL((value, SQL_AS, quote_column(alias)))


def quote_value(value):
    if isinstance(value, (Mapping, list)):
        return SQL(".")
    elif isinstance(value, Date):
        return SQL(text_type(value.unix))
    elif isinstance(value, Duration):
        return SQL(text_type(value.seconds))
    elif is_text(value):
        return SQL("'" + value.replace("'", "''") + "'")
    elif value == None:
        return SQL_NULL
    elif value is True:
        return SQL_TRUE
    elif value is False:
        return SQL_FALSE
    else:
        return SQL(text_type(value))


def quote_list(values):
    return sql_iso(sql_list(map(quote_value, values)))


def sql_eq(**item):
    """
    RETURN SQL FOR COMPARING VARIABLES TO VALUES (AND'ED TOGETHER)

    :param item: keyword parameters representing variable and value
    :return: SQL
    """
    return SQL_AND.join(
        [
            ConcatSQL((quote_column(k), SQL_EQ, quote_value(v)))
            if v != None
            else ConcatSQL((quote_column(k), SQL_IS_NULL))
            for k, v in item.items()
        ]
    )


def sql_lt(**item):
    """
    RETURN SQL FOR LESS-THAN (<) COMPARISION BETWEEN VARIABLES TO VALUES

    :param item: keyword parameters representing variable and value
    :return: SQL
    """
    k, v = first(item.items())
    return ConcatSQL((quote_column(k), SQL_LT, quote_value(v)))


def sql_query(command):
    """
    VERY BASIC QUERY EXPRESSION TO SQL
    :param command: jx-expression
    :return: SQL
    """
    command = wrap(command)
    acc = [SQL_SELECT]
    if command.select:
        acc.append(JoinSQL(SQL_COMMA, map(quote_column, listwrap(command.select))))
    else:
        acc.append(SQL_STAR)

    acc.append(SQL_FROM)
    acc.append(quote_column(command["from"]))
    if command.where.eq:
        acc.append(SQL_WHERE)
        acc.append(sql_eq(**command.where.eq))
    if command.orderby:
        acc.append(SQL_ORDERBY)
        acc.append(JoinSQL(SQL_COMMA, map(quote_column, listwrap(command.orderby))))
    return ConcatSQL(acc)


def sql_create(table, properties, primary_key=None, unique=None):
    """
    :param table:  NAME OF THE TABLE TO CREATE
    :param properties: DICT WITH {name: type} PAIRS (type can be plain text)
    :param primary_key: COLUMNS THAT MAKE UP THE PRIMARY KEY
    :param unique: COLUMNS THAT SHOULD BE UNIQUE
    :return:
    """
    acc = [
        SQL_CREATE,
        quote_column(table),
        SQL_OP,
        sql_list([quote_column(k) + SQL(v) for k, v in properties.items()]),
    ]

    if primary_key:
        acc.append(SQL_COMMA),
        acc.append(SQL(" PRIMARY KEY ")),
        acc.append(sql_iso(sql_list([quote_column(c) for c in listwrap(primary_key)])))
    if unique:
        acc.append(SQL_COMMA),
        acc.append(SQL(" UNIQUE ")),
        acc.append(sql_iso(sql_list([quote_column(c) for c in listwrap(unique)])))

    acc.append(SQL_CP)
    return ConcatSQL(acc)


def sql_insert(table, records):
    records = listwrap(records)
    keys = list({k for r in records for k in r.keys()})
    return ConcatSQL(
        [
            SQL_INSERT,
            quote_column(table),
            sql_iso(sql_list(map(quote_column, keys))),
            SQL_VALUES,
            sql_list(
                sql_iso(sql_list([quote_value(r[k]) for k in keys])) for r in records
            ),
        ]
    )


BEGIN = "BEGIN"
COMMIT = "COMMIT"
ROLLBACK = "ROLLBACK"


def _upgrade():
    global _upgraded
    global _sqlite3

    try:
        import sys
        import platform

        if "windows" in platform.system().lower():
            original_dll = File.new_instance(sys.exec_prefix, "dlls/sqlite3.dll")
            if platform.architecture()[0] == "32bit":
                source_dll = File("vendor/pyLibrary/vendor/sqlite/sqlite3_32.dll")
            else:
                source_dll = File("vendor/pyLibrary/vendor/sqlite/sqlite3_64.dll")

            if not all(
                a == b
                for a, b in zip_longest(
                    source_dll.read_bytes(), original_dll.read_bytes()
                )
            ):
                original_dll.backup()
                File.copy(source_dll, original_dll)
        else:
            pass
    except Exception as e:
        Log.warning("could not upgrade python's sqlite", cause=e)

    import sqlite3 as _sqlite3

    _ = _sqlite3
    _upgraded = True


json_type_to_sqlite_type = {
    BOOLEAN: "TINYINT",
    INTEGER: "INTEGER",
    NUMBER: "REAL",
    STRING: "TEXT",
    OBJECT: "TEXT",
    NESTED: "TEXT",
}
