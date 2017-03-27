# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

import mo_json
from activedata_etl import etl2path
from activedata_etl import key2etl
from mo_dots import coalesce, wrap, Null
from mo_kwargs import override
from mo_logs import Log, strings
from mo_logs.exceptions import suppress_exception
from mo_math.randoms import Random
from mo_threads import Lock
from mo_times.dates import Date, unicode2Date, unix2Date
from mo_times.durations import Duration
from mo_times.timer import Timer
from pyLibrary import convert
from pyLibrary.aws.s3 import strip_extension
from pyLibrary.env import elasticsearch
from pyLibrary.queries import jx

MAX_RECORD_LENGTH = 400000


class RolloverIndex(object):
    """
    MIMIC THE elasticsearch.Index, WITH EXTRA keys() FUNCTION
    AND THREADED QUEUE AND SPLIT DATA BY
    """
    @override
    def __init__(self, rollover_field, rollover_interval, rollover_max, queue_size=10000, batch_size=5000, kwargs=None):
        """
        :param rollover_field: the FIELD with a timestamp to use for determining which index to push to
        :param rollover_interval: duration between roll-over to new index
        :param rollover_max: remove old indexes, do not add old records
        :param queue_size: number of documents to queue in memory
        :param batch_size: number of documents to push at once
        :param kwargs: plus additional ES settings
        :return:
        """
        self.settings = kwargs
        self.locker = Lock("lock for rollover_index")
        self.rollover_field = jx.get(rollover_field)
        self.rollover_interval = self.settings.rollover_interval = Duration(kwargs.rollover_interval)
        self.rollover_max = self.settings.rollover_max = Duration(kwargs.rollover_max)
        self.known_queues = {}  # MAP DATE TO INDEX
        self.cluster = elasticsearch.Cluster(self.settings)

    def __getattr__(self, item):
        return getattr(self.cluster, item)
        # Log.error("Not supported")

    def _get_queue(self, row):
        row = wrap(row)
        if row.json:
            row.value, row.json = mo_json.json2value(row.json), None
        timestamp = Date(self.rollover_field(wrap(row).value))
        if timestamp == None or timestamp < Date.today() - self.rollover_max:
            return Null

        rounded_timestamp = timestamp.floor(self.rollover_interval)
        with self.locker:
            queue = self.known_queues.get(rounded_timestamp.unix)
        if queue == None:
            candidates = jx.run({
                "from": self.cluster.get_aliases(),
                "where": {"regex": {"index": self.settings.index + "\d\d\d\d\d\d\d\d_\d\d\d\d\d\d"}},
                "sort": "index"
            })
            best = None
            for c in candidates:
                c = wrap(c)
                c.date = unicode2Date(c.index[-15:], elasticsearch.INDEX_DATE_FORMAT)
                if timestamp > c.date:
                    best = c
            if not best or rounded_timestamp > best.date:
                if rounded_timestamp < wrap(candidates[-1]).date:
                    es = elasticsearch.Index(read_only=False, alias=best.alias, index=best.index, kwargs=self.settings)
                else:
                    try:
                        es = self.cluster.create_index(create_timestamp=rounded_timestamp, kwargs=self.settings)
                        es.add_alias(self.settings.index)
                    except Exception as e:
                        if "IndexAlreadyExistsException" not in e:
                            Log.error("Problem creating index", cause=e)
                        return self._get_queue(row)  # TRY AGAIN
            else:
                es = elasticsearch.Index(read_only=False, alias=best.alias, index=best.index, kwargs=self.settings)

            with suppress_exception:
                es.set_refresh_interval(seconds=60 * 5, timeout=5)

            self._delete_old_indexes(candidates)
            threaded_queue = es.threaded_queue(max_size=self.settings.queue_size, batch_size=self.settings.batch_size, silent=True)
            with self.locker:
                queue = self.known_queues[rounded_timestamp.unix] = threaded_queue
        return queue

    def _delete_old_indexes(self, candidates):
        for c in candidates:
            timestamp = unicode2Date(c.index[-15:], "%Y%m%d_%H%M%S")
            if timestamp + self.rollover_interval < Date.today() - self.rollover_max:
                # Log.warning("Will delete {{index}}", index=c.index)
                try:
                    self.cluster.delete_index(c.index)
                except Exception as e:
                    Log.warning("could not delete index {{index}}", index=c.index, cause=e)
        for t, q in list(self.known_queues.items()):
            if unix2Date(t) + self.rollover_interval < Date.today() - self.rollover_max:
                with self.locker:
                    del self.known_queues[t]

        pass

    # ADD keys() SO ETL LOOP CAN FIND WHAT'S GETTING REPLACED
    def keys(self, prefix=None):
        path = jx.reverse(etl2path(key2etl(prefix)))

        result = self.es.search({
            "fields": ["_id"],
            "query": {
                "filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [{"term": {"etl" + (".source" * i) + ".id": v}} for i, v in enumerate(path)]}
                }
            }
        })

        if result.hits.hits:
            return set(result.hits.hits._id)
        else:
            return set()

    def extend(self, documents, queue=None):
        i = 0;
        if queue == None:
            for i, doc in enumerate(documents):
                queue = self._get_queue(doc)
                if queue != None:
                    break
            else:
                Log.note("All documents are too old")
                return

        queue.extend(documents[i::])

    def add(self, doc, queue=None):
        if queue == None:
            queue = self._get_queue(doc)
            if queue == None:
                Log.note("Document not added: Too old")
                return

        queue.add(doc)

    def delete(self, filter):
        self.es.delete(filter)

    def copy(self, keys, source, sample_only_filter=None, sample_size=None, done_copy=None):
        """
        :param keys: THE KEYS TO LOAD FROM source
        :param source: THE SOURCE (USUALLY S3 BUCKET)
        :param sample_only_filter: SOME FILTER, IN CASE YOU DO NOT WANT TO SEND EVERYTHING
        :param sample_size: FOR RANDOM SAMPLE OF THE source DATA
        :param done_copy: CALLBACK, ADDED TO queue, TO FINISH THE TRANSACTION
        :return: LIST OF SUB-keys PUSHED INTO ES
        """
        num_keys = 0
        queue = None
        pending = []  # FOR WHEN WE DO NOT HAVE QUEUE YET
        for key in keys:
            timer = Timer("Process {{key}}", param={"key": key})
            try:
                with timer:
                    for rownum, line in enumerate(source.read_lines(strip_extension(key))):
                        if not line:
                            continue

                        if rownum > 0 and rownum % 1000 == 0:
                            Log.note("Ingested {{num}} records from {{key}} in bucket {{bucket}}", num=rownum, key=key, bucket=source.name)

                        row, please_stop = fix(rownum, line, source, sample_only_filter, sample_size)
                        num_keys += 1

                        if queue == None:
                            queue = self._get_queue(row)
                            if queue == None:
                                pending.append(row)
                                continue
                            if pending:
                                queue.extend(pending)
                                pending = []

                        queue.add(row)

                        if please_stop:
                            break
            except Exception as e:
                done_copy = None
                Log.warning("Could not process {{key}} after {{duration|round(places=2)}}seconds", key=key, duration=timer.duration.seconds, cause=e)

        if done_copy:
            if queue == None:
                done_copy()
            else:
                queue.add(done_copy)

        Log.note("{{num}} keys from {{key|json}} added", num=num_keys, key=keys)
        return num_keys


def fix(rownum, line, source, sample_only_filter, sample_size):
    # ES SCHEMA IS STRICTLY TYPED, USE "code" FOR TEXT IDS
    line = line.replace('{"id": "bb"}', '{"code": "bb"}').replace('{"id": "tc"}', '{"code": "tc"}')

    # ES SCHEMA IS STRICTLY TYPED, THE SUITE OBJECT CAN NOT BE HANDLED
    if source.name.startswith("active-data-test-result"):
        # "suite": {"flavor": "plain-chunked", "name": "mochitest"}
        found = strings.between(line, '"suite": {', '}')
        if found:
            suite_json = '{' + found + "}"
            if suite_json:
                suite = mo_json.json2value(suite_json)
                suite = convert.value2json(coalesce(suite.fullname, suite.name))
                line = line.replace(suite_json, suite)

    if rownum == 0:
        value = mo_json.json2value(line)
        if len(line) > MAX_RECORD_LENGTH:
            _shorten(value, source)
        _id, value = _fix(value)
        row = {"id": _id, "value": value}
        if sample_only_filter and Random.int(int(1.0/coalesce(sample_size, 0.01))) != 0 and jx.filter([value], sample_only_filter):
            # INDEX etl.id==0, BUT NO MORE
            if value.etl.id != 0:
                Log.error("Expecting etl.id==0")
            return row, True
    elif len(line) > MAX_RECORD_LENGTH:
        value = mo_json.json2value(line)
        _shorten(value, source)
        _id, value = _fix(value)
        row = {"id": _id, "value": value}
    elif line.find('"resource_usage":') != -1:
        value = mo_json.json2value(line)
        _id, value = _fix(value)
        row = {"id": _id, "value": value}
    else:
        # FAST
        _id = strings.between(line, "\"_id\": \"", "\"")  # AVOID DECODING JSON
        row = {"id": _id, "json": line}

    return row, False


def _shorten(value, source):
    if source.name.startswith("active-data-test-result"):
        value.result.subtests = [s for s in value.result.subtests if s.ok is False]
        value.result.missing_subtests = True
        value.repo.changeset.files = None

    shorter_length = len(convert.value2json(value))
    if shorter_length > MAX_RECORD_LENGTH:
        result_size = len(convert.value2json(value.result))
        if source.name == "active-data-test-result":
            if result_size > MAX_RECORD_LENGTH:
                Log.warning("Epic test failure in {{name}} results in big record for {{id}} of length {{length}}", id=value._id, name=source.name, length=shorter_length)
            else:
                pass  # NOT A PROBLEM
        else:
            Log.warning("Monstrous {{name}} record {{id}} of length {{length}}", id=value._id, name=source.name, length=shorter_length)


def _fix(value):
    if value.repo._source:
        value.repo = value.repo._source
    if not value.build.revision12:
        value.build.revision12 = value.build.revision[0:12]
    if value.resource_usage:
        value.resource_usage = None

    _id = value._id

    return _id, value
