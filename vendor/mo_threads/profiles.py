# encoding: utf-8
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

import cProfile
import pstats
from datetime import datetime
from time import clock

from mo_dots import Data, wrap
from mo_logs import Log

ON = False
profiles = {}

class Profiler(object):
    """
    VERY SIMPLE PROFILER FOR USE IN with STATEMENTS
    PRIMARILY TO BE USED IN PyPy, WHERE cProfile IMPACTS
    OPTIMIZED RUN TIME TOO MUCH
    """

    def __new__(cls, *args):
        if ON:
            output = profiles.get(args[0])
            if output:
                return output
        output = object.__new__(cls)
        return output

    def __init__(self, description):
        if ON and not hasattr(self, "description"):
            from jx_python.windows import Stats
            self.description = description
            self.samples = []
            self.stats = Stats()()
            profiles[description] = self

    def __enter__(self):
        if ON:
            self.start = clock()
        return self

    def __exit__(self, type, value, traceback):
        if ON:
            self.end = clock()
            duration = self.end - self.start

            from jx_python.windows import Stats

            self.stats.add(duration)
            if self.samples is not None:
                self.samples.append(duration)
                if len(self.samples) > 100:
                    self.samples = None


def write(profile_settings):
    from mo_files import File
    from mo_logs.convert import datetime2string
    from mo_math import MAX
    from pyLibrary.convert import list2tab

    profs = list(profiles.values())
    for p in profs:
        p.stats = p.stats.end()

    stats = [{
        "description": p.description,
        "num_calls": p.stats.count,
        "total_time": p.stats.count * p.stats.mean,
        "total_time_per_call": p.stats.mean
    }
        for p in profs if p.stats.count > 0
    ]
    stats_file = File(profile_settings.filename, suffix=datetime2string(datetime.now(), "_%Y%m%d_%H%M%S"))
    if stats:
        stats_file.write(list2tab(stats))
    else:
        stats_file.write("<no profiles>")

    stats_file2 = File(profile_settings.filename, suffix=datetime2string(datetime.now(), "_series_%Y%m%d_%H%M%S"))
    if not profs:
        return

    max_samples = MAX([len(p.samples) for p in profs if p.samples])
    if not max_samples:
        return

    r = range(max_samples)
    profs.insert(0, Data(description="index", samples=r))
    stats = [
        {p.description: wrap(p.samples)[i] for p in profs if p.samples}
        for i in r
    ]
    if stats:
        stats_file2.write(list2tab(stats))


class CProfiler(object):
    """
    cProfiler WRAPPER TO HANDLE ROGUE THREADS (NOT PROFILED BY DEFAULT)
    """

    def __init__(self):
        self.cprofiler = None

    def __enter__(self):
        if Log.cprofiler:
            Log.note("starting cprofile")
            self.cprofiler = cProfile.Profile()
            self.cprofiler.enable()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cprofiler:
            self.cprofiler.disable()
            Log.cprofiler_stats.add(pstats.Stats(self.cprofiler))
            del self.cprofiler
            Log.note("done cprofile")

