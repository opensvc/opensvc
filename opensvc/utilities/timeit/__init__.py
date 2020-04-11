from __future__ import print_function

import os
import sys

TIMING = os.environ.get("OSVC_TIMING")
TIMING_DATA = []

if TIMING:
    import atexit
    import inspect
    import json
    import time

    exec_start = time.time()

    def print_timings():
        exec_end = time.time()
        exec_duration = (exec_end - exec_start) * 1000
        total_duration = 0
        total_calls = 0
        for d in sorted(TIMING_DATA, key=lambda x: x["duration"]):
            print(json.dumps(d, indent=4), file=sys.stderr)
            total_duration += d["duration"]
            total_calls += 1
        print("Total number of timed functions calls: %d" % total_calls, file=sys.stderr)
        print("Total duration of timed functions:     %2.2f ms" % total_duration, file=sys.stderr)
        print("Total duration of the execution:       %2.2f ms" % exec_duration, file=sys.stderr)

    atexit.register(print_timings)

def timeit(method):
    if TIMING != "1":
        return method
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        du = (te - ts) * 1000
        frame = inspect.stack()[1]
        try:
            cmd = " ".join(args[0])
        except:
            cmd = str(args)
        try:
            _, fr_filename, fr_lineno, fr_function, _, _ = frame
        except:
            fr_filename = frame.filename
            fr_lineno = frame.lineno
            fr_function = frame.function
        TIMING_DATA.append({
            "fn": method.__name__,
            "cmd": cmd,
            "start": ts,
            "duration": du,
            "frame": {
                "filename": fr_filename,
                "lineno": fr_lineno,
                "function": fr_function,
            },
        })
        #print("%r %2.2f ms\n  %s\n  %s\n  %s" % (method.__name__, du, frame, args, kwargs), file=sys.stderr)
        return result
    return timed


