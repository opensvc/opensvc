from __future__ import print_function

import datetime
import json
import os
import sys
from optparse import Option, OptionParser
from subprocess import Popen, PIPE

import drivers.check
from utilities.proc import justcall
from utilities.storage import Storage

class Check(drivers.check.Check):
    chk_type = "jstat"

    def find_java(self):
        cmd = ["pgrep", "java"]
        out, err, ret = justcall(cmd)
        out = out.strip()
        if not out:
            return []
        return [int(pid) for pid in out.split()]

    def do_check(self):
        data = []
        for pid in self.find_java():
            ids = pid_to_ids(pid)
            if ids.instance is not None:
                instance = ids.instance
            elif ids.rid is not None:
                instance = ids.rid
            else:
                # not handled
                continue
            path = ids.path if ids.path else ids.name if ids.name else ""
            jstat = get_executable(Storage(), pid)
            if not jstat:
                continue
            for stat in STATS:
                for key, val in get_stat_metrics(jstat, pid, stat).items():
                    data.append({
                        "instance": "%s.%s.%s" % (instance, stat, key),
                        "value": val,
                        "path": path,
                    })
        #print(json.dumps(data, indent=4))
        return data

VERSION = "1.0"
USAGE = ""
STAT_HELP = """
Determines the statistics information that jstat displays. The following 
table lists the available options. Use the -options general option to 
display the list of options for a particular platform installation.

Option	          Displays...
======            ===========
class	          Statistics on the behavior of the class loader.
compiler          Statistics of the behavior of the HotSpot Just-in-Time
                  compiler.
gc                Statistics of the behavior of the garbage collected heap.
gccapacity        Statistics of the capacities of the generations and their
                  corresponding spaces.
gccause           Summary of garbage collection statistics (same as -gcutil),
                  with the cause of the last and current (if applicable)
                  garbage collection events.
gcnew             Statistics of the behavior of the new generation.
gcnewcapacity     Statistics of the sizes of the new generations and its
                  corresponding spaces.
gcold	          Statistics of the behavior of the old and permanent
                  generations.
gcoldcapacity     Statistics of the sizes of the old generation.
gcpermcapacity    Statistics of the sizes of the permanent generation.
gcutil            Summary of garbage collection statistics.
printcompilation  HotSpot compilation method statistics.
"""

STATS = [
    "class",
#    "compiler",
    "gc",
    "gccapacity",
#    "gccause",
    "gcnew",
    "gcnewcapacity",
    "gcold",
    "gcoldcapacity",
    "gcpermcapacity",
    "gcutil",
#    "printcompilation",
]
DEFAULT_STATS = [
    "gc",
]

OPTIONS = [
     Option(
        "-p", "--pid", action="store", dest="pid", type="int",
        help="The pid of the JVM to extract metrics from."),
     Option(
        "-x", "--executable", action="store", dest="executable",
        help="The jstat executable to use. Defaults to the jstat "
             "binary found in the same dir as the monitored java."),
     Option(
        "-s", "--stat", action="append", dest="stat",
        help=STAT_HELP),
]


def pid_to_ids(pid):
    with open("/proc/%d/environ" % pid) as fp:
        buff = fp.read()
    data = Storage()
    for line in buff.split('\0'):
        line = line.replace("\n", "")
        try:
            key, val = line.split("=", 1)
        except ValueError:
            continue
        if key == "OPENSVC_SVC_ID":
            data["svc_id"] = val
        elif key == "OPENSVC_SVCNAME":
            data["svcname"] = val
        elif key == "OPENSVC_SVCPATH":
            data["path"] = val
        elif key == "OPENSVC_RID":
            data["rid"] = val
        elif key == "OPENSVC_CHK_INSTANCE":
            data["instance"] = val
    return data

def get_pid(options):
    if options.pid:
        return options.pid

def get_executable(options, pid):
    if options.executable:
        return options.executable
    java = os.readlink("/proc/%d/exe" % pid)
    java_d = os.path.dirname(java)
    jstat = os.path.join(java_d, "jstat")
    if os.path.exists(jstat):
        return jstat

def get_stats(options):
    if options.stat:
        return list(set(options.stat)-set(STATS))
    return DEFAULT_STATS

def get_stat_metrics(jstat, pid, stat):
    cmd = [jstat, "-%s"%stat, str(pid)]
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if proc.returncode != 0:
        return {}
    lines = out.decode().splitlines()
    headers = lines[0].split()
    metrics = [float(val) for val in lines[1].replace(",", ".").split()]
    data = {}
    for head, metric in zip(headers, metrics):
        data[head] = metric
    return data

def main(argv):
    parser = OptionParser(version=VERSION, usage=USAGE, option_list=OPTIONS)
    options, args = parser.parse_args(argv)

    pid = get_pid(options)
    if pid is None:
        print("pid not defined and not guessed", file=sys.stderr)
        return 1

    jstat = get_executable(options, pid)
    if jstat is None:
        print("jstat executable not defined and not guessed", file=sys.stderr)
        return 1

    stats = get_stats(options)
    data = {
        "timestamp": int(datetime.datetime.utcnow().timestamp()),
        "stats": {},
    }

    for stat in stats:
        data["stats"][stat] = get_stat_metrics(jstat, pid, stat)

    print(json.dumps(data, indent=4))
    

if __name__ == "__main__":
    ret = main(sys.argv)
    sys.exit(ret)

