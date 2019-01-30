#!/usr/bin/python

from __future__ import print_function
import sys
from subprocess import *
from distutils.version import LooseVersion

try:
    arg = sys.argv[1]
except:
    arg = "HEAD"

def get_commits():
    cmd = ["git", "log", "--oneline", arg]
    proc = Popen(cmd, stdout=PIPE)
    out, _ = proc.communicate()
    commits = {}
    for line in out.decode().splitlines():
        cid, desc = line.split(" ", 1)
        commits[cid] = [cid, desc]
    return commits

def get_versions(cids):
    cmd = ["git", "describe", "--tags"] + cids
    proc = Popen(cmd, stdout=PIPE)
    out, _ = proc.communicate()
    versions = list(out.decode().splitlines())
    return versions

def main():
    commits = get_commits()
    cids = [c for c in commits]
    versions = get_versions(cids)
    for i, cid in enumerate(cids):
        commits[cid].insert(0, versions[i])
    for commit in sorted(commits.values(), key=lambda x: LooseVersion(x[0]), reverse=True):
        print("%-18s  %s" % (commit[0], commit[2]))

try:
    main()
except BrokenPipeError:
    pass
