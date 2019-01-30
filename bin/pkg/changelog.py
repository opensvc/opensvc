#!/usr/bin/python

from __future__ import print_function
import sys
from subprocess import *
from distutils.version import LooseVersion

try:
    branch = sys.argv[1]
except:
    branch = "HEAD"

cmd = ["git", "log", "--oneline", "%s.."%branch]
proc = Popen(cmd, stdout=PIPE)
out, _ = proc.communicate()
commits = {}
for line in out.decode().splitlines():
    cid, desc = line.split(" ", 1)
    commits[cid] = [cid, desc]
cids = [c for c in commits]
cmd = ["git", "describe", "--tags"] + cids
proc = Popen(cmd, stdout=PIPE)
out, _ = proc.communicate()
versions = list(out.decode().splitlines())
for i, cid in enumerate(cids):
    commits[cid].insert(0, versions[i])
for commit in sorted(commits.values(), key=lambda x: LooseVersion(x[0]), reverse=True):
    print("%-18s  %s" % (commit[0], commit[2]))
