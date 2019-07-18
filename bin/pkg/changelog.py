#!/usr/bin/python

from __future__ import print_function
import argparse
import sys
from subprocess import *
from distutils.version import LooseVersion

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--commits", type=str, default="HEAD", help="commits boundary. ex : 2.0..HEAD ")
parser.add_argument("-v", "--verbose", action="store_true", help="add long commit id")
args = parser.parse_args()

def get_commits():
    cmd = ["git", "log", '--pretty=format:%h %H %s', args.commits]
    proc = Popen(cmd, stdout=PIPE)
    out, _ = proc.communicate()
    commits = {}
    for line in out.decode().splitlines():
        cid, lcid, desc = line.split(" ", 2)
        commits[cid] = [cid, lcid, desc]
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
        if args.verbose:
            print("%-18s %s  %s" % (commit[0], commit[2], commit[3]))
        else:
            print("%-18s  %s" % (commit[0], commit[3]))

try:
    main()
except BrokenPipeError:
    pass
