#!/usr/bin/env opensvc

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'com.opensvc'))

from comp import *

import files
import packages

syntax = """syntax: %s check|fixable|fix"""%sys.argv[0]

if len(sys.argv) != 2:
    print >>sys.stderr, "wrong number of arguments"
    print >>sys.stderr, syntax
    sys.exit(RET_ERR)

objs = []

try:
    o = packages.CompPackages(prefix='OSVC_COMP_BDC_DHCPD_PACKAGE')
    objs.append(o)
except NotApplicable:
    pass

try:
    o = files.CompFiles(prefix='OSVC_COMP_BDC_DHCPD_FILE')
    objs.append(o)
except NotApplicable:
    pass

def check():
    r = 0
    for o in objs:
        r |= o.check()
    return r

def fixable():
    return RET_NA

def fix():
    r = 0
    for o in objs:
        r |= o.fix()
    return r

try:
    if sys.argv[1] == 'check':
        RET = check()
    elif sys.argv[1] == 'fix':
        RET = fix()
    elif sys.argv[1] == 'fixable':
        RET = fixable()
    else:
        print >>sys.stderr, "unsupported argument '%s'"%sys.argv[1]
        print >>sys.stderr, syntax
        RET = RET_ERR
except NotApplicable:
    sys.exit(RET_NA)
except:
    import traceback
    traceback.print_exc()
    sys.exit(RET_ERR)

sys.exit(RET)

