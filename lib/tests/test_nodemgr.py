# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import sys
import json
from StringIO import StringIO

import nodemgr

UNICODE_STRING = "bêh"

def test_nodemgr_print_schedule():
    """
    Print node schedules
    """
    ret = nodemgr.main(argv=["print", "schedule"])
    assert ret == 0

def test_nodemgr_print_schedule_json():
    """
    Print node schedules (json format)
    """
    _stdout = sys.stdout

    try:
        out = StringIO()
        sys.stdout = out
        ret = nodemgr.main(argv=["print", "schedule", "--format", "json", "--color", "no"])
        output = out.getvalue().strip()
    finally:
        sys.stdout = _stdout

    print(output)
    data = json.loads(output)

    assert ret == 0
    assert isinstance(json.loads(output), list)

def test_nodemgr_print_config():
    """
    Print node config
    """
    ret = nodemgr.main(argv=["print", "config"])
    assert ret == 0

def test_nodemgr_print_config_json():
    """
    Print node config (json format)
    """
    _stdout = sys.stdout

    try:
        out = StringIO()
        sys.stdout = out
        ret = nodemgr.main(argv=["print", "config", "--format", "json", "--color", "no"])
        output = out.getvalue().strip()
    finally:
        sys.stdout = _stdout

    print(output)
    data = json.loads(output)

    assert ret == 0
    assert isinstance(json.loads(output), dict)

def test_nodemgr_print_authconfig():
    """
    Print node auth config
    """
    ret = nodemgr.main(argv=["print", "authconfig"])
    assert ret == 0

def test_nodemgr_print_authconfig_json():
    """
    Print node auth config (json format)
    """
    _stdout = sys.stdout

    try:
        out = StringIO()
        sys.stdout = out
        ret = nodemgr.main(argv=["print", "authconfig", "--format", "json", "--color", "no"])
        output = out.getvalue().strip()
    finally:
        sys.stdout = _stdout

    print(output)
    data = json.loads(output)

    assert ret == 0
    assert isinstance(json.loads(output), dict)

def test_set():
    """
    Set node env.comment to a unicode string
    """
    ret = nodemgr.main(argv=["set", "--param", "env.comment", "--value", UNICODE_STRING])
    assert ret == 0

def test_get():
    """
    Get node env.comment
    """
    _stdout = sys.stdout

    try:
        out = StringIO()
        sys.stdout = out
        ret = nodemgr.main(argv=["get", "--param", "env.comment"])
        output = out.getvalue().strip()
    finally:
        sys.stdout = _stdout

    from rcUtilities import try_decode
    print(output)

    assert ret == 0
    assert try_decode(output) == UNICODE_STRING

def test_unset():
    """
    Unset env.comment
    """
    ret = nodemgr.main(argv=["unset", "--param", "env.comment"])
    assert ret == 0

def test_get_not_found():
    """
    Get an unset keyword
    """
    _stderr = sys.stdout

    try:
        err = StringIO()
        sys.stderr = err
        ret = nodemgr.main(argv=["get", "--param", "env.comment"])
        output = err.getvalue().strip()
    finally:
        sys.stderr = _stderr

    assert ret == 1

def test_nodemgr_checks():
    """
    Run node checks
    """
    ret = nodemgr.main(argv=["checks"])
    assert ret == 0

def test_nodemgr_sysreport():
    """
    Run node sysreport
    """
    ret = nodemgr.main(argv=["sysreport"])
    assert ret == 0

def test_nodemgr_pushasset():
    """
    Run node pushasset
    """
    ret = nodemgr.main(argv=["pushasset"])
    assert ret == 0

def test_nodemgr_collect_stats():
    """
    Run node collect stats
    """
    ret = nodemgr.main(argv=["collect_stats"])
    assert ret == 0

def test_nodemgr_pushstats():
    """
    Run node pushstats
    """
    ret = nodemgr.main(argv=["pushstats"])
    assert ret == 0

def test_nodemgr_pushpkg():
    """
    Run node pushpkg
    """
    ret = nodemgr.main(argv=["pushpkg"])
    assert ret == 0

def test_nodemgr_pushpatch():
    """
    Run node pushpatch
    """
    ret = nodemgr.main(argv=["pushpatch"])
    assert ret == 0

def test_nodemgr_pushdisks():
    """
    Run node pushdisks
    """
    ret = nodemgr.main(argv=["pushdisks"])
    assert ret == 0

def test_nodemgr_logs():
    """
    Print node logs
    """
    ret = nodemgr.main(argv=["logs", "--no-pager"])
    assert ret == 0


