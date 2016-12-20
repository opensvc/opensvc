# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import sys
import json
from StringIO import StringIO

import svcmgr

UNICODE_STRING = "bêh"

def test_svcmgr_print_schedule():
    ret = svcmgr.main(argv=["print", "schedule"])
    assert ret == 0

def test_svcmgr_print_schedule_json():
    _stdout = sys.stdout

    try:
        out = StringIO()
        sys.stdout = out
        ret = svcmgr.main(argv=["print", "schedule", "--format", "json", "--color", "no"])
        output = out.getvalue().strip()
    finally:
        sys.stdout = _stdout

    print(output)
    data = json.loads(output)

    assert ret == 0
    assert isinstance(json.loads(output), dict)

def test_svcmgr_print_config():
    ret = svcmgr.main(argv=["print", "config"])
    assert ret == 0

def test_svcmgr_print_config_json():
    _stdout = sys.stdout

    try:
        out = StringIO()
        sys.stdout = out
        ret = svcmgr.main(argv=["print", "config", "--format", "json", "--color", "no"])
        output = out.getvalue().strip()
    finally:
        sys.stdout = _stdout

    print(output)
    data = json.loads(output)

    assert ret == 0
    assert isinstance(json.loads(output), dict)

def test_svcmgr_print_status():
    ret = svcmgr.main(argv=["print", "status"])
    assert ret == 0

def test_svcmgr_print_status_json():
    _stdout = sys.stdout

    try:
        out = StringIO()
        sys.stdout = out
        ret = svcmgr.main(argv=["print", "status", "--format", "json", "--color", "no"])
        output = out.getvalue().strip()
    finally:
        sys.stdout = _stdout

    print(output)
    data = json.loads(output)

    assert ret == 0
    assert isinstance(json.loads(output), dict)

def test_create_empty():
    ret = svcmgr.main(argv=["create", "-s", "unittest"])
    assert ret == 0

def test_create_set_default():
    ret = svcmgr.main(argv=["-s", "unittest", "set", "--param", "comment", "--value", UNICODE_STRING])
    assert ret == 0

def test_create_get_default():
    _stdout = sys.stdout

    try:
        out = StringIO()
        sys.stdout = out
        ret = svcmgr.main(argv=["-s", "unittest", "get", "--param", "comment"])
        output = out.getvalue().strip()
    finally:
        sys.stdout = _stdout

    from rcUtilities import try_decode
    print(output)

    assert ret == 0
    assert try_decode(output) == UNICODE_STRING

def test_create_unset_default():
    ret = svcmgr.main(argv=["-s", "unittest", "unset", "--param", "comment"])
    assert ret == 0

def test_create_get_default_not_found():
    _stderr = sys.stdout

    try:
        err = StringIO()
        sys.stderr = err
        ret = svcmgr.main(argv=["-s", "unittest", "get", "--param", "comment"])
        output = err.getvalue().strip()
    finally:
        sys.stderr = _stderr

    assert ret == 1
    assert "not found" in output

def test_delete():
    ret = svcmgr.main(argv=["delete", "-s", "unittest"])
    assert ret == 0


