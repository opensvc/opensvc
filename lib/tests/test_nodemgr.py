from __future__ import print_function

import sys
import json
from StringIO import StringIO

import nodemgr

def test_nodemgr_print_schedule():
    ret = nodemgr.main(argv=["print", "schedule"])
    assert ret == 0

def test_nodemgr_print_schedule_json():
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
    ret = nodemgr.main(argv=["print", "config"])
    assert ret == 0

def test_nodemgr_print_config_json():
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
    ret = nodemgr.main(argv=["print", "authconfig"])
    assert ret == 0

def test_nodemgr_print_authconfig_json():
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


