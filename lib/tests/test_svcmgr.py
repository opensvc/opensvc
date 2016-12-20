from __future__ import print_function

import sys
import json
from StringIO import StringIO

import svcmgr

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


