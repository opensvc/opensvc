# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import json
import socket
from StringIO import StringIO

import svcmgr
import nodemgr

UNICODE_STRING = "bêh"

class TestSvcmgr:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_001_svcmgr_print_schedule(self):
        ret = svcmgr.main(argv=["-s", "*", "print", "schedule"])
        assert ret == 0

    def test_002_svcmgr_print_schedule_json(self):
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = svcmgr.main(argv=["-s", "*", "print", "schedule", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        print(output)
        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    def test_003_svcmgr_print_config(self):
        ret = svcmgr.main(argv=["-s", "*", "print", "config"])
        assert ret == 0

    def test_004_svcmgr_print_config_json(self):
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = svcmgr.main(argv=["-s", "*", "print", "config", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        print(output)
        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    def test_005_svcmgr_print_status(self):
        ret = svcmgr.main(argv=["-s", "*", "print", "status"])
        assert ret == 0

    def test_006_svcmgr_print_status_json(self):
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = svcmgr.main(argv=["-s", "*", "print", "status", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        print(output)
        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    def test_007_create_empty(self):
        ret = svcmgr.main(argv=["create", "-s", "unittest"])
        assert ret == 0

    def test_008_set_default(self):
        ret = svcmgr.main(argv=["-s", "unittest", "set", "--param", "comment", "--value", UNICODE_STRING])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", "unittest", "set", "--param", "env.list_entry_ref_indirect_eval2", "--value", "{nodes[$(0//(3//{#nodes}))]}"])
        assert ret == 0

    def test_009_get_default(self):
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

    def test_010_unset_default(self):
        ret = svcmgr.main(argv=["-s", "unittest", "unset", "--param", "comment"])
        assert ret == 0

    def test_011_get_default_not_found(self):
        _stderr = sys.stderr

        try:
            err = StringIO()
            sys.stderr = err
            ret = svcmgr.main(argv=["-s", "unittest", "get", "--param", "comment"])
            output = err.getvalue().strip()
        finally:
            sys.stderr = _stderr

        assert ret == 1

    def test_012_get_list_entry_ref_indirect_eval2(self):
        nodename = socket.gethostname().lower()
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = svcmgr.main(argv=["-s", "unittest", "get", "--param", "env.list_entry_ref_indirect_eval2", "--eval"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        assert ret == 0
        assert output == nodename

    def test_013_validate_config(self):
        ret = svcmgr.main(argv=["validate", "config", "-s", "unittest"])
        assert ret == 0

    def test_014_frozen(self):
        """
        The service is frozen after create.
        """
        ret = svcmgr.main(argv=["frozen", "-s", "unittest"])
        assert ret == 1
        ret = svcmgr.main(argv=["thaw", "-s", "unittest", "--local"])
        assert ret == 0

    def test_015_node_freeze(self):
        ret = nodemgr.main(argv=["freeze", "--local"])
        assert ret == 0
        ret = nodemgr.main(argv=["frozen"])
        assert ret == 1
        ret = svcmgr.main(argv=["frozen", "-s", "unittest"])
        assert ret == 0

    def test_016_node_refreeze(self):
        """
        Re-freeze a frozen node is a valid noop.
        """
        ret = nodemgr.main(argv=["freeze", "--local"])
        assert ret == 0

    def test_017_node_thaw(self):
        ret = nodemgr.main(argv=["thaw", "--local"])
        assert ret == 0
        ret = nodemgr.main(argv=["frozen"])
        assert ret == 0
        ret = svcmgr.main(argv=["frozen", "-s", "unittest"])
        assert ret == 0

    def test_018_node_rethaw(self):
        ret = nodemgr.main(argv=["thaw", "--local"])
        assert ret == 0

    def test_019_freeze(self):
        ret = svcmgr.main(argv=["freeze", "-s", "unittest", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["frozen", "-s", "unittest"])
        assert ret == 1

    def test_020_thaw(self):
        ret = svcmgr.main(argv=["thaw", "-s", "unittest", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["frozen", "-s", "unittest"])
        assert ret == 0

    def test_021_logs(self):
        ret = svcmgr.main(argv=["logs", "-s", "unittest", "--no-pager"])
        assert ret == 0

    def test_022_push(self):
        ret = svcmgr.main(argv=["push", "-s", "unittest"])
        assert ret == 0

    def test_023_pull(self):
        ret = svcmgr.main(argv=["pull", "-s", "unittest"])
        assert ret == 0

    def test_024_svc_selector(self):
        ret = svcmgr.main(argv=["ls", "-s", "uni*"])
        assert ret == 0
        ret = svcmgr.main(argv=["ls", "-s", "notexists*"])
        assert ret == 0
        ret = svcmgr.main(argv=["ls", "-s", "*dns,ha*+app.timeout>1*"])
        assert ret == 0
        ret = svcmgr.main(argv=["ls", "-s", "ip:+task:"])
        assert ret == 0
        ret = svcmgr.main(argv=["ls", "-s", "!*excluded"])
        assert ret == 0

    def test_025_delete(self):
        ret = svcmgr.main(argv=["delete", "-s", "unittest", "--local"])
        assert ret == 0


