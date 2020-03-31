# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import socket
import sys
import uuid

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import commands.node
import commands.svc
from utilities.string import try_decode


UNICODE_STRING = "bêh"
SVCNAME = "unittest-" + str(uuid.uuid4())


class TestSvcmgr:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_001_create_empty(self):
        """
        Create a trivial service
        """
        ret = commands.svc.main(argv=["create", "-s", SVCNAME])
        assert ret == 0
        ret = commands.svc.main(argv=["create", "-s", SVCNAME + '2'])
        assert ret == 0

    def test_002_svc_print_schedule(self):
        """
        Print all services schedules
        """
        ret = commands.svc.main(argv=["-s", "*", "print", "schedule"])
        assert ret == 0

    def test_003_svc_print_schedule_json(self):
        """
        Print all services schedules (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = commands.svc.main(argv=["-s", "*", "print", "schedule", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        print(output)
        print(ret)
        data = json.loads(output)

        assert ret == 0
        assert isinstance(data, dict)

    def test_004_svc_print_config(self):
        """
        Print all services config
        """
        ret = commands.svc.main(argv=["-s", "*", "print", "config"])
        assert ret == 0

    def test_005_svc_print_config_json(self):
        """
        Print all services config (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = commands.svc.main(argv=["-s", "*", "print", "config", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        print(output)
        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    def test_006_svc_print_status(self):
        """
        Print all services status
        """
        ret = commands.svc.main(argv=["-s", "*", "print", "status"])
        assert ret == 0

    def test_007_svc_print_status_json(self):
        """
        Print all services status (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = commands.svc.main(argv=["-s", "*", "print", "status", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        print(repr(output))
        data = json.loads(output)

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    def test_0081_set_default(self):
        """
        Set DEFAULT.comment to an unicode string
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "set", "--param", "comment", "--value", UNICODE_STRING])
        assert ret == 0

    def test_0082_get_default(self):
        """
        Get DEFAULT.comment
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = commands.svc.main(argv=["-s", SVCNAME, "get", "--param", "comment"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout
        assert ret == 0
        assert try_decode(output) == UNICODE_STRING

    def test_0083_set_env(self):
        """
        Set env.list_entry_ref_indirect_eval2 to {nodes[$(0//(3//{#nodes}))]}
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "set", "--param", "env.list_entry_ref_indirect_eval2", "--value", "{nodes[$(0//(3//{#nodes}))]}"])
        assert ret == 0

    def test_0084_get_env(self):
        """
        Get evaluated env.list_entry_ref_indirect_eval2
        """
        nodename = socket.gethostname().lower()
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = commands.svc.main(argv=["-s", SVCNAME, "get", "--param", "env.list_entry_ref_indirect_eval2", "--eval"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        assert ret == 0
        assert output == nodename

    def test_010_unset_default(self):
        """
        Unset DEFAULT.comment
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "unset", "--param", "comment"])
        assert ret == 0

    def test_011_get_default_not_found(self):
        """
        Get unset keyword
        """
        _stderr = sys.stderr

        try:
            err = StringIO()
            sys.stderr = err
            ret = commands.svc.main(argv=["-s", SVCNAME, "get", "--param", "comment"])
            output = err.getvalue().strip()
        finally:
            sys.stderr = _stderr

        assert ret == 1

    def test_013_validate_config(self):
        """
        Validate config
        """
        ret = commands.svc.main(argv=["validate", "config", "-s", SVCNAME])
        assert ret == 0

    def test_014_frozen(self):
        """
        The service is frozen after create.
        """
        ret = commands.svc.main(argv=["frozen", "-s", SVCNAME])
        assert ret > 1
        ret = commands.svc.main(argv=["thaw", "-s", SVCNAME, "--local"])
        assert ret == 0

    def test_0151_node_freeze(self):
        """
        Freeze the local node
        """
        ret = commands.node.main(argv=["freeze", "--local"])
        assert ret == 0

    def test_0152_node_frozen(self):
        """
        The local node is frozen
        """
        ret = commands.node.main(argv=["frozen"])
        assert ret > 0

    def test_0153_svc_not_frozen(self):
        """
        The service is not frozen
        """
        ret = commands.svc.main(argv=["frozen", "-s", SVCNAME])
        assert ret == 0

    def test_0154_node_refreeze(self):
        """
        Re-freeze the local node (valid noop)
        """
        ret = commands.node.main(argv=["freeze", "--local"])
        assert ret == 0

    def test_0155_node_thaw(self):
        """
        Thaw the local node
        """
        ret = commands.node.main(argv=["thaw", "--local"])
        assert ret == 0

    def test_0156_node_frozen(self):
        """
        The local node is no longer frozen
        """
        ret = commands.node.main(argv=["frozen"])
        assert ret == 0

    def test_0157_svc_frozen(self):
        """
        The service is still not frozen
        """
        ret = commands.svc.main(argv=["frozen", "-s", SVCNAME])
        assert ret == 0

    def test_0158_node_rethaw(self):
        """
        Re-thaw the local node (valid noop)
        """
        ret = commands.node.main(argv=["thaw", "--local"])
        assert ret == 0

    def test_01611_freeze(self):
        """
        Freeze the service
        """
        ret = commands.svc.main(argv=["freeze", "-s", SVCNAME, "--local"])
        assert ret == 0

    def test_01612_freeze(self):
        """
        Freeze the services (parallel)
        """
        ret = commands.svc.main(argv=["freeze", "-s", "unittest*", "--parallel", "--local"])
        assert ret == 0

    def test_01613_freeze(self):
        """
        Freeze the services (serial)
        """
        ret = commands.svc.main(argv=["freeze", "-s", "unittest*", "--local"])
        assert ret == 0

    def test_0162_frozen(self):
        """
        The service is frozen
        """
        ret = commands.svc.main(argv=["frozen", "-s", SVCNAME])
        assert ret > 0

    def test_0163_thaw(self):
        """
        Thaw the service
        """
        ret = commands.svc.main(argv=["thaw", "-s", SVCNAME, "--local"])
        assert ret == 0

    def test_0164_frozen(self):
        """
        The service is no longer frozen
        """
        ret = commands.svc.main(argv=["frozen", "-s", SVCNAME])
        assert ret == 0

    def test_0165_multi_get(self):
        """
        Multi-service get
        """
        ret = commands.svc.main(argv=["get", "-s", "unittest*", "--param", "nodes", "--eval"])
        assert ret == 0

    def test_021_logs(self):
        """
        Print service logs
        """
        ret = commands.svc.main(argv=["logs", "-s", SVCNAME, "--no-pager"])
        assert ret == 0

    def test_022_push(self):
        """
        Push service to the collector
        """
        ret = commands.svc.main(argv=["push", "-s", SVCNAME])
        assert ret == 0

    def test_023_pull(self):
        """
        Pull the service from the collector
        """
        ret = commands.svc.main(argv=["pull", "-s", SVCNAME])
        assert ret == 0

    def test_0241_svc_selector(self):
        """
        Service selector: <none>
        """
        ret = commands.svc.main(argv=["ls"])
        assert ret == 0

    def test_0242_svc_selector(self):
        """
        Service selector: uni*
        """
        ret = commands.svc.main(argv=["ls", "-s", "uni*"])
        assert ret == 0

    def test_0243_svc_selector(self):
        """
        Service selector: notexists*
        """
        ret = commands.svc.main(argv=["ls", "-s", "notexists*"])
        assert ret == 0

    def test_0244_svc_selector(self):
        """
        Service selector: *dns,ha*+app.timeout>1*
        """
        ret = commands.svc.main(argv=["ls", "-s", "*dns,ha*+app.timeout>1*"])
        assert ret == 0

    def test_0245_svc_selector(self):
        """
        Service selector: ip:+task:
        """
        ret = commands.svc.main(argv=["ls", "-s", "ip:+task:"])
        assert ret == 0

    def test_0246_svc_selector(self):
        """
        Service selector: !*excluded
        """
        ret = commands.svc.main(argv=["ls", "-s", "!*excluded"])
        assert ret == 0

    def test_0247_svc_selector(self):
        """
        Service selector: notexists
        """
        ret = commands.svc.main(argv=["ls", "-s", "notexists"])
        assert ret == 1

    def test_0248_svc_selector(self):
        """
        Service selector: OSVC_SERVICE_LINK=unittest
        """
        os.environ["OSVC_SERVICE_LINK"] = SVCNAME
        ret = commands.svc.main(argv=["ls"])
        assert ret == 0
        del os.environ["OSVC_SERVICE_LINK"]

    def test_0251_compliance(self):
        """
        Service compliance auto
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "auto"])
        assert ret == 0

    def test_0252_compliance(self):
        """
        Service compliance check
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "check"])
        assert ret == 0

    def test_0253_compliance(self):
        """
        Service compliance fix
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "fix"])
        assert ret == 0

    def test_0254_compliance(self):
        """
        Service compliance show moduleset
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "show", "moduleset"])
        assert ret == 0

    def test_0255_compliance(self):
        """
        Service compliance list moduleset
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "list", "moduleset"])
        assert ret == 0

    def test_0256_compliance(self):
        """
        Service compliance show ruleset
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "show", "ruleset"])
        assert ret == 0

    def test_0257_compliance(self):
        """
        Service compliance list ruleset
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "list", "ruleset"])
        assert ret == 0

    def test_0258_compliance(self):
        """
        Service compliance attach
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "attach", "--ruleset", "abcdef", "--moduleset", "abcdef"])
        assert ret == 1

    def test_0259_compliance(self):
        """
        Service compliance detach
        """
        ret = commands.svc.main(argv=["-s", SVCNAME, "compliance", "detach", "--ruleset", "abcdef", "--moduleset", "abcdef"])
        assert ret == 0

    def test_026_delete(self):
        """
        Delete local service instance
        """
        ret = commands.svc.main(argv=["delete", "-s", SVCNAME, "--local"])
        assert ret == 0
        ret = commands.svc.main(argv=["delete", "-s", SVCNAME + '2', "--local"])
        assert ret == 0


