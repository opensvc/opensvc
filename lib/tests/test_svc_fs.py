# coding: utf8

from __future__ import print_function

import sys
import os
mod_d = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, mod_d)

import platform
from nose.plugins.skip import Skip, SkipTest

import svc
import resFsLinux
import rcExceptions as ex
import rcLogger
import uuid

SVCNAME = "unittest-" + str(uuid.uuid4())

if platform.uname()[0] != "Linux":
    raise SkipTest

class TestSvc:
    def tearDown(self):
        if self.svc.node:
            self.svc.node.close()

    def setUp(self):
        rcLogger.DEFAULT_HANDLERS = ["file"]
        self.svc = svc.Svc(SVCNAME)
        r = resFsLinux.Mount(rid="fs#1",
                             mount_point="/srv/"+SVCNAME,
                             device="/tmp",
                             mount_options="bind,rw",
                             fs_type="none")
        self.svc += r

    def test_002_start(self):
        """
        Svc::action("start")
        """
        ret = self.svc.action("start", {"local": True})
        assert ret == 0
        assert self.svc.get_resource("fs#1").is_up() == True

    def test_003_restart(self):
        """
        Svc::action("restart")
        """
        ret = self.svc.action("restart", {"local": True})
        assert ret == 0

    def test_004_action_on_wrong_rid(self):
        """
        Svc::action("start"), rid scoped
        """
        ret = self.svc.action("start", {"rid": "fs#2", "local": True})
        assert ret == 1

    def test_005_update(self):
        """
        Svc::action("update"), from json
        """
        ret = self.svc.action("update", {
            "resource": ['{"rtype": "fs", "mnt": "/srv/{svcname}/foo", "dev": "/tmp", "type": "none", "mnt_opt": "bind"}'],
            "provision": True,
        })
        assert ret == 0

    def test_006_update(self):
        """
        Svc::action("update"), from dict
        """
        ret = self.svc.action("update", {
            "resource": [{"rtype": "fs", "mnt": "/srv/{svcname}/bar", "dev": "/tmp", "type": "none", "mnt_opt": "bind"}],
            "provision": True,
        })
        assert ret == 0

    def test_007_start(self):
        """
        Svc::action("start")
        """
        ret = self.svc.action("start", {"local": True})
        assert ret == 0

    def test_008_stop(self):
        """
        Svc::action("stop")
        """
        ret = self.svc.action("stop", {"local": True})
        assert ret == 0

    def test_009_delete_rid_unprovision(self):
        """
        Svc::action("delete"), rid scoped, unprovision
        """
        ret = self.svc.action("delete", {"rid": "fs#1", "unprovision": True})
        assert ret == 0

    def test_010_push(self):
        """
        Svc::action("push_config")
        """
        ret = self.svc.action("push_config")
        assert ret == 0

    def test_011_delete_unprovision(self):
        """
        Svc::action("delete"), unprovision
        """
        ret = self.svc.action("delete", {"unprovision": True, "local": True})
        assert ret == 0

    def test_012_pull_provision(self):
        """
        Svc::action("pull"), provision
        """
        ret = self.svc.action("pull", {"provision": True, "local": True})
        assert ret == 0

    def test_13_delete_unprovision(self):
        """
        Svc::action("delete"), unprovision
        """
        ret = self.svc.action("delete", {"unprovision": True, "local": True})
        assert ret == 0

