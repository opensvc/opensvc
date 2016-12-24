# coding: utf8

from __future__ import print_function

import platform
from nose.plugins.skip import Skip, SkipTest

import svc
import resFsLinux
import rcExceptions as ex
import rcLogger

SVCNAME = "unittest"

if platform.uname()[0] != "Linux":
    raise SkipTest

class TestSvc:
    def tearDown(self):
        if self.svc.node:
            self.svc.node.close()

    def setUp(self):
        rcLogger.DEFAULT_HANDLERS = []
        self.svc = svc.Svc(SVCNAME)
        r = resFsLinux.Mount("fs#1",
                             mount_point="/srv/"+SVCNAME,
                             device="/tmp",
                             mount_options="bind,rw",
                             fs_type="none")
        self.svc += r

    def test_002_start(self):
        ret = self.svc.action("start")
        assert ret == 0

    def test_003_restart(self):
        ret = self.svc.action("restart")
        assert ret == 0

    def test_004_action_on_wrong_rid(self):
        try:
            self.svc.action("start", {"rid": "fs#2"})
            # shouldn't reach here, fs#2 doesn't exist
            assert False
        except ex.excError:
            assert True

    def test_005_update(self):
        ret = self.svc.action("update", {
            "resource": ['{"rtype": "fs", "mnt": "/srv/{svcname}/foo", "dev": "/tmp", "type": "none", "mnt_opt": "bind"}'],
            "provision": True,
        })
        assert ret == 0

    def test_006_update(self):
        ret = self.svc.action("update", {
            "resource": [{"rtype": "fs", "mnt": "/srv/{svcname}/foo", "dev": "/tmp", "type": "none", "mnt_opt": "bind"}],
            "provision": True,
        })
        assert ret == 0

    def test_007_start(self):
        ret = self.svc.action("start")
        assert ret == 0

    def test_008_stop(self):
        ret = self.svc.action("stop")
        assert ret == 0

    def test_009_delete_rid_unprovision(self):
        ret = self.svc.action("delete", {"rid": "fs#1", "unprovision": True})
        assert ret == 0

    def test_010_delete_unprovision(self):
        ret = self.svc.action("delete", {"unprovision": True})
        assert ret == 0

    def test_011_pull_provision(self):
        ret = self.svc.action("pull", {"provision": True})
        assert ret == 0

