# coding: utf8

from __future__ import print_function

import platform
import time
from nose.plugins.skip import Skip, SkipTest
from multiprocessing import Process

import svc
import resFsLinux
import rcExceptions as ex
import rcLogger
import rcStatus

SVCNAME = "unittest"

if platform.uname()[0] != "Linux":
    raise SkipTest

class Mount(resFsLinux.Mount):
    def stop(self):
        resFsLinux.Mount.stop(self)
        time.sleep(2)

class TestSvc:
    def tearDown(self):
        self.svc.action("stop")
        if self.svc.node:
            self.svc.node.close()

    def setUp(self):
        rcLogger.DEFAULT_HANDLERS = []
        self.svc = svc.Svc(SVCNAME)
        r = resFsLinux.Mount("fs#1",
                             mount_point="/srv/"+SVCNAME,
                             device="/tmp",
                             mount_options="bind,rw",
                             fs_type="none", restart=1)
        self.svc += r
        self.svc.action("start")

    def test_001_resource_autorestart(self):
        self.svc.vcall(["umount", "/srv/"+SVCNAME])
        assert self.svc.get_resources()[0].status(refresh=True) == rcStatus.UP

    def test_002_resource_monitor_during_stop(self):
        """
        A resource monitor action during a stop is blocked by the action lock.
        A status() call after a succesful stop does not restart resources.
        """
        _svc = svc.Svc(SVCNAME)
        r = Mount("fs#1",
                   mount_point="/srv/"+SVCNAME,
                   device="/tmp",
                   mount_options="bind,rw",
                   fs_type="none", restart=1)
        _svc += r
        _svc.action("start")

        def worker(_svc):
            _svc.action("stop")
            _svc.node.close()
            
        proc = Process(
            target=worker,
            args=[_svc],
            name='worker_'+_svc.svcname,
        )
        proc.start()
        time.sleep(0.2)
        ret1 = _svc.action("resource_monitor", {"waitlock": 0})
        ret2 = _svc.action("status", {"waitlock": 0, "refresh": True})
        proc.join()
        assert ret1 == 1
        assert ret2 == 0
        try:
            assert r.status(refresh=True) == rcStatus.DOWN
        finally:
            _svc.node.close()


