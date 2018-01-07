# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import json
import socket
from StringIO import StringIO
from rcUtilities import try_decode

import svcmgr
import nodemgr

class TestSvcmgr:

    @classmethod
    def setup_class(cls):
        ret = svcmgr.main(argv=["-s", "unittest", "create"])
        assert ret == 0

    @classmethod
    def teardown_class(cls):
        ret = svcmgr.main(argv=["-s", "unittest", "delete", "--local"])
        assert ret == 0

    def test_001(self):
        """
        Provision, disk.loop
        """
        ret = svcmgr.main(argv=["-s", "unittest", "set",
                                "--kw", "disk#0.type=loop",
                                "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                "--kw", "disk#0.size=10m",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", "unittest", "provision", "--local"])
        assert ret == 0

    def test_002(self):
        """
        Unprovision, disk.loop
        """
        ret = svcmgr.main(argv=["-s", "unittest", "delete", "--unprovision", "--rid", "disk#0"])
        assert ret == 0

    def test_011(self):
        """
        Provision, disk.vg
        """
        ret = svcmgr.main(argv=["-s", "unittest", "set",
                                "--kw", "disk#0.type=loop",
                                "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                "--kw", "disk#0.size=10m",
                                "--kw", "disk#1.type=vg",
                                "--kw", "disk#1.name={svcname}",
                                "--kw", "disk#1.pvs={disk#0.file}",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", "unittest", "provision", "--local"])
        assert ret == 0

    def test_012(self):
        """
        Unprovision, disk.loop
        """
        ret = svcmgr.main(argv=["-s", "unittest", "unprovision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", "unittest", "delete", "--unprovision", "--rid", "disk#0,disk#1"])
        assert ret == 0

    def test_021(self):
        """
        Provision, fs.ext4
        """
        ret = svcmgr.main(argv=["-s", "unittest", "set",
                                "--kw", "disk#0.type=loop",
                                "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                "--kw", "disk#0.size=10m",
                                "--kw", "fs#0.type=ext4",
                                "--kw", "fs#0.mkfs_opt=-L {svcname}.fs.0",
                                "--kw", "fs#0.dev={disk#0.file}",
                                "--kw", "fs#0.mnt=/var/tmp/{svcname}",
                                "--kw", "fs#0.mnt_opt=rw,noatime",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", "unittest", "provision", "--local"])
        assert ret == 0

    def test_022(self):
        """
        Unprovision, fs.ext4
        """
        ret = svcmgr.main(argv=["-s", "unittest", "unprovision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", "unittest", "delete", "--unprovision", "--rid", "disk#0,fs#0"])
        assert ret == 0

    def test_031(self):
        """
        Provision, disk.md
        """
        ret = svcmgr.main(argv=["-s", "unittest", "set",
                                "--kw", "disk#0.type=loop",
                                "--kw", "disk#0.file=/var/tmp/{svcname}.1.dd",
                                "--kw", "disk#0.size=10m",
                                "--kw", "disk#1.type=loop",
                                "--kw", "disk#1.file=/var/tmp/{svcname}.2.dd",
                                "--kw", "disk#1.size=10m",
                                "--kw", "disk#2.type=md",
                                "--kw", "disk#2.level=raid0",
                                "--kw", "disk#2.devs={disk#0.exposed_devs[0]} {disk#1.exposed_devs[0]}",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", "unittest", "provision", "--local"])
        assert ret == 0

    def test_032(self):
        """
        Unprovision, disk.md
        """
        ret = svcmgr.main(argv=["-s", "unittest", "unprovision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", "unittest", "delete", "--unprovision", "--rid", "disk#0,disk#1,disk#2"])
        assert ret == 0


