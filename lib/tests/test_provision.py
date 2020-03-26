# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import logging
import uuid

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import svcmgr

logging.disable(logging.CRITICAL)

SVCNAME = "unittest-" + str(uuid.uuid4())


class Test:

    @classmethod
    def setup_class(cls):
        ret = svcmgr.main(argv=["-s", SVCNAME, "create"])
        assert ret == 0

    @classmethod
    def teardown_class(cls):
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--local"])
        assert ret == 0

    def test_001(self):
        """
        Provision, disk.loop
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
                                "--kw", "disk#0.type=loop",
                                "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                "--kw", "disk#0.size=10m",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_002(self):
        """
        Unprovision, disk.loop
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "disk#0"])
        assert ret == 0

    def test_011(self):
        """
        Provision, disk.vg
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
                                "--kw", "disk#0.type=loop",
                                "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                "--kw", "disk#0.size=10m",
                                "--kw", "disk#1.type=vg",
                                "--kw", "disk#1.name={svcname}",
                                "--kw", "disk#1.pvs={disk#0.file}",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_012(self):
        """
        Unprovision, disk.vg
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "disk#0,disk#1"])
        assert ret == 0

    def test_021(self):
        """
        Provision, disk.lv
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
                                "--kw", "disk#0.type=loop",
                                "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                "--kw", "disk#0.size=10m",
                                "--kw", "disk#1.type=vg",
                                "--kw", "disk#1.name={svcname}",
                                "--kw", "disk#1.pvs={disk#0.file}",
                                "--kw", "disk#2.type=lv",
                                "--kw", "disk#2.name=init",
                                "--kw", "disk#2.vg={disk#1.name}",
                                "--kw", "disk#2.size=100%FREE",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_022(self):
        """
        Unprovision, disk.lv
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "disk#0,disk#1,disk#2"])
        assert ret == 0

    def test_031(self):
        """
        Provision, disk.md
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
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
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_032(self):
        """
        Unprovision, disk.md
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "disk#0,disk#1,disk#2"])
        assert ret == 0

    def test_121(self):
        """
        Provision, fs.ext4
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
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
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_122(self):
        """
        Unprovision, fs.ext4
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "disk#0,fs#0"])
        assert ret == 0

    def test_131(self):
        """
        Provision, fs.btrfs
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
                                "--kw", "disk#0.type=loop",
                                "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                "--kw", "disk#0.size=1g",
                                "--kw", "fs#0.type=btrfs",
                                "--kw", "fs#0.dev={disk#0.file}",
                                "--kw", "fs#0.mnt=/var/tmp/{svcname}",
                                "--kw", "fs#0.mnt_opt=rw,noatime,subvol=init",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_132(self):
        """
        Unprovision, fs.btrfs
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "disk#0,fs#0"])
        assert ret == 0

    def test_201(self):
        """
        Provision, container.docker (shared)
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
                                "--kw", "docker_daemon_private=false",
                                "--kw", "container#0.type=docker",
                                "--kw", "container#0.image=alpine:latest",
                                "--kw", "container#0.run_args=-it --net=none",
                                "--kw", "container#0.run_command=/bin/sh",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_202(self):
        """
        Unprovision, container.docker (shared)
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "container#0"])
        assert ret == 0

    def test_301(self):
        """
        Provision, ip
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
                                "--kw", "ip#0.provisioner=collector",
                                "--kw", "ip#0.ipdev=lo",
                                "--kw", "ip#0.network=192.168.0.0",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_302(self):
        """
        Unprovision, ip
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "ip#0"])
        assert ret == 0

    def test_311(self):
        """
        Provision, ip.docker
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "set",
                                "--kw", "docker_daemon_private=false",
                                "--kw", "container#0.type=docker",
                                "--kw", "container#0.image=alpine:latest",
                                "--kw", "container#0.run_args=-it --net=none",
                                "--kw", "container#0.run_command=/bin/sh",
                                "--kw", "ip#0.type=docker",
                                "--kw", "ip#0.ipname=172.17.172.17",
                                "--kw", "ip#0.ipdev=docker0",
                                "--kw", "ip#0.network=172.17.0.0",
                                "--kw", "ip#0.netmask=16",
                                "--kw", "ip#0.container_rid=container#0",
                               ])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "provision", "--local"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "resinfo"])
        assert ret == 0
        ret = svcmgr.main(argv=["-s", SVCNAME, "print", "devs"])
        assert ret == 0

    def test_312(self):
        """
        Unprovision, ip.docker
        """
        ret = svcmgr.main(argv=["-s", SVCNAME, "delete", "--unprovision", "--rid", "container#0,ip#0"])
        assert ret == 0


