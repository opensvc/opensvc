# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import logging
import uuid

import pytest

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from commands.svcmgr import Mgr
import commands.mgr


@pytest.fixture(scope='function')
def has_privs(mocker):
    mocker.patch.object(commands.mgr, 'check_privs')


logging.disable(logging.CRITICAL)

SVCNAME = "unittest-" + str(uuid.uuid4())


@pytest.fixture(scope='function')
def has_svc():
    assert Mgr()(argv=["-s", SVCNAME, "create"]) == 0


@pytest.mark.linux
@pytest.mark.usefixtures('osvc_path_tests', 'has_privs', 'has_svc')
@pytest.mark.parametrize('type,unprovision_rid,properties',
                         [
                             ['loop', 'disk#0', [
                                 "--kw", "disk#0.type=loop",
                                 "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                 "--kw", "disk#0.size=10m"
                             ]],

                             ['vg', 'disk#0,disk#1',[
                                 "--kw", "disk#0.type=loop",
                                 "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                 "--kw", "disk#0.size=10m",
                                 "--kw", "disk#1.type=vg",
                                 "--kw", "disk#1.name={svcname}",
                                 "--kw", "disk#1.pvs={disk#0.file}",
                             ]],

                             ['lv', 'disk#0,disk#1,disk#2', [
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
                             ]],

                             ['md', 'disk#0,disk#1,disk#2', [
                                 "--kw", "disk#0.type=loop",
                                 "--kw", "disk#0.file=/var/tmp/{svcname}.1.dd",
                                 "--kw", "disk#0.size=10m",
                                 "--kw", "disk#1.type=loop",
                                 "--kw", "disk#1.file=/var/tmp/{svcname}.2.dd",
                                 "--kw", "disk#1.size=10m",
                                 "--kw", "disk#2.type=md",
                                 "--kw", "disk#2.level=raid0",
                                 "--kw", "disk#2.devs={disk#0.exposed_devs[0]} {disk#1.exposed_devs[0]}",
                             ]],

                             ['ext4', 'disk#0,fs#0', [
                                 "--kw", "disk#0.type=loop",
                                 "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                 "--kw", "disk#0.size=10m",
                                 "--kw", "fs#0.type=ext4",
                                 "--kw", "fs#0.mkfs_opt=-L {svcname}.fs.0",
                                 "--kw", "fs#0.dev={disk#0.file}",
                                 "--kw", "fs#0.mnt=/var/tmp/{svcname}",
                                 "--kw", "fs#0.mnt_opt=rw,noatime",
                             ]],
                             ['btrfs', 'disk#0,fs#0', [
                                 "--kw", "disk#0.type=loop",
                                 "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
                                 "--kw", "disk#0.size=1g",
                                 "--kw", "fs#0.type=btrfs",
                                 "--kw", "fs#0.dev={disk#0.file}",
                                 "--kw", "fs#0.mnt=/var/tmp/{svcname}",
                                 "--kw", "fs#0.mnt_opt=rw,noatime,subvol=init",
                             ]],

                             ['container.docker shared', 'container#0', [
                                 "--kw", "docker_daemon_private=false",
                                 "--kw", "container#0.type=docker",
                                 "--kw", "container#0.image=alpine:latest",
                                 "--kw", "container#0.run_args=-it --net=none",
                                 "--kw", "container#0.run_command=/bin/sh",
                             ]],
                             ['ip', 'ip#0', [
                                 "--kw", "ip#0.provisioner=collector",
                                 "--kw", "ip#0.ipdev=lo",
                                 "--kw", "ip#0.network=192.168.0.0",
                             ]],
                             ['ip.docker', 'container#0,ip#0', [
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
                             ]],
])
class TestProvisionUnprovision:
    @staticmethod
    def test_disk_loop(type, unprovision_rid,properties):
        print('\nls...')
        assert Mgr()(argv=["-s", SVCNAME, "ls"]) == 0
        print('\nprint config...')
        assert Mgr()(argv=["-s", SVCNAME, "print", "config"]) == 0
        print('\nset properties: ', properties)
        assert Mgr()(argv=["-s", SVCNAME, "set"] + properties) == 0
        print('\nprint config...')
        assert Mgr()(argv=["-s", SVCNAME, "print", "config"]) == 0
        print('\nprovision --local...')
        assert Mgr()(argv=["-s", SVCNAME, "provision", "--local"]) == 0
        print('\nprint resinfo...')
        assert Mgr()(argv=["-s", SVCNAME, "print", "resinfo"]) == 0
        print('\nprint status -r...')
        assert Mgr()(argv=["-s", SVCNAME, "print", "status", "-r"]) == 0
        print('\nprint devs...')
        assert Mgr()(argv=["-s", SVCNAME, "print", "devs"]) == 0
        # print('\ndelete --unprovision --rid ', unprovision_rid, '...')
        # assert Mgr()(argv=["-s", SVCNAME, "delete", '--unprovision', '--rid', unprovision_rid]) == 0
