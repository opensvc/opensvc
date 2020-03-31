import os

import pytest

from commands.svc import Mgr

scenarios = [
    ['loop', 'disk#0', [
        "--kw", "disk#0.type=loop",
        "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
        "--kw", "disk#0.size=10m"
    ]],

    ['vg', 'disk#0,disk#1', [
        "--kw", "disk#0.type=loop",
        "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
        "--kw", "disk#0.size=10m",
        "--kw", "disk#1.type=vg",
        "--kw", "disk#1.name={svcname}",
        "--kw", "disk#1.pvs={disk#0.file}",
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

    ['container-docker-shared', 'container#0', [
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

]

skipped_scenarios = [
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
    ['btrfs', 'disk#0,fs#0', [
        "--kw", "disk#0.type=loop",
        "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
        "--kw", "disk#0.size=1g",
        "--kw", "fs#0.type=btrfs",
        "--kw", "fs#0.dev={disk#0.file}",
        "--kw", "fs#0.mnt=/var/tmp/{svcname}",
        "--kw", "fs#0.mnt_opt=rw,noatime,subvol=init",
    ]],

    ['ip-docker', 'container#0,ip#0', [
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
]


@pytest.mark.linux
@pytest.mark.slow
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('name,unprovision_rid,properties', scenarios)
class TestProvision:
    @staticmethod
    def test_create_provision_delete_unprovision(mocker, name, unprovision_rid, properties):
        svcname = "pytest-" + name
        mocker.patch.dict(os.environ, {'OSVC_DETACHED': '1'})
        for args in [
            ['create', '--debug'],
            ['set'] + properties,
            ['provision', '--local', '--debug'],
            ['print', 'status', '-r', '--debug'],
            ['print', 'config', '--debug'],
            ['print', 'resinfo', '--debug'],
            ['print', 'devs', '--debug'],
            ['delete', '--unprovision', '--rid', unprovision_rid, '--debug']
        ]:
            cmd_args = ["-s", svcname] + args
            print('run Mgr()(argv=%s)' % cmd_args)
            assert Mgr()(argv=cmd_args) == 0
