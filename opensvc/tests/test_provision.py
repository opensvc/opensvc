import json
import os
import platform

import pytest

from commands.svc import Mgr

scenarios = {
    'linux': [
        ['loop-standby-converters', 'test-start', ['--rid', 'disk#0,disk#1,disk#2'], [
            "--kw", "disk#0.type=loop",
            "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
            "--kw", "disk#0.size=10m",

            "--kw", "disk#1.type=loop",
            "--kw", "disk#1.file=/var/tmp/{svcname}.dd",
            "--kw", "disk#1.size=5mib",  # with mbi converter
            "--kw", "disk#1.standby=true",

            "--kw", "disk#2.type=loop",
            "--kw", "disk#2.file=/var/tmp/{svcname}.dd",
            "--kw", "disk#2.size=4000kib",  # with kbi converter
        ]],

        ['lvm-unprovision-rid-disk', 'test-start', ['--rid', 'disk'], [
            "--kw", "disk#loop0.type=loop",
            "--kw", "disk#loop0.file=/var/tmp/{svcname}.{rid}",
            "--kw", "disk#loop0.size=10m",

            "--kw", "disk#loop1.type=loop",
            "--kw", "disk#loop1.file=/var/tmp/{svcname}.{rid}",
            "--kw", "disk#loop1.size=10m",

            "--kw", "disk#lvm.type=lvm",
            "--kw", "disk#lvm.vgname={svcname}",
            "--kw", "disk#lvm.pvs={disk#loop0.exposed_devs[0]} {disk#loop1.exposed_devs[0]}",

            "--kw", "disk#vol0.type=lv",
            "--kw", "disk#vol0.name=vol0",
            "--kw", "disk#vol0.vg={disk#lvm.name}",
            "--kw", "disk#vol0.size=100%FREE",
        ]],

        ['vg-with-2-disks-subset', 'test-start', ['--subset', 'g1'], [
            "--kw", "disk#loop0.subset=g1",
            "--kw", "disk#loop0.type=loop",
            "--kw", "disk#loop0.file=/var/tmp/{svcname}.{rid}",
            "--kw", "disk#loop0.size=10m",
            "--kw", "disk#loop1.subset=g1",
            "--kw", "disk#loop1.type=loop",
            "--kw", "disk#loop1.file=/var/tmp/{svcname}.{rid}",
            "--kw", "disk#loop1.size=10m",
            "--kw", "disk#vg.subset=g1",
            "--kw", "disk#vg.type=vg",
            "--kw", "disk#vg.name={svcname}",
            "--kw", "disk#vg.pvs={disk#loop0.file} {disk#loop1.file}",
        ]],

        ['raw', 'test-start', ['--rid', 'disk'], [
            "--kw", "disk#loop0.type=loop",
            "--kw", "disk#loop0.file=/var/tmp/{svcname}.{rid}",
            "--kw", "disk#loop0.size=10m",
            "--kw", "disk#loop1.type=loop",
            "--kw", "disk#loop1.file=/var/tmp/{svcname}.{rid}",
            "--kw", "disk#loop1.size=10m",
            "--kw", "disk#raw.type=raw",
            "--kw", "disk#raw.user=root",
            "--kw", "disk#raw.group=root",
            "--kw", "disk#raw.create_char_devices=true",
            "--kw", "disk#raw.perm=640",
            "--kw",
            "disk#raw.devs={disk#loop0.exposed_devs[0]}:/tmp/raw/raw00 {disk#loop1.exposed_devs[0]}:/tmp/raw/raw01",
        ]],

        ['md-raid0-all', 'test-start', [], [
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

        ['md-raid1', 'test-start', ['--rid', 'disk'], [
            "--kw", "disk#0.type=loop",
            "--kw", "disk#0.file=/var/tmp/{svcname}.1.dd",
            "--kw", "disk#0.size=10m",
            "--kw", "disk#1.type=loop",
            "--kw", "disk#1.file=/var/tmp/{svcname}.2.dd",
            "--kw", "disk#1.size=10m",
            "--kw", "disk#2.type=md",
            "--kw", "disk#2.level=raid1",
            "--kw", "disk#2.devs={disk#0.exposed_devs[0]} {disk#1.exposed_devs[0]}",
        ]],

        ['ext3-ext4', 'test-start', ['--rid', 'disk,fs'], [
            "--kw", "disk#ext3.type=loop",
            "--kw", "disk#ext3.file=/var/tmp/{svcname}.{rid}",
            "--kw", "disk#ext3.size=10m",
            "--kw", "disk#ext4.type=loop",
            "--kw", "disk#ext4.file=/var/tmp/{svcname}.{rid}",
            "--kw", "disk#ext4.size=10m",
            "--kw", "fs#ext3.type=ext3",
            "--kw", "fs#ext3.dev={disk#ext3.exposed_devs[0]}",
            "--kw", "fs#ext3.mnt=/var/tmp/{svcname}-{rid}",
            "--kw", "fs#ext4.type=ext4",
            "--kw", "fs#ext3.mnt_opt=rw,noatime",
            "--kw", "fs#ext4.dev={disk#ext4.exposed_devs[0]}",
            "--kw", "fs#ext4.mnt=/var/tmp/{svcname}-{rid}",
            "--kw", "fs#ext4.mnt_opt=rw,noatime",
        ]],

        ['xfs-unprovision-all', 'test-start', [], [
            "--kw", "disk#0.type=loop",
            "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
            "--kw", "disk#0.size=20m",
            "--kw", "fs#0.type=xfs",
            "--kw", "fs#0.dev={disk#0.exposed_devs[0]}",
            "--kw", "fs#0.mnt=/var/tmp/{svcname}-{rid}",
            "--kw", "fs#0.mnt_opt=rw,noatime",
        ]],

        ['container-docker-shared', 'test-start', ['--rid', 'container#0'], [
            "--kw", "docker_daemon_private=false",
            "--kw", "container#0.type=docker",
            "--kw", "container#0.image=alpine:latest",
            "--kw", "container#0.run_args=-it --net=none",
            "--kw", "container#0.run_command=/bin/sh",
        ]],

        ['ip', 'nothing', ['--rid', 'ip#0'], [
            "--kw", "ip#0.provisioner=collector",
            "--kw", "ip#0.ipdev=lo",
            "--kw", "ip#0.network=192.168.0.0",
        ]],
    ],
    'sunos': [
        ['loop-converters', 'test-start', ['--rid', 'disk#0'], [
            "--kw", "disk#0.type=loop",
            "--kw", "disk#0.file=/var/tmp/{svcname}.dd",
            "--kw", "disk#0.size=10m",
        ]]
    ]
}


@pytest.mark.linux
@pytest.mark.sunos
@pytest.mark.slow
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('name, extra_test, unprovision_args, properties', scenarios.get(platform.system().lower(), []))
class TestProvision:
    @staticmethod
    def test_service_lifecycle(mocker, capture_stdout, tmp_file, name, extra_test, unprovision_args, properties):
        """
        Test service provisioning lifecycle
        o create service
        o provision service
        o start service (ensure up)
        o unprovision (ensure down)
        o delete --unprovision
        """
        svcname = "pytest-" + name
        mocker.patch.dict(os.environ, {'OSVC_DETACHED': '1'})

        def assert_run_cmd_success(svc_cmd_args):
            cmd_args = ["-s", svcname] + svc_cmd_args
            print('--------------')
            print('run Mgr()(argv=%s)' % cmd_args)
            assert Mgr()(argv=cmd_args) == 0

        def assert_service_avail(value):
            cmd_args = ["-s", svcname, 'print', 'status', '-r', '--format', 'json']
            print('run Mgr()(argv=%s)' % cmd_args)
            with capture_stdout(tmp_file):
                assert Mgr()(argv=cmd_args) == 0
            with open(tmp_file, 'r') as status_file:
                status = json.load(status_file)
            print(json.dumps(status, indent=4))
            if value is 'down' and 'standby' in svcname:
                value = 'stdby ' + value
            print('assert service avail is ', value)
            assert status['avail'] == value

        def show_svc_info():
            group_cmds = [
                ['print', 'status', '-r', '--debug'],
                ['print', 'resinfo', '--debug'],
                ['print', 'devs', '--debug'],
            ]
            for args in group_cmds:
                assert_run_cmd_success(args)

        assert_run_cmd_success(['create', '--debug'] + properties)
        assert_run_cmd_success(['unprovision', '--local'])  # clean previous test
        assert_run_cmd_success(['print', 'config', '--format', 'json'])
        assert_run_cmd_success(['print', 'config'])
        assert_service_avail('down')
        show_svc_info()
        assert_run_cmd_success(['provision', '--local', '--debug'])
        show_svc_info()
        if extra_test == 'test-start':
            assert_run_cmd_success(['start', '--local', '--debug'])
            assert_service_avail('up')
        assert_run_cmd_success(['unprovision', '--local', '--debug'] + unprovision_args)
        assert_service_avail('down')
        assert_run_cmd_success(['delete', '--unprovision', '--debug', '--local'] + unprovision_args)
