import socket

import pytest
try:
    # noinspection PyCompatibility
    from unittest.mock import call
except ImportError:
    # noinspection PyUnresolvedReferences
    from mock import call

import core.exceptions as ex
from core.node import Node
from core.objects.svc import Svc
from drivers.resource.disk.drbd import DiskDrbd, driver_capabilities


@pytest.fixture(scope='function')
def just_call(mocker):
    return mocker.patch('drivers.resource.disk.drbd.justcall')


@pytest.fixture(scope='function')
def sleep(mocker):
    return mocker.patch('drivers.resource.disk.drbd.time.sleep')


@pytest.fixture(scope='function')
def get_disk():
    def func(cd=None):
        cd_svc = cd or {
            "DEFAULT": {"nodes": "node1 node2"},
            "disk#1": {"type": "drbd", "res": "foo", "disk": "a_disk"}
        }
        svc = Svc(name='plop', volatile=True, node=Node(), cd=cd_svc)
        return svc.get_resource('disk#1')

    return func


@pytest.fixture(scope='function')
def disk(get_disk):
    return get_disk()


@pytest.fixture(scope='function')
def allocations(mocker):
    return mocker.patch.object(DiskDrbd, 'allocations', {"minors": (0, 1, 2), 'ports': (7289, 7290, 7291)})


@pytest.fixture(scope='function')
def gethostbyname(mocker):
    def func(name):
        return 'ip-' + name

    mocker.patch.object(socket, 'gethostbyname', side_effect=func)


@pytest.fixture(scope='function')
def daemon_get_allocations(mocker):
    return mocker.patch.object(
        DiskDrbd,
        "daemon_get_allocations",
        return_value={"nodes": {"node1": {"minors": [0, 1, 2], "ports": [7289, 7290, 7291]},
                                "node2": {"minors": [0, 1, 2, 3], "ports": [7289, 7290, 7291, 7292]}},
                      "status": 0})


@pytest.fixture(scope='function')
def _daemon_lock(mocker):
    return mocker.patch.object(Node, '_daemon_lock', mocker.Mock())


@pytest.fixture(scope='function')
def _daemon_unlock(mocker):
    return mocker.patch.object(Node, '_daemon_unlock', mocker.Mock())


@pytest.fixture(scope='function')
def node_daemon_post(mocker):
    return mocker.patch.object(Node, 'daemon_post', mocker.Mock(side_effect=[{"status": 0}] * 2))


@pytest.fixture(scope='function')
def svc_daemon_post(mocker):
    return mocker.patch.object(Svc, 'daemon_post', mocker.Mock(side_effect=[{"status": 0}] * 2))


@pytest.fixture(scope='function')
def which(mocker):
    return mocker.patch('drivers.resource.disk.drbd.which')


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdCapabilities:
    @staticmethod
    def test_has_no_drbd_capabilities_when_drbdadm_is_not_present(which):
        which.return_value = None
        assert "disk.drbd" not in driver_capabilities()
        assert "disk.drbd.mesh" not in driver_capabilities()

    @staticmethod
    def test_has_disk_drbd_if_drbdadm_exists(which, just_call):
        just_call.side_effect = [("", "", 0)]
        assert 'disk.drbd' in driver_capabilities()

    @staticmethod
    def test_doesnot_have_disk_drbd_mesh_if_modinfo_report_version_8(which, just_call):
        just_call.side_effect = [
            ("Version: 9.15.0", "", 0),  # drbdadm output
            ("version: 8.0.25-1", "", 0),  # modinfo drbd output
        ]
        capabilities = driver_capabilities()
        assert "disk.drbd" in capabilities
        assert "disk.drbd.mesh" not in capabilities

    @staticmethod
    def test_has_disk_drbd_mesh_if_modinfo_report_version_9(which, just_call):
        just_call.side_effect = [
            ("Version: 9.15.0", "", 0),  # drbdadm output
            ("version: 9.0.25-1", "", 0),  # modinfo drbd output
        ]
        capabilities = driver_capabilities()
        assert "disk.drbd" in capabilities
        assert "disk.drbd.mesh" in capabilities


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdWriteConfig:
    @staticmethod
    def test_use_config_file_name_into_etc_drbd_d_from_res_value():
        assert DiskDrbd(res='foo').cf == '/etc/drbd.d/foo.res'

    @staticmethod
    @pytest.mark.usefixtures('_daemon_lock', '_daemon_unlock')
    @pytest.mark.usefixtures('node_daemon_post', 'svc_daemon_post')
    @pytest.mark.usefixtures('daemon_get_allocations')
    @pytest.mark.usefixtures('gethostbyname')
    @pytest.mark.parametrize('capabilities, disk_cd, expected_config', [
        [['disk.drbd'],
         {"type": "drbd", "res": "foo", "disk": "a_disk"},
         """resource foo {
    on node1 {
        device    /dev/drbd4;
        disk      a_disk;
        meta-disk internal;
        address   ip-node1:7293;
    }
    on node2 {
        device    /dev/drbd4;
        disk      a_disk;
        meta-disk internal;
        address   ip-node2:7293;
    }
}
"""],
        [['disk.drbd', 'disk.drbd.mesh'],
         {"type": "drbd", "res": "foo", "disk": "a_disk"},
         """resource foo {
    on node1 {
        device    /dev/drbd4;
        disk      a_disk;
        meta-disk internal;
        address   ip-node1:7293;
        node-id   0;
    }
    on node2 {
        device    /dev/drbd4;
        disk      a_disk;
        meta-disk internal;
        address   ip-node2:7293;
        node-id   1;
    }
    connection-mesh {
        hosts node1 node2;
    }
}
"""],
        [['disk.drbd', 'disk.drbd.mesh'],
         {"type": "drbd", "res": "foo", "disk": "/dev/vg1/lv1", "port": "7298"},
         """resource foo {
    on node1 {
        device    /dev/drbd4;
        disk      /dev/vg1/lv1;
        meta-disk internal;
        address   ip-node1:7298;
        node-id   0;
    }
    on node2 {
        device    /dev/drbd4;
        disk      /dev/vg1/lv1;
        meta-disk internal;
        address   ip-node2:7298;
        node-id   1;
    }
    connection-mesh {
        hosts node1 node2;
    }
}
"""]
    ])
    def test_has_create_correct_config_file(mocker, tmp_file, klass_has_capability, capabilities, disk_cd,
                                            expected_config, get_disk):
        klass_has_capability(DiskDrbd, capabilities)
        mocker.patch.object(DiskDrbd, 'cf', str(tmp_file))
        get_disk(cd={"DEFAULT": {"nodes": "node1 node2"}, "disk#1": disk_cd}).write_config()

        assert open(tmp_file, 'r').read() == expected_config


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdProvisioner:
    @staticmethod
    def test_make_correct_leader_calls(mocker, disk):
        expected_methods = ['write_config',
                            'create_md',
                            'drbdadm_down',
                            'drbdadm_up',
                            'start_role',
                            'get_cstate']
        for method in expected_methods:
            mocker.patch.object(DiskDrbd, method, mocker.Mock(method))
        disk.svc.options['leader'] = True
        disk.provisioner()
        for method in expected_methods:
            assert getattr(disk, method).call_count == 1
        disk.start_role.assert_called_once_with("Primary", extra_args=["--force"])

    @staticmethod
    def test_make_correct_nonleader_calls(mocker, disk):
        expected_methods = ['write_config_from_peer',
                            'create_md',
                            'drbdadm_down',
                            'drbdadm_up',
                            'drbdadm_disconnect',
                            'drbdadm_connect']
        for method in expected_methods:
            mocker.patch.object(DiskDrbd, method, mocker.Mock(method))
        disk.provisioner()
        for method in expected_methods:
            assert getattr(disk, method).call_count == 1


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdCreateMd:
    @staticmethod
    def test_it_does_not_recreate_if_already_exists(mocker, disk):
        mocker.patch.object(DiskDrbd, 'has_md', return_value=True)
        disk.create_md()

    @staticmethod
    def test_it_call_correct_mdadm_create_md(mocker, disk):
        mocker.patch.object(DiskDrbd, 'has_md', return_value=False)
        mocker.patch.object(DiskDrbd, 'vcall', return_value=(0, '', ''))
        disk.create_md()
        disk.vcall.assert_called_once_with(['drbdadm', 'create-md', '--force', 'foo'])

    @staticmethod
    @pytest.mark.parametrize('mdadm_exit_code', range(1, 10))
    def test_it_raise_if_mdadm_cmd_return_non_0(mocker, disk, mdadm_exit_code):
        mocker.patch.object(DiskDrbd, 'has_md', return_value=False)
        mocker.patch.object(DiskDrbd, 'vcall', return_value=(mdadm_exit_code, '', ''))
        with pytest.raises(ex.Error):
            disk.create_md()


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdadmDown:
    @staticmethod
    def test_it_repeat_drbdadm_command_until_exit_status_0(sleep, just_call, disk):
        just_call.side_effect = [
            ('', '', 11),
            ('', '', 11),
            ('', '', 11),
            ('', '', 0),
        ]
        disk.drbdadm = "drbdadm"
        disk.drbdadm_down()
        assert sleep.call_count == 3
        assert just_call.call_count == 4
        assert just_call.call_args_list == [call(['drbdadm', 'down', 'foo'])] * 4

    @staticmethod
    def test_it_raise_if_drbdadm_command_is_not_0_or_11(sleep, just_call, disk):
        just_call.side_effect = [('', '', 1)]
        disk.drbdadm = "drbdadm"
        with pytest.raises(ex.Error):
            disk.drbdadm_down()

    @staticmethod
    def test_it_raise_if_drbdadm_change_in_progress_time_takes_to_long(sleep, just_call, disk):
        just_call.side_effect = [('', '', 11)] * 20
        disk.drbdadm = "drbdadm"
        with pytest.raises(ex.Error):
            disk.drbdadm_down()


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdadmUp:
    @staticmethod
    def test_it_repeat_drbdadm_command_until_exit_status_0(sleep, mocker, just_call, disk):
        just_call.side_effect = [
            ('', '', 11),
            ('', '', 11),
            ('', '', 11),
            ('', '', 0),
        ]
        disk.drbdadm = "drbdadm"
        mocker.patch.object(DiskDrbd, 'call', side_effect=[(0, 'Diskless/DUnknown', ''),
                                                           (0, 'UpToDate/UpToDate', '')])
        disk.drbdadm_up()
        assert sleep.call_count == 4  # 3 for up, + 1 for dstate
        assert just_call.call_count == 4
        assert just_call.call_args_list == [call(['drbdadm', 'up', 'foo'])] * 4
        assert disk.call.call_args_list == [call(['drbdadm', 'dstate', 'foo'])] * 2

    @staticmethod
    def test_it_raise_if_drbdadm_command_until_exit_status_non_0_or_11(sleep, just_call, disk):
        just_call.side_effect = [('', '', 11)] * 3 + [('', '', 1)]
        disk.drbdadm = "drbdadm"
        with pytest.raises(ex.Error):
            disk.drbdadm_up()
        assert sleep.call_count == 3  # 3 sleep for status 11
        assert just_call.call_count == 4  # 3 pending, last status 1
        assert just_call.call_args_list == [call(['drbdadm', 'up', 'foo'])] * 4

    @staticmethod
    def test_it_raise_if_drbdadm_command_fail_to_get_changed_state(sleep, just_call, disk):
        just_call.side_effect = [('', '', 11)] * 11
        disk.drbdadm = "drbdadm"
        with pytest.raises(ex.Error):
            disk.drbdadm_up()
        assert sleep.call_count == 10
        assert just_call.call_count == 10
        assert just_call.call_args_list == [call(['drbdadm', 'up', 'foo'])] * 10

    @staticmethod
    def test_it_raise_if_drbdadm_command_fail_to_stable_dstate(sleep, mocker, just_call, disk):
        just_call.side_effect = [('', '', 0)]
        mocker.patch.object(DiskDrbd, 'call', side_effect=[(0, 'Diskless/DUnknown', '')] * 11)
        disk.drbdadm = "drbdadm"
        with pytest.raises(ex.Error):
            disk.drbdadm_up()
        assert just_call.call_args_list == [call(['drbdadm', 'up', 'foo'])]
        assert disk.call.call_args_list == [call(['drbdadm', 'dstate', 'foo'])] * 5


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdadmStartRole:
    @staticmethod
    @pytest.mark.parametrize('role', ['Primary', 'Secondary'])
    def test_returns_immediately_if_has_already_the_correct_role_drbd9(sleep, role, just_call, disk):
        just_call.side_effect = [(role, '', 0)]
        disk.drbdadm = "drbdadm"
        disk.start_role(role)
        assert just_call.call_args_list == [call(['drbdadm', 'role', 'foo'])]

    @staticmethod
    @pytest.mark.parametrize('role', ['Primary', 'Secondary'])
    def test_has_can_rollback_values_to_false_when_no_action_required(sleep, role, just_call, disk):
        just_call.side_effect = [(role, '', 0)]
        disk.drbdadm = "drbdadm"
        disk.start_role(role)
        assert disk.can_rollback_role is False
        assert disk.can_rollback is False

    @staticmethod
    @pytest.mark.parametrize('role', ['Primary', 'Secondary'])
    def test_returns_immediately_if_has_already_the_correct_role_drbd8(sleep, role, just_call, disk):
        just_call.side_effect = [(role + '/not_used_here', '', 0)]
        disk.drbdadm = "drbdadm"
        disk.start_role(role)
        assert just_call.call_args_list == [call(['drbdadm', 'role', 'foo'])]

    @staticmethod
    @pytest.mark.parametrize('role', ['Primary', 'Secondary'])
    def test_call_change_role_when_required(sleep, role, just_call, disk):
        just_call.side_effect = [('not_expected_role/not_used_here', '', 0)] + [('', '', 11)] * 2 + [('', '', 0)]
        disk.drbdadm = "drbdadm"
        disk.start_role(role)
        expected_calls = [call(['drbdadm', 'role', 'foo'])]
        expected_calls += [call(['drbdadm', role.lower(), 'foo'])] * 2  # pending
        expected_calls += [call(['drbdadm', role.lower(), 'foo'])]  # succeed
        assert just_call.call_args_list == expected_calls

    @staticmethod
    @pytest.mark.parametrize('role', ['Primary', 'Secondary'])
    def test_set_can_rollback_role_and_can_rollback_to_true(sleep, role, just_call, disk):
        just_call.side_effect = [('not_expected_role/not_used_here', '', 0), ('', '', 0)]
        disk.drbdadm = "drbdadm"
        disk.start_role(role)
        assert disk.can_rollback_role is True
        assert disk.can_rollback is True

    @staticmethod
    def test_raise_if_can_not_get_current_role(just_call, disk):
        just_call.side_effect = [('', '', 1)]
        disk.drbdadm = "drbdadm"
        with pytest.raises(ex.Error):
            disk.start_role('a_bad_role')


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdadmGetCstate:
    @staticmethod
    def test_raise_when_role_cmd_return_non_0(sleep, mocker, just_call, disk):
        mocker.patch.object(DiskDrbd, 'prereq', mocker.Mock())
        just_call.side_effect = [('', '', 1)]
        disk.drbdadm = "drbdadm"
        with pytest.raises(ex.Error):
            disk.get_cstate()

    @staticmethod
    @pytest.mark.parametrize('cstate_just_result', [('', '', 10), ('', 'Device minor not allocated', 1)])
    def test_return_unattached_examples(sleep, mocker, just_call, cstate_just_result, disk):
        mocker.patch.object(DiskDrbd, 'prereq', mocker.Mock())
        just_call.return_value = cstate_just_result
        disk.drbdadm = "drbdadm"
        assert disk.get_cstate() == 'Unattached'


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDrbdProvisionerAlternateTestMethod:
    @staticmethod
    def test_make_correct_leader_calls(sleep, mocker, just_call, disk):
        disk.svc.options['leader'] = True
        mocker.patch.object(DiskDrbd, 'prereq', mocker.Mock())
        mocker.patch.object(DiskDrbd, 'write_config', mocker.Mock())
        mocker.patch.object(DiskDrbd, 'vcall', return_value=(0, '', ''))
        mocker.patch.object(DiskDrbd, 'call', return_value=(0, '', 'No valid meta data found'))
        just_call.side_effect = [
            ('', '', 11),  # down fails
            ('', '', 0),   # down succeed
            ('', '', 11),  # up fails
            ('', '', 0),   # up succeed
            ('Secondary', '', 0),  # get role is Secondary
            ('', '', 11),  # set role fails
            ('', '', 0),  # set role succeed
            ('UpToDate/UpToDate', '', 0),  # cstate
            ('', 'Device minor not allocated', 1),
        ]
        disk.drbdadm = 'drbdadm'
        disk.provisioner()

        assert disk.write_config.call_count == 1
        assert just_call.call_args_list == [call(['drbdadm', 'down', 'foo']),
                                            call(['drbdadm', 'down', 'foo']),
                                            call(['drbdadm', 'up', 'foo']),
                                            call(['drbdadm', 'up', 'foo']),
                                            call(['drbdadm', 'role', 'foo']),
                                            call(['drbdadm', 'primary', '--force', 'foo']),
                                            call(['drbdadm', 'primary', '--force', 'foo']),
                                            call(['drbdadm', 'cstate', 'foo'])]
        assert disk.vcall.call_args_list == [call(['drbdadm', 'create-md', '--force', 'foo'])]
        assert disk.call.call_args_list == [call(['drbdadm', '--', '--force', 'dump-md', 'foo'],
                                                 errlog=False, outlog=False),
                                            call(['drbdadm', 'dstate', 'foo'])]
