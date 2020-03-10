import json
import os
import sys

from mgr import Mgr

import pytest

from node import Node

OS_LIST = {'Linux', 'SunOS', 'Darwin', 'FreeBSD', 'HP-UX', 'OSF1'}
OS_LIST_WITH_FS_FLAG = {'Linux', 'SunOS'}


@pytest.fixture(scope='function')
def has_privs(mocker):
    mocker.patch('mgr.check_privs', return_value=None)


@pytest.fixture(scope='function')
def fake_svc(osvc_path_tests, has_privs, mocker):
    mocker.patch.object(sys, 'argv', ['mgr', "create", '--debug'])
    Mgr(selector='fake-svc')()


@pytest.fixture(scope='function')
def mgr_cmd(mock_argv):
    def func(argv, **kwargs):
        mock_argv(argv)
        return Mgr(**kwargs)()

    return func


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
class TestServiceActionWithoutPrivs:
    @staticmethod
    def test_no_call_to_main_and_exit_1(mocker, mock_argv, mock_sysname, sysname):
        mocker.patch('rcUtilities.os.geteuid', return_value=66)
        mock_sysname(sysname)
        mock_argv(['mgr', "create"])
        sys_exit = mocker.patch.object(sys, 'exit', side_effect=Exception("exit"))
        _main = mocker.patch.object(Mgr, '_main')
        with pytest.raises(Exception, match="exit"):
            Mgr(selector='svc1')()
        sys_exit.assert_called_once_with(1)
        assert _main.call_count == 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_privs')
@pytest.mark.parametrize('sysname', OS_LIST)
class TestServiceActionWithPriv:
    @staticmethod
    def test_wrong_action_exit_1(fake_svc, mock_argv, mock_sysname, sysname):
        mock_argv(['mgr', "wrong-action"])
        mock_sysname(sysname)

        assert Mgr(selector='fake-svc')() == 1

    @staticmethod
    def test_print_config(fake_svc, tmp_file, capture_stdout, mock_argv, mock_sysname, sysname):
        mock_argv(['mgr', "print", "config"])
        mock_sysname(sysname)

        with capture_stdout(tmp_file):
            assert Mgr(selector='fake-svc')() == 0
        with open(tmp_file) as f:
            config_text = f.read()
        assert '[DEFAULT]' in config_text
        assert 'id =' in config_text

    @staticmethod
    def test_print_config_json(fake_svc, tmp_file, capture_stdout, mock_argv, mock_sysname, sysname):
        mock_argv(['mgr', "print", "config", '--format', 'json'])
        mock_sysname(sysname)

        with capture_stdout(tmp_file):
            assert Mgr(selector='fake-svc')() == 0
        with open(tmp_file) as json_file:
            config = json.load(json_file)
        assert config['DEFAULT']['id']

    @staticmethod
    def test_create_call_node_create_service(mocker, mock_argv, mock_sysname, sysname):
        mock_argv(['mgr', "create"])
        mock_sysname(sysname)
        node_create_service = mocker.patch.object(Node, 'create_service', return_value=None)

        assert Mgr(selector='svc1', node=Node())() == 0
        assert node_create_service.call_count == 1


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_privs')
class TestServiceActionFsFlag:
    @staticmethod
    @pytest.mark.parametrize('sysname', OS_LIST_WITH_FS_FLAG)
    def test_create_service_with_fs_flag_then_verify_config(
            mock_argv, tmp_file, capture_stdout, mock_sysname, sysname):
        mock_sysname(sysname)

        mock_argv(['mgr', 'create', '--kw', 'fs#1.type=flag'])
        assert Mgr(selector=sysname)() == 0

        mock_argv(['mgr', 'set', '--kw', 'fs#2.type=flag'])
        assert Mgr(selector=sysname)() == 0

        mock_argv(['om', 'print', 'config', '--format', 'json'])
        with capture_stdout(tmp_file):
            assert Mgr(selector=sysname)() == 0
        with open(tmp_file) as config_file:
            config = json.load(config_file)
        assert config["fs#1"] == {'type': 'flag'}
        assert config["fs#2"] == {'type': 'flag'}

    @staticmethod
    @pytest.mark.parametrize('sysname', OS_LIST ^ OS_LIST_WITH_FS_FLAG)
    def test_set_fs_flag_not_added_when_not_supported_on_os(
            fake_svc, mock_argv, tmp_file, capture_stdout, mock_sysname, sysname):
        mock_argv(['mgr', 'set', '--kw', 'fs#1.type=flag'])
        mock_sysname(sysname)

        assert Mgr(selector='fake-svc')() == 1

    @staticmethod
    @pytest.mark.parametrize('sysname', OS_LIST_WITH_FS_FLAG)
    def test_create_then_start_then_verify_flag_file_exists(tmp_path,
                                                            mocker,
                                                            mock_argv,
                                                            mock_sysname,
                                                            sysname):
        mock_sysname(sysname)
        base_flag_d = str(tmp_path)
        mocker.patch('resFsFlag' + sysname + '.Fs.base_flag_d',
                     new_callable=mocker.PropertyMock(return_value=base_flag_d))

        expected_flag_file = os.path.join(base_flag_d, 'svc', sysname, 'fs#1.flag')

        assert not os.path.exists(expected_flag_file)

        mock_argv(['mgr', 'create', '--kw', 'fs#1.type=flag', '--debug'])
        Mgr(selector=sysname)()

        assert not os.path.exists(expected_flag_file)

        mock_argv(['mgr', 'start', '--debug', '--local'])
        assert Mgr(selector=sysname)() == 0

        assert os.path.exists(expected_flag_file)


@pytest.mark.ci
@pytest.mark.usefixtures('has_privs')
class TestServiceActionWithVolume:
    @staticmethod
    def test_provision_service_with_config(has_service_with_vol_and_cfg, mock_argv):
        expected_voldir = os.path.join(
            str(has_service_with_vol_and_cfg),
            'var',
            'pool',
            'directory',
            'vol-test.root.vol.default')

        mock_argv(['mgr', 'provision', '--local', '--leader', '--debug'])
        assert Mgr(selector='svc')() == 0

        def assert_file_contain(file, expected_value):
            with open(os.path.join(expected_voldir, file)) as file:
                assert file.read() == expected_value

        assert_file_contain('simple_dest', 'cfg content of key simple')
        assert_file_contain('simple_b', 'cfg content of key /simpleb')
        assert_file_contain('baR', 'cfg content of key camelCase/Foo/baR')
        assert_file_contain('double-star-to-only-one', 'cfg content of key i/j/k/only-one')

        assert_file_contain(os.path.join('star-to-dir', 'b', 'c'), 'cfg content of key a/b/c')
        assert_file_contain(os.path.join('star-to-dir', 'e', 'f1'), 'cfg content of key a/e/f1')
        assert_file_contain(os.path.join('star-to-dir', 'e', 'f2'), 'cfg content of key a/e/f2')
        assert_file_contain(os.path.join('star-to-dir', 'g'), 'cfg content of key a/g')

        assert_file_contain(os.path.join('recursive-dir', 'b', 'c'), 'cfg content of key a/b/c')
        assert_file_contain(os.path.join('recursive-dir', 'e', 'f1'), 'cfg content of key a/e/f1')
        assert_file_contain(os.path.join('recursive-dir', 'e', 'f2'), 'cfg content of key a/e/f2')
        assert_file_contain(os.path.join('recursive-dir', 'g'), 'cfg content of key a/g')

        assert_file_contain(os.path.join('recursive-dir-with-os-sep', 'b', 'c'), 'cfg content of key a/b/c')
        assert_file_contain(os.path.join('recursive-dir-with-os-sep', 'e', 'f1'), 'cfg content of key a/e/f1')
        assert_file_contain(os.path.join('recursive-dir-with-os-sep', 'e', 'f2'), 'cfg content of key a/e/f2')
        assert_file_contain(os.path.join('recursive-dir-with-os-sep', 'g'), 'cfg content of key a/g')

        assert_file_contain(os.path.join('recursive-dir-with-os-sep_2', 'a', 'b', 'c'), 'cfg content of key a/b/c')
        assert_file_contain(os.path.join('recursive-dir-with-os-sep_2', 'a', 'e', 'f1'), 'cfg content of key a/e/f1')
        assert_file_contain(os.path.join('recursive-dir-with-os-sep_2', 'a', 'e', 'f2'), 'cfg content of key a/e/f2')
        assert_file_contain(os.path.join('recursive-dir-with-os-sep_2', 'a', 'g'), 'cfg content of key a/g')

        assert_file_contain(os.path.join('os-sep-d', 'f'), 'cfg content of key /e/f')

        assert_file_contain(os.path.join('all-cfg1', 'simple'), 'cfg content of key simple')
        assert_file_contain(os.path.join('all-cfg1', 'simpleb'), 'cfg content of key /simpleb')
        assert_file_contain(os.path.join('all-cfg1', 'a', 'g'), 'cfg content of key a/g')
        assert_file_contain(os.path.join('all-cfg1', 'e', 'f'), 'cfg content of key /e/f')


@pytest.mark.ci
@pytest.mark.usefixtures('has_privs')
class TestServiceActionWhenNoDaemonListen:
    @staticmethod
    @pytest.mark.parametrize('sysname', OS_LIST_WITH_FS_FLAG)
    def test_no_hang(osvc_path_tests, mock_argv, mock_sysname, sysname):
        import socket

        h2_sock = os.path.join(str(osvc_path_tests), 'var', 'lsnr', 'h2.sock')
        assert not os.path.exists(h2_sock)

        sockuxh2 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sockuxh2.bind(h2_sock)
        sockuxh2.close()
        assert os.path.exists(h2_sock)

        mock_argv(['mgr', 'create', '--debug'])
        Mgr(selector=sysname)()

        mock_argv(['mgr', 'start', '--debug', '--local'])
        assert Mgr(selector=sysname)() == 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_privs')
class TestCreateAddDecode:
    @staticmethod
    @pytest.mark.parametrize('key', ['lowercase', 'camelCase', 'UPPERCASE'])
    @pytest.mark.parametrize('obj', ['demo/cfg/name', 'demo/sec/name'])
    def test_decoded_value_is_correct(mock_argv, capture_stdout, tmp_file, obj, key):
        def run_command(args):
            mock_argv(args)
            return Mgr(selector=obj)()

        assert run_command(['mgr', 'create']) == 0
        assert run_command(['mgr', 'add', '--key', key, '--value', 'john']) == 0
        with capture_stdout(tmp_file):
            assert run_command(['mgr', 'decode', '--key', key]) == 0

        with open(tmp_file) as output_file:
            assert output_file.read() == 'john'

    @staticmethod
    @pytest.mark.parametrize('obj', ['demo/cfg/name', 'demo/sec/name'])
    def test_accept_empty_values(mgr_cmd, capture_stdout, tmp_file, obj):
        assert mgr_cmd(['mgr', 'create'], selector=obj) == 0
        assert mgr_cmd(['mgr', 'add', '--key', 'empty', '--value', ''], selector=obj) == 0
        with capture_stdout(tmp_file):
            assert mgr_cmd(['mgr', 'decode', '--key', 'empty'], selector=obj) == 0

        with open(tmp_file) as output_file:
            assert output_file.read() == ''

    @staticmethod
    @pytest.mark.parametrize('obj', ['demo/cfg/name', 'demo/sec/name'])
    def test_accept_from_empty_files(mgr_cmd, capture_stdout, tmp_file, obj):
        open(tmp_file, 'w+').close()
        assert mgr_cmd(['mgr', 'create'], selector=obj) == 0
        assert mgr_cmd(['mgr', 'add', '--key', 'empty', '--from', tmp_file], selector=obj) == 0
        with capture_stdout(tmp_file):
            assert mgr_cmd(['mgr', 'decode', '--key', 'empty'], selector=obj) == 0

        with open(tmp_file) as output_file:
            assert output_file.read() == ''
