import json
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
def mock_argv(mocker):
    def func(argv):
        mocker.patch.object(sys, 'argv', argv)

    return func


@pytest.fixture(scope='function')
def fake_svc(osvc_path_tests, has_privs, mocker):
    mocker.patch.object(sys, 'argv', ['mgr', "create", '--debug'])
    Mgr(selector='fake-svc')()


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
