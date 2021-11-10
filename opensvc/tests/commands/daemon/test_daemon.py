import json
import os
import sys
import time

import pytest

import commands.daemon
import commands.svcmon
import core.exceptions as ex
from core.comm import Crypt, DEFAULT_DAEMON_TIMEOUT
from core.node import Node
from daemon.main import main as daemon_main
from env import Env

try:
    # noinspection PyCompatibility
    from unittest.mock import call
except ImportError:
    # noinspection PyUnresolvedReferences
    from mock import call


@pytest.fixture(scope='function')
def daemon_join(mocker):
    return mocker.patch('commands.daemon.Node._daemon_join')


@pytest.fixture(scope='function')
def daemon_post(mocker):
    return mocker.patch.object(Node, 'daemon_post')


@pytest.fixture(scope='function')
def daemon_get(mocker):
    return mocker.patch.object(Crypt,
                               'daemon_get',
                               return_value={
                                   "data": {
                                       "ploc": {
                                           "id": "01e4491d-083f-48ba-8cdf-c5c8771e6b92",
                                           "requested": 1593425914.0323133,
                                           "requester": "u2004-1"
                                       },
                                       "plic": {
                                           "id": "01e4491d-083f-48ba-8cdf-c5c8771e6b99",
                                           "requested": 1593425914.0423133,
                                           "requester": "u2004-1"
                                       }
                                   },
                                   "status": 0
                               })


@pytest.fixture(scope='function')
def daemon_start_native(mocker):
    return mocker.patch.object(Node, 'daemon_start_native', return_value=None)


@pytest.fixture(scope='function')
def daemon_is_running(mocker):
    return mocker.patch.object(Node, 'daemon_running', return_value=0)


@pytest.fixture(scope='function')
def daemon_is_not_running(mocker):
    return mocker.patch.object(Node, 'daemon_running', return_value=1)


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_node_config', 'has_euid_0')
class TestDaemonLockShow:
    @staticmethod
    @pytest.mark.parametrize('server', ['', 'https://u2004-15:1215'])
    def test_calls_daemon_get_on_correct_api_path(daemon_get, server):
        argv = ["lock", "show"]
        if server:
            argv += ["--server", server]
        assert commands.daemon.main(argv=argv) == 0
        daemon_get.assert_called_once_with(
            {'action': 'cluster/locks'},
            timeout=DEFAULT_DAEMON_TIMEOUT,
            server=server,
            with_result=True)

    @staticmethod
    def test_return_non_0_if_daemon_get_status_has_error(daemon_get):
        daemon_get.return_value = {"data": {}, "status": 0, "error": "blah"}
        assert commands.daemon.main(argv=["lock", "show"]) == 1

    @staticmethod
    def test_return_non_0_if_daemon_get_status_is_not_0(daemon_get):
        daemon_get.return_value = {"data": {}, "status": 1}
        assert commands.daemon.main(argv=["lock", "show"]) == 1

    @staticmethod
    def test_has_correct_default_format_forest(daemon_get, capture_stdout, tmp_file):
        with capture_stdout(tmp_file):
            commands.daemon.main(argv=["lock", "show"])
        if int(sys.version[0]) > 2:
            expected_output = '''
name     id                                    requester  requested           
|- plic  01e4491d-083f-48ba-8cdf-c5c8771e6b99  u2004-1    1593425914.0423133  
`- ploc  01e4491d-083f-48ba-8cdf-c5c8771e6b92  u2004-1    1593425914.0323133  

'''
        else:
            expected_output = '''
name     id                                    requester  requested      
|- plic  01e4491d-083f-48ba-8cdf-c5c8771e6b99  u2004-1    1593425914.04  
`- ploc  01e4491d-083f-48ba-8cdf-c5c8771e6b92  u2004-1    1593425914.03  

'''
        with open(tmp_file, 'r') as output_file:
            assert '\n' + output_file.read() == expected_output

    @staticmethod
    def test_output_with_format_json(daemon_get, capture_stdout, tmp_file):
        with capture_stdout(tmp_file):
            commands.daemon.main(argv=["lock", "show", "--format", "json"])
        with open(tmp_file, 'r') as output_file:
            assert json.load(output_file) == daemon_get.return_value['data']


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_node_config', 'has_euid_0')
class TestNodemgrDaemonJoin:
    @staticmethod
    def test_need_option_node(daemon_join):
        assert commands.daemon.main(argv=["join", "--secret", "xxxx"]) == 1
        assert daemon_join.call_count == 0

    @staticmethod
    def test_need_option_secret(daemon_join):
        assert commands.daemon.main(argv=["join", "--node", "node1"]) == 1
        assert daemon_join.call_count == 0

    @staticmethod
    def test_run_join(daemon_join):
        assert commands.daemon.main(argv=["join", "--secret", "xxx", "--node", "node1"]) == 0
        assert daemon_join.call_count == 1

    @staticmethod
    @pytest.mark.parametrize('remote_env, initial_env, env_after_join', [
        ("PRD", "PRD", "PRD"),
        ("TST", "PRD", "TST"),
        ("TST", "TST", "TST"),
        ("PRD", "TST", "PRD"),
        ("PRD", None, "PRD"),
        (None, "PRD", None),
        (None, "TST", None),

        # this rule may change in future, leave local env unset because it is same as default TST
        ("TST", None, None),
    ])
    def test_run_join_update_local_node_config_section_node_env(
            daemon_post,
            remote_env,
            initial_env,
            env_after_join,
    ):
        daemon_post_return_value = {
            "status": 0,
            "data": {
                "node": {"data": {"node": {}}},
                "cluster": {
                    "data": {
                        "DEFAULT": {"id": "e8d6509d-6493-4e5a-a423-a1fe780a8cdf"},
                        "cluster": {
                            "nodes": "node1 %s" % Env.nodename,
                            "id": "810aea42-5faf-11eb-a714-080027001711",
                            "secret": "xxx"
                        },
                        "metadata": {"name": "cluster", "kind": "ccfg", "namespace": None}
                    },
                    "mtime": 1611653218.393235
                }
            }
        }
        if remote_env:
            daemon_post_return_value["data"]["node"]["data"]["node"]["env"] = remote_env
        daemon_post.return_value = daemon_post_return_value
        node = Node()
        if initial_env:
            node.set_multi(['node.env=%s' % initial_env])
        assert commands.daemon.main(argv=["join", "--secret", "xxx", "--node", "node1"]) == 0
        assert node.print_config_data().get('node', {}).get('env') == env_after_join


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_node_config', 'has_euid_0')
class TestNodemgrDaemonReJoin:
    @staticmethod
    def test_need_option_node(daemon_join):
        assert commands.daemon.main(argv=["rejoin"]) == 1
        assert daemon_join.call_count == 0

    @staticmethod
    def test_run_join(daemon_join):
        assert commands.daemon.main(argv=["rejoin", "--node", "node1"]) == 0
        assert daemon_join.call_count == 1


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_node_config', 'has_euid_0')
class TestNodemgrDaemonActions:
    @staticmethod
    @pytest.mark.slow
    def test_start_status_restart_stop_with_running_check(mocker, capture_stdout, tmp_file):
        wait_time_for_osvcd_ready = 8
        if 'OPENSVC_CI_EXTRA_TIME_OSVCD_STARTUP' in os.environ:
            # give extra time when slow ci
            extra_wait_time = int(os.environ['OPENSVC_CI_EXTRA_TIME_OSVCD_STARTUP'])
            print('wait extra time:  OPENSVC_CI_EXTRA_TIME_OSVCD_STARTUP=%s' % extra_wait_time)
            wait_time_for_osvcd_ready += extra_wait_time

        def daemon_main_target():
            daemon_main(args=[])

        def daemon_start_native(_):
            if int(sys.version[0]) > 2:
                from multiprocessing import get_context
                Process = get_context("fork").Process
            else:
                from multiprocessing import Process
            proc = Process(target=daemon_main_target)
            proc.start()
            proc.join()
            time.sleep(0.5)
            return 0

        mocker.patch.object(commands.daemon.Node, 'daemon_start_native', daemon_start_native)
        mocker.patch.object(commands.daemon.Node, 'daemon_handled_by_systemd', return_value=False)
        mocker.patch('utilities.asset.Asset.get_boot_id', side_effect='fake_boot_id')

        print('daemon is not running')
        assert commands.daemon.main(argv=["running", "--debug"]) > 0

        print('daemon start...')
        assert commands.daemon.main(argv=["start", "--debug"]) == 0

        print('daemon is running...')
        assert commands.daemon.main(argv=["running", "--debug"]) == 0

        print('sleep %ss for osvcd ready' % wait_time_for_osvcd_ready)
        time.sleep(wait_time_for_osvcd_ready)

        print('daemon status json...')
        with capture_stdout(tmp_file):
            assert commands.daemon.main(argv=["status", "--format", "json"]) == 0
        with open(tmp_file, 'r') as status_file:
            status = json.load(status_file)
        print(status)
        if "daemon" in status:
            assert status['daemon']['state'] == 'running'
        assert status['monitor']['state'] == 'running'
        assert status['listener']['state'] == 'running'
        assert status['scheduler']['state'] == 'running'

        print('daemon mutex status json...')
        with capture_stdout(tmp_file):
            assert commands.daemon.main(argv=["mutex", "status", "--format", "json"]) == 0
        with open(tmp_file, 'r') as status_file:
            status = json.load(status_file)
        print(status)
        assert len(status) > 0, "no mutexes returned"
        mutex_busy = 0
        mutex_free = 0
        for name, value in status.items():
            if "owner=None" in value or "unlocked" in value:
                mutex_free = mutex_free + 1
            else:
                mutex_busy = mutex_busy + 1
        assert mutex_free > mutex_busy, \
            "found more busy than free mutexes: \n%s" % status
        assert len([k for k in status.keys() if "logger" in k]) > 0, "no logger in mutexes"

        print('daemon stats json...')
        with capture_stdout(tmp_file):
            assert commands.daemon.main(argv=["stats", "--debug", "--format", "json"]) == 0
        with open(tmp_file, 'r') as stats_file:
            stats = json.load(stats_file)
        print(stats)
        assert stats[Env.nodename]["daemon"]["cpu"]["time"] >= 0, \
            "unable to detect daemon cpu time"

        print('daemon status...')
        assert commands.daemon.main(argv=["status", "--debug"]) == 0

        print('om mon...')
        assert commands.svcmon.main(argv=["mon"]) == 0

        print('om mon --format compact...')
        assert commands.svcmon.main(argv=["mon", "--format", "compact"]) == 0

        print('daemon restart...')
        assert commands.daemon.main(argv=["restart", "--debug"]) == 0

        print('daemon is running...')
        assert commands.daemon.main(argv=["running", "--debug"]) == 0

        print('sleep %ss for osvcd ready' % wait_time_for_osvcd_ready)
        time.sleep(wait_time_for_osvcd_ready)

        print('daemon stop...')
        assert commands.daemon.main(argv=["stop", "--debug"]) == 0

        print('daemon is not running...')
        assert commands.daemon.main(argv=["running", "--debug"]) > 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_node_config', 'has_euid_0')
class TestDaemonStart:
    @staticmethod
    def test_does_nothing_if_already_started(daemon_is_running, daemon_start_native):
        assert commands.daemon.main(argv=["start"]) == 0
        assert daemon_start_native.call_count == 0

    @staticmethod
    def test_start_native_if_not_yet_running(daemon_is_not_running, daemon_start_native):
        assert commands.daemon.main(argv=["start"]) == 0
        assert daemon_start_native.call_count == 1

    @staticmethod
    def test_return_1_if_errors_during_start_native(daemon_is_not_running, daemon_start_native):
        daemon_start_native.side_effect = ex.Error
        assert commands.daemon.main(argv=["start"]) == 1


def load_json(name):
    with open(os.path.join(os.path.dirname(__file__), "fixtures", name + ".json"), "r") as f:
        return json.load(f)


@pytest.mark.ci
@pytest.mark.usefixtures('has_euid_0', 'osvc_path_tests')
class TestDaemonStats:
    @staticmethod
    @pytest.mark.parametrize("nodes", ["*", "node1", "node1,node2"])
    def test_it_sends_correct_api_call(mocker, nodes):
        daemon_get = mocker.patch.object(Node, "daemon_get", return_value={"status": 0, "data": {}})
        assert commands.daemon.main(argv=["stats", "--node", nodes]) == 0
        daemon_get.assert_called_once_with({
            'action': 'daemon_stats',
            'options': {'selector': '**'}},
            node=nodes,
            server='',
            silent=False,
            timeout=5)

    @staticmethod
    @pytest.mark.parametrize("name", ["conn_refused", "one_node", "two_nodes", "one_node_with_error", "two_nodes_with_error"])
    def test_it_returns_0(mocker, name):
        mocker.patch.object(Node, "daemon_get", return_value=load_json("daemon_stats_" + name))
        assert commands.daemon.main(argv=["stats"]) == 0

    @staticmethod
    def test_it_returns_0_when_daemon_request_raises(mocker, log):
        mocker.patch.object(Node, "daemon_get", side_effect=Exception("socket error..."))
        assert commands.daemon.main(argv=["stats"]) == 0
        log.warning.assert_has_calls([call("daemon stats error")])

    @staticmethod
    @pytest.mark.parametrize("name", ["one_node", "two_nodes", "one_node_with_error", "two_nodes_with_error"])
    def test_it_output_some_available_stats(mocker, capture_stdout, tmp_file, name):
        mocker.patch.object(Node, "daemon_get", return_value=load_json("daemon_stats_" + name))

        with capture_stdout(tmp_file):
            assert commands.daemon.main(argv=["stats"]) == 0
        with open(tmp_file, 'r') as output_file:
            output = output_file.read()
        assert "cpu.time" in output

    @staticmethod
    @pytest.mark.parametrize("name", ["conn_refused", "one_node_with_error", "two_nodes_with_error"])
    def test_it_log_errors(mocker, log, name):
        mocker.patch.object(Node, "daemon_get", return_value=load_json("daemon_stats_" + name))
        assert commands.daemon.main(argv=["stats"]) == 0
        if "one_nodes_with_error" in name:
            log.warning.assert_has_calls([call("daemon stats error")])
        if "two_nodes_with_error" in name:
            log.warning.assert_has_calls([call("daemon stats error for node %s", "node-error")])

