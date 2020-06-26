import json
import os
import time

import pytest
import commands.daemon

from daemon.main import main as daemon_main


@pytest.fixture(scope='function')
def daemon_join(mocker):
    return mocker.patch('commands.daemon.Node._daemon_join')


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
            from multiprocessing import Process
            proc = Process(target=daemon_main_target)
            proc.start()
            proc.join()
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
        assert status['listener']['state'] == 'running'
        assert status['scheduler']['state'] == 'running'

        print('daemon status...')
        assert commands.daemon.main(argv=["status", "--debug"]) == 0

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
