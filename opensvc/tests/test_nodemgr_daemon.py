import json
import os
import time

import pytest

from commands import nodemgr
from daemon.main import main as daemon_main


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests', 'has_node_config')
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

        mocker.patch.object(nodemgr.Node, 'check_privs', return_value=None)
        mocker.patch.object(nodemgr.Node, 'daemon_start_native', daemon_start_native)
        mocker.patch.object(nodemgr.Node, 'daemon_handled_by_systemd', return_value=False)
        mocker.patch('utilities.asset.Asset.get_boot_id', side_effect='fake_boot_id')

        print('daemon is not running')
        assert nodemgr.main(argv=["daemon", "running", "--debug"]) > 0

        print('daemon start...')
        assert nodemgr.main(argv=["daemon", "start", "--debug"]) == 0

        print('daemon is running...')
        assert nodemgr.main(argv=["daemon", "running", "--debug"]) == 0

        print('sleep %ss for osvcd ready' % wait_time_for_osvcd_ready)
        time.sleep(wait_time_for_osvcd_ready)

        print('daemon status json...')
        with capture_stdout(tmp_file):
            assert nodemgr.main(argv=["daemon", "status", "--format", "json"]) == 0
        with open(tmp_file, 'r') as status_file:
            status = json.load(status_file)
        print(status)
        assert status['listener']['state'] == 'running'
        assert status['scheduler']['state'] == 'running'

        print('daemon status...')
        assert nodemgr.main(argv=["daemon", "status", "--debug"]) == 0

        print('daemon restart...')
        assert nodemgr.main(argv=["daemon", "restart", "--debug"]) == 0

        print('daemon is running...')
        assert nodemgr.main(argv=["daemon", "running", "--debug"]) == 0

        print('sleep %ss for osvcd ready' % wait_time_for_osvcd_ready)
        time.sleep(wait_time_for_osvcd_ready)

        print('daemon stop...')
        assert nodemgr.main(argv=["daemon", "stop", "--debug"]) == 0

        print('daemon is not running...')
        assert nodemgr.main(argv=["daemon", "running", "--debug"]) > 0
