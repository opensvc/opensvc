import osvcd
import pytest

from rcGlobalEnv import rcEnv


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestDaemonRun:
    @staticmethod
    def test_refuse_to_run_when_daemon_process_is_already_running(mock_argv, mocker):
        loop_forever = mocker.patch('osvcd.Daemon.loop_forever')
        mocker.patch('osvcd.daemon_process_running', return_value=True)
        mock_argv(['--debug', '-f'])

        with pytest.raises(SystemExit) as error:
            osvcd.main()

        assert loop_forever.call_count == 0
        assert error.value.code == 1

    @staticmethod
    def test_refuse_to_run_when_osvcd_lock_is_held(mock_argv, mocker):
        loop_forever = mocker.patch('osvcd.Daemon.loop_forever')
        mock_argv(['--debug', '-f'])
        from time import sleep

        def lock_holder():
            # need lock holder in separate process
            with osvcd.cmlock(lockfile=rcEnv.paths.daemon_lock):
                sleep(50)

        from multiprocessing import Process
        proc = Process(target=lock_holder)
        proc.start()
        sleep(0.2)
        with pytest.raises(SystemExit) as error:
            osvcd.main()
        proc.terminate()
        assert loop_forever.call_count == 0
        assert error.value.code == 1

    @staticmethod
    def test_run_loop_forever_when_no_other_daemon_are_here(mock_argv, mocker):
        loop_forever = mocker.patch('osvcd.Daemon.loop_forever')
        mock_argv(['--debug', '-f'])
        osvcd.main()
        assert loop_forever.call_count == 1


    @staticmethod
    def test_run_loop_forever_when_daemon_is_dead(mock_argv, mocker):
        loop_forever = mocker.patch('osvcd.Daemon.loop_forever')
        with open(rcEnv.paths.daemon_pid, 'w') as pid_file:
            pid_file.write('1')
        mock_argv(['--debug', '-f'])
        osvcd.main()
        assert loop_forever.call_count == 1
