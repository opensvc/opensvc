import osvcd
import pytest

from rcGlobalEnv import rcEnv


@pytest.fixture(scope='function')
def loop_forever(mocker):
    return mocker.patch.object(osvcd.Daemon, 'loop_forever')


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
        proc.join(timeout=1)
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

    @staticmethod
    def test_refuse_to_run_when_another_daemon_process_is_running_with_non_same_pid_as_us(
            mocker,
            mock_argv,
            loop_forever):
        mocker.patch('osvcd.daemon_process_running', return_value=True)
        mocker.patch('osvcd.os.getpid', return_value=799)
        # write daemon signature with another pid
        osvcd.Daemon().write_pid()
        # ensure testing pid is not another pid
        mocker.patch('osvcd.os.getpid', return_value=790)

        with pytest.raises(SystemExit) as error:
            mock_argv(['--debug', '-f'])
            osvcd.main()
        assert loop_forever.call_count == 0
        assert error.value.code == 1

    @staticmethod
    def test_run_loop_forever_when_we_are_detected_daemon(mock_argv, loop_forever):
        # write same as us signature
        osvcd.Daemon().write_pid()

        mock_argv(['--debug', '-f'])
        osvcd.main()

        assert loop_forever.call_count == 1
