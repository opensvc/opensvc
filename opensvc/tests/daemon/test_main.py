import pytest

from daemon.main import Daemon, main, cmlock
from env import Env


@pytest.fixture(scope='function')
def loop_forever(mocker):
    return mocker.patch.object(Daemon, 'loop_forever')


@pytest.fixture(scope='function')
def daemon_process_running(mocker):
    return mocker.patch(main.__module__ + '.daemon_process_running', return_value=False)


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestDaemonRun:
    @staticmethod
    def test_refuse_to_run_when_daemon_process_is_already_running(loop_forever, daemon_process_running):
        daemon_process_running.return_value = True

        with pytest.raises(SystemExit) as error:
            main(['--debug', '-f'])

        assert loop_forever.call_count == 0
        assert error.value.code == 1

    @staticmethod
    def test_refuse_to_run_when_osvcd_lock_is_held(loop_forever):
        import os
        from time import sleep

        def lock_holder():
            # need lock holder in separate process
            with cmlock(lockfile=Env.paths.daemon_lock):
                sleep(50)

        pid = os.fork()
        if pid > 0:
            sleep(0.05)
        else:
            lock_holder()
            return

        sleep(0.5)
        with pytest.raises(SystemExit) as error:
            main(['--debug', '-f'])
        os.kill(pid, 9)
        assert loop_forever.call_count == 0
        assert error.value.code == 1
        os.waitpid(pid, 0)

    @staticmethod
    def test_run_loop_forever_when_no_other_daemon_are_here(loop_forever):
        main(['--debug', '-f'])
        assert loop_forever.call_count == 1


    @staticmethod
    def test_run_loop_forever_when_daemon_is_dead(loop_forever):
        with open(Env.paths.daemon_pid, 'w') as pid_file:
            pid_file.write('1')
        main(['--debug', '-f'])
        assert loop_forever.call_count == 1

    @staticmethod
    def test_refuse_to_run_when_another_daemon_process_is_running_with_non_same_pid_as_us(
            mocker,
            daemon_process_running,
            loop_forever):
        daemon_process_running.return_value = True
        mocker.patch(main.__module__ + '.os.getpid', return_value=799)
        # write daemon signature with another pid
        Daemon().write_pid()
        # ensure testing pid is not another pid
        mocker.patch(main.__module__ + '.os.getpid', return_value=790)

        with pytest.raises(SystemExit) as error:
            main(['--debug', '-f'])

        assert loop_forever.call_count == 0
        assert error.value.code == 1

    @staticmethod
    def test_run_loop_forever_when_we_are_detected_daemon(loop_forever):
        # write same as us signature
        Daemon().write_pid()

        main(['--debug', '-f'])

        assert loop_forever.call_count == 1
