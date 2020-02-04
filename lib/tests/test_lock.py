import os

import lock
import pytest


@pytest.fixture(scope='function')
def sleep(mocker):
    mocker.patch('lock.time.sleep')


@pytest.mark.ci
@pytest.mark.usefixtures('sleep')
@pytest.mark.parametrize('timeout', range(10))
class TestLockUnlock:
    @staticmethod
    def test_lock_unlock(tmp_file, timeout):
        assert os.path.exists(tmp_file) is False
        lock_fd = lock.lock(lockfile=tmp_file, timeout=timeout, intent="test")
        assert os.path.exists(tmp_file) is True
        lock.unlock(lock_fd)

    @staticmethod
    def test_can_lock_again(tmp_file, timeout):
        assert lock.lock(lockfile=tmp_file, timeout=timeout, intent="test") > 0
        for _ in range(timeout):
            assert lock.lock(lockfile=tmp_file, timeout=timeout, intent="test") is None

    @staticmethod
    def test_lock_raise_lock_timeout_if_held_by_another_pid(mocker, tmp_file, timeout):
        assert lock.lock(lockfile=tmp_file, timeout=timeout, intent="foo") > 0
        mocker.patch('lock.os.getpid', return_value=-1)
        with pytest.raises(lock.LockTimeout, match="holder pid .*, holder intent "):
            lock.lock(lockfile=tmp_file, timeout=timeout, intent="test")

    @staticmethod
    def test_lock_raise_lock_timeout_if_held_by_another_pid_real_multiprocess(tmp_file, timeout):
        def worker():
            import sys
            try:
                sys.exit(lock.lock(lockfile=tmp_file, timeout=timeout, intent="test"))
            except lock.LockTimeout:
                sys.exit(255)

        assert lock.lock(lockfile=tmp_file, timeout=timeout, intent="test") > 0
        from multiprocessing import Process
        proc = Process(target=worker)
        proc.start()
        proc.join()
        assert proc.exitcode == 255


@pytest.mark.ci
class TestCmlockWhenNoLockDir:
    @staticmethod
    def test_create_lock_dir_if_absent(tmp_path):
        assert lock.lock(lockfile=os.path.join(str(tmp_path), 'lockdir', 'lockfile')) > 0


@pytest.mark.ci
@pytest.mark.usefixtures('sleep')
@pytest.mark.parametrize('timeout', range(10))
class TestCmlock:
    @staticmethod
    def test_try_x_times_to_get_lock_until_it_acquires_lock(mocker, tmp_file, timeout):
        runs = []
        if timeout == 0:
            side_effects = [None]
            expected_lock_nowait = 1
        else:
            side_effects = [lock.LockAcquire({"pid": 0, "intent": ""})] * (timeout - 1)
            # noinspection PyTypeChecker
            side_effects.append(None)
            expected_lock_nowait = timeout

        # mocker.patch('lock.os.getpid', return_value=-1)
        lock_nowait = mocker.patch('lock.lock_nowait', side_effect=side_effects)

        with lock.cmlock(lockfile=tmp_file, timeout=timeout):
            runs.append(1)

        assert len(runs) == 1
        assert lock_nowait.call_count == expected_lock_nowait

    @staticmethod
    def test_no_run_x_acquired_fails(mocker, tmp_file, timeout):
        side_effects = [lock.LockAcquire({"pid": 0, "intent": ""})] * (timeout + 1)
        mocker.patch('lock.os.getpid', return_value=-1)
        lock_nowait = mocker.patch('lock.lock_nowait', side_effect=side_effects)

        runs = []
        with pytest.raises(lock.LockTimeout):
            with lock.cmlock(lockfile=tmp_file, timeout=timeout):
                runs.append(1)

        assert len(runs) == 0
        assert lock_nowait.call_count == max(timeout, 1)


@pytest.mark.ci
class TestLockExceptions:
    @staticmethod
    def test_timeout_exc():
        """
        LockTimeOut exception
        """
        try:
            raise lock.LockTimeout(intent="test", pid=20000)
        except lock.LockTimeout as exc:
            assert exc.intent == "test"
            assert exc.pid == 20000

    @staticmethod
    def test_acquire_exc():
        """
        LockAcquire exception
        """
        try:
            raise lock.LockAcquire(intent="test", pid=20000)
        except lock.LockAcquire as exc:
            assert exc.intent == "test"
            assert exc.pid == 20000
