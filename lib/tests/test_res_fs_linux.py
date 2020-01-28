import pytest
from resFsLinux import Mount


@pytest.fixture(scope='function')
def log(mocker):
    return mocker.patch.object(Mount, 'log', autospec=True)


@pytest.fixture(scope='function')
def rc_mounts_mounts(mocker):
    return mocker.patch('resFsLinux.rcMounts.Mounts')


@pytest.fixture(scope='function')
def label(mocker):
    return mocker.patch.object(Mount, 'label', autospec=True)


@pytest.fixture(scope='function')
def remove_deeper_mounts(mocker):
    return mocker.patch.object(Mount, 'remove_deeper_mounts', autospec=True)


@pytest.fixture(scope='function')
def is_up(mocker):
    return mocker.patch.object(Mount, 'is_up', autospec=True)


@pytest.fixture(scope='function')
def try_umount(mocker):
    return mocker.patch.object(Mount, 'try_umount', autospec=True)


@pytest.fixture(scope='function')
def stat(mocker):
    return mocker.patch('resFsLinux.os.stat', autospec=True)


@pytest.mark.usefixtures('label', 'log', 'remove_deeper_mounts', 'rc_mounts_mounts')
@pytest.mark.ci
class TestStop:
    @staticmethod
    @pytest.mark.parametrize('errno', [5, 13])
    def test_call_try_umount_even_if_stat_raises_io_errors(try_umount, is_up, stat, errno):
        is_up.return_value = True
        stat.side_effect = OSError(errno, "")
        try_umount.return_value = 0

        Mount(mount_point='/tmp/foo').stop()

        assert try_umount.call_count == 1

    @staticmethod
    def test_stop_does_not_call_try_umount_if_not_up(is_up, try_umount):
        is_up.return_value = False

        Mount(mount_point='/tmp/foo').stop()

        assert try_umount.call_count == 0
