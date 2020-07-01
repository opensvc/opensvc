import pytest

from utilities.proc import *


@pytest.mark.ci
class TestCheckPrivs:
    @staticmethod
    @pytest.mark.parametrize('euid', range(1, 10))
    def test_exit_1_when_geteuid_is_no_0(mocker, euid):
        mocker.patch('os.geteuid', return_value=euid)
        sys_exit = mocker.patch.object(sys, 'exit', side_effect=Exception("exit"))
        with pytest.raises(Exception, match="exit"):
            check_privs()
        sys_exit.assert_called_once_with(1)

    @staticmethod
    @pytest.mark.parametrize('euid', range(1, 200))
    def test_pass_when_geteuid_is_0(mocker, euid):
        mocker.patch('os.geteuid', return_value=0)
        check_privs()
