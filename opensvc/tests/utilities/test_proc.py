import pytest

from utilities.proc import *


NON_LINUX_OS_LIST = {"SunOS", "Darwin", "FreeBSD", "HP-UX", "OSF1"}


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


@pytest.mark.ci
class TestGetUpdatedPreexecFn:
    @staticmethod
    @pytest.mark.parametrize('sysname', NON_LINUX_OS_LIST)
    def test_get_updated_preexec_fn_return_unchanged_func_on_non_linux_os(mock_sysname, sysname):
        mock_sysname(sysname)

        def func(foo):
            return foo

        assert get_updated_preexec_fn(func) is func

    @staticmethod
    def test_returned_function_call_reset_oom_score_adjv(mock_sysname, mocker):
        oom_score_adj_mock = mocker.patch("utilities.proc.oom_score_adj")
        mock_sysname('Linux')
        original = {"calls": 0}

        def func():
            original["calls"] = original["calls"] + 1

        updated_func = get_updated_preexec_fn(func)
        updated_func()
        assert original["calls"] == 1, "preexec_fn has not been called !"
        oom_score_adj_mock.assert_called_once_with(pid="self", value=0)


@pytest.mark.ci
class TestCall:
    @staticmethod
    def test_automatically_calls_get_updated_preexec_fn(mock_sysname, mocker):
        true_file = Env.syspaths.true
        mock_sysname("Linux")
        get_updated_preexec_fn_mock = mocker.patch("utilities.proc.get_updated_preexec_fn")
        assert call([true_file]) == (0, "", "")
        get_updated_preexec_fn_mock.assert_called_once_with(None)

    @staticmethod
    def test_automatically_calls_get_updated_preexec_fn_with_preexec_fn_arg(mock_sysname, mocker):
        true_file = Env.syspaths.true
        mock_sysname("Linux")

        def func():
            pass

        get_updated_preexec_fn_mock = mocker.patch("utilities.proc.get_updated_preexec_fn")
        assert call([true_file], preexec_fn=func) == (0, "", "")
        get_updated_preexec_fn_mock.assert_called_once_with(func)


@pytest.mark.ci
class TestLCall:
    @staticmethod
    def test_automatically_calls_get_updated_preexec_fn(mock_sysname, mocker):
        true_file = Env.syspaths.true
        mock_sysname("Linux")
        get_updated_preexec_fn_mock = mocker.patch("utilities.proc.get_updated_preexec_fn")
        assert lcall([true_file], logging) == 0
        get_updated_preexec_fn_mock.assert_called_once_with(None)

    @staticmethod
    def test_automatically_calls_get_updated_preexec_fn_with_preexec_fn_arg(mock_sysname, mocker):
        true_file = Env.syspaths.true
        mock_sysname("Linux")

        def func():
            pass

        get_updated_preexec_fn_mock = mocker.patch("utilities.proc.get_updated_preexec_fn")
        assert lcall([true_file], logging, preexec_fn=func) == 0
        get_updated_preexec_fn_mock.assert_called_once_with(func)
