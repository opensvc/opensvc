import pytest

from commands.usr import Mgr


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestUsr:
    @staticmethod
    def test_can_create_user_with_password():
        user = 'system/usr/test'
        assert Mgr()(argv=["-s", user, "create"]) == 0
        assert Mgr()(argv=["-s", user, "add", "--key", "password", "--value", "apass"]) == 0

    @staticmethod
    def test_can_get_user_keys(capsys):
        user = 'system/usr/test'
        assert Mgr()(argv=["-s", user, "create"]) == 0
        assert Mgr()(argv=["-s", user, "add", "--key", "password", "--value", "pass1"]) == 0
        capsys.readouterr()
        assert Mgr()(argv=["-s", user, "keys"]) == 0
        assert capsys.readouterr().out.strip() == "password"

    @staticmethod
    def test_can_get_user_password(capsys):
        user = 'system/usr/test'
        assert Mgr()(argv=["-s", user, "create"]) == 0
        assert Mgr()(argv=["-s", user, "add", "--key", "password", "--value", "pass2"]) == 0
        capsys.readouterr()
        assert Mgr()(argv=["-s", user, "decode", "--key", "password"]) == 0
        assert capsys.readouterr().out.strip() == "pass2"
