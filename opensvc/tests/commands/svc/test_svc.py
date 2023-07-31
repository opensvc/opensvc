import json
import pytest

from commands.svc import Mgr


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestPurgeLocal:
    @staticmethod
    def test_object_is_not_recreated_after_local_purge():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create"]) == 0
        assert Mgr()(argv=["-s", svcname, "ls"]) == 0
        assert Mgr()(argv=["-s", svcname, "purge", "--local"]) == 0
        assert Mgr()(argv=["-s", svcname, "ls"]) > 0


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestCreateWithKw:
    @staticmethod
    def test_create_id_refused_when_config_is_not_valid_env():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo={bar}", "--kw", "env.bar={foo}"]) > 0
        assert Mgr()(argv=["-s", svcname, "ls"]) > 0

    @staticmethod
    def test_create_is_accepted_when_config_has_valid_env():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo={bar}", "--kw", "env.baz={foo}"]) == 0
        assert Mgr()(argv=["-s", svcname, "ls"]) == 0

    @staticmethod
    def test_create_correctly_set_env_keywords(capsys):
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=00BAB10C", "--kw", "env.bar=BADDCAFE"]) == 0
        for kw, value in {"env.foo": "00BAB10C", "env.bar": "BADDCAFE"}.items():
            capsys.readouterr()
            assert Mgr()(argv=["-s", svcname, "get", "--kw", kw]) == 0
            assert capsys.readouterr().out.strip() == value


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestSet:
    @staticmethod
    def test_refuse_dry_run():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=bar"]) == 0
        assert Mgr()(argv=["-s", svcname, "set", "--kw", "env.foo=dry_run", "--dry-run", "--local"]) == 1

    @staticmethod
    def test_update_service_config(tmp_file, capture_stdout):
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=bar"]) == 0
        assert Mgr()(argv=["-s", svcname, "set", "--kw", "env.foo=BAR", "--local"]) == 0
        with capture_stdout(tmp_file):
            assert Mgr()(argv=["-s", svcname, "print", "config", "--format", "json"]) == 0
        assert json.load(open(tmp_file, "r"))["env"]["foo"] == "BAR"

    @staticmethod
    def test_set_invalid_env_values_is_refused():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create"]) == 0
        assert Mgr()(argv=["-s", svcname, "set", "--kw", "env.foo={bar}", "--kw", "env.bar={foo}"]) > 0

    @staticmethod
    def test_set_valid_env_values_is_accepted():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create"]) == 0
        assert Mgr()(argv=["-s", svcname, "set", "--kw", "env.foo={bar}", "--kw", "env.bar=something"]) == 0


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestUnset:
    @staticmethod
    def test_refuse_dry_run():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=bar"]) == 0
        assert Mgr()(argv=["-s", svcname, "unset", "--kw", "env.foo", "--dry-run", "--local"]) == 1

    @staticmethod
    def test_update_config(tmp_file, capture_stdout):
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=bar"]) == 0
        assert Mgr()(argv=["-s", svcname, "unset", "--kw", "env.foo", "--local"]) == 0
        with capture_stdout(tmp_file):
            assert Mgr()(argv=["-s", svcname, "print", "config", "--format", "json"]) == 0
        assert "foo" not in json.load(open(tmp_file, "r"))["env"]


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestDeleteRid:
    @staticmethod
    def test_remove_section(tmp_file, capture_stdout):
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=bar"]) == 0
        assert Mgr()(argv=["-s", svcname, "delete", "--rid", "env", "--local"]) == 0
        with capture_stdout(tmp_file):
            assert Mgr()(argv=["-s", svcname, "print", "config", "--format", "json"]) == 0
        assert "env" not in json.load(open(tmp_file, "r"))

    @staticmethod
    def test_refuse_dry_run():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=bar"]) == 0
        assert Mgr()(argv=["-s", svcname, "delete", "--rid", "env", "--dry-run", "--local"]) == 1


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestDelete:
    @staticmethod
    def test_delete_service():
        svcname = "a-service"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=bar"]) == 0
        assert Mgr()(argv=["-s", svcname, "ls"]) == 0
        assert Mgr()(argv=["-s", svcname, "delete", "--local"]) == 0
        assert Mgr()(argv=["-s", svcname, "ls"]) == 1

    @staticmethod
    def test_refuse_dry_run():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "env.foo=bar"]) == 0
        assert Mgr()(argv=["-s", svcname, "delete", "--dry-run", "--local"]) == 1
        assert Mgr()(argv=["-s", svcname, "ls"]) == 0


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestUpdate:
    @staticmethod
    def test_it_updates_config(mock_sysname, tmp_file, capture_stdout):
        svcname = "pytest"
        mock_sysname("Linux")
        assert Mgr()(argv=["-s", svcname, "create"]) == 0

        assert Mgr()(argv=["-s", svcname, "update", "--resource", '{"rid": "fs#1", "type": "flag"}', "--local"]) == 0
        with capture_stdout(tmp_file):
            assert Mgr()(argv=["-s", svcname, "print", "config", "--format", "json"]) == 0
        assert json.load(open(tmp_file, "r"))["fs#1"] == {"type": "flag"}

    @staticmethod
    def test_refuse_dry_run():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create"]) == 0

        assert Mgr()(argv=["-s", svcname, "update", "--resource", '{"comment": "Comment"}', "--local",
                           "--dry-run"]) == 1

    @staticmethod
    def test_resource_option_is_not_preserved_on_next_mgr_call():
        assert Mgr()(argv=["-s", "svc1", "create"]) == 0
        assert Mgr()(argv=["-s", "svc1", "update", "--resource", '{"comment": "foo"}', "--local"]) == 0
        assert Mgr()(argv=["-s", "svc2", "create"]) == 0

    @staticmethod
    def test_update_allowed_without_resource():
        svcname = "pytest"
        assert Mgr()(argv=["-s", svcname, "create"]) == 0
        assert Mgr()(argv=["-s", svcname, "update", "--local"]) == 0
