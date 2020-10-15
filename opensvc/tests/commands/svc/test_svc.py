import json
import pytest

from commands.svc import Mgr


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
        mock_sysname("linux")
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
