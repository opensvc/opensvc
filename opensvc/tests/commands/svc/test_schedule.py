import json
import os
import pytest

from commands.svc import Mgr


def get_action(schedules, action):
    return [schedule for schedule in schedules if schedule["action"] == action][0]


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestPrintSchedule:
    @staticmethod
    def test_define_correct_default_status_schedule_of_10(mocker, tmp_file, capture_stdout):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {"OSVC_DETACHED": "1"})
        assert Mgr()(argv=["-s", svcname, "create"]) == 0
        assert Mgr()(argv=["-s", svcname, "print", "schedule"]) == 0
        with capture_stdout(tmp_file):
            assert Mgr()(argv=["-s", svcname, "print", "schedule", "--format", "json"]) == 0
        assert get_action(json.load(open(tmp_file, "r")), "status") == {
            "action": "status",
            "config_parameter": "DEFAULT.status_schedule",
            "last_run": "-",
            "schedule_definition": "@10"
        }

    @staticmethod
    def test_define_correct_custom_status_schedule(mocker, tmp_file, capture_stdout):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {"OSVC_DETACHED": "1"})
        assert Mgr()(argv=["-s", svcname, "create", "--kw", "status_schedule=@2"]) == 0
        assert Mgr()(argv=["-s", svcname, "print", "schedule"]) == 0
        with capture_stdout(tmp_file):
            assert Mgr()(argv=["-s", svcname, "print", "schedule", "--format", "json"]) == 0
        assert get_action(json.load(open(tmp_file, "r")), "status") == {
            "action": "status",
            "config_parameter": "DEFAULT.status_schedule",
            "last_run": "-",
            "schedule_definition": "@2"
        }
