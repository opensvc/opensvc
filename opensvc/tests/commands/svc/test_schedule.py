import datetime
import json
import os
try:
    # noinspection PyCompatibility
    from unittest.mock import ANY
except ImportError:
    # noinspection PyUnresolvedReferences
    from mock import ANY

import pytest

from commands.svc import Mgr


def get_action(schedules, action):
    return [schedule for schedule in schedules if schedule["action"] == action][0]


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestPrintSchedule:
    @staticmethod
    def test_define_correct_default_status_schedule_of_10(mocker, capsys):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {"OSVC_DETACHED": "1"})
        assert Mgr()(argv=["-s", svcname, "create"]) == 0
        assert Mgr()(argv=["-s", svcname, "print", "schedule"]) == 0
        capsys.readouterr()
        assert Mgr()(argv=["-s", svcname, "print", "schedule", "--format", "json"]) == 0
        assert get_action(json.loads(capsys.readouterr().out), "status") == {
            "action": "status",
            "config_parameter": "DEFAULT.status_schedule",
            "last_run": "-",
            "next_run": ANY,
            "schedule_definition": "@10"
        }

    @staticmethod
    @pytest.mark.parametrize('schedule_def', ['@2', '@2s', '@60'])
    def test_define_correct_custom_status_schedule(mocker, capsys, schedule_def):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {"OSVC_DETACHED": "1"})
        with capsys.disabled():
            assert Mgr()(argv=["-s", svcname, "create", "--kw", "status_schedule=%s" % schedule_def]) == 0
            assert Mgr()(argv=["-s", svcname, "print", "schedule"]) == 0
            assert Mgr()(argv=["-s", svcname, "print", "schedule", "--format", "json"]) == 0
        assert Mgr()(argv=["-s", svcname, "print", "schedule", "--format", "json"]) == 0
        action = get_action(json.loads(capsys.readouterr().out), "status")
        assert action == {
            "action": "status",
            "config_parameter": "DEFAULT.status_schedule",
            "last_run": "-",
            "next_run": ANY,
            "schedule_definition": schedule_def
        }
        # ensure correct datetime format
        datetime.datetime.strptime(action['next_run'], "%Y-%m-%d %H:%M:%S")
