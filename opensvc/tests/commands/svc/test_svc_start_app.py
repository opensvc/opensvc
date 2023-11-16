import pytest
import time

import env
from commands.svc import Mgr


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
@pytest.mark.usefixtures("has_euid_0")
class TestStartApp:
    @staticmethod
    def test_short_wait_for_app_up_when_no_checker(demote):
        svcname = "pytest"
        create_args = [
            "-s", svcname,
            "create",
            "--kw", "start_timeout=4",
            "--kw", "app#1.start=%s" % env.Env.syspaths.true,
        ]
        assert Mgr()(argv=create_args) == 0
        begin = time.time()
        exit_code = Mgr()(argv=["-s", svcname, "start", "--local"])

        assert time.time() - begin < 3.9, \
            "wait for resource status up should return earlier when no checker"

        assert exit_code == 0, (
            "start app should have exit code 0"
            " when start command is .../bin/true"
            " and no checker"
        )

    @staticmethod
    def test_exit_code_1_when_start_command_exit_non_0(demote):
        svcname = "pytest"
        create_args = [
            "-s", svcname,
            "create",
            "--kw", "app#1.start=%s" % env.Env.syspaths.false,
        ]
        assert Mgr()(argv=create_args) == 0
        assert Mgr()(argv=["-s", svcname, "start", "--local"]) == 1, (
            "start app should have exit code 0"
            " when start command is .../bin/true"
            " and no checker"
        )
