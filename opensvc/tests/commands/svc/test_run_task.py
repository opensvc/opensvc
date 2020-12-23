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
from env import Env


def assert_run_cmd_success(svcname, svc_cmd_args):
    cmd_args = ["-s", svcname] + svc_cmd_args
    print('--------------')
    print('run Mgr()(argv=%s)' % cmd_args)
    assert Mgr()(argv=cmd_args) == 0


@pytest.fixture(scope='function')
def preexec_factory(mocker):
    return mocker.patch('drivers.resource.app.preexec', autospec=True)


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('has_euid_0')
class TestRun:
    @staticmethod
    @pytest.mark.usefixtures('preexec_factory')
    @pytest.mark.parametrize('service_kws, success_flags', [
        [['--kw', 'task#1.command=/usr/bin/touch {private_var}/simple_cmd'], ['simple_cmd']],
        [['--kw', 'task#1.command=/usr/bin/touch {private_var}/complex_cmd1'
                  ' && /usr/bin/touch {private_var}/complex_cmd2'],
         ['complex_cmd1', 'complex_cmd2']],
        [['--kw', 'task#1.command=/usr/bin/touch {private_var}/complex_cmd1'
                  ';/usr/bin/touch {private_var}/complex_cmd2'],  # no space before ';' to avoid comment
         ['complex_cmd1', 'complex_cmd2']],
        [['--kw', 'task#1.command=/usr/bin/touch {private_var}/complex_cmd1'
                  '; /usr/bin/touch {private_var}/complex_cmd2'],
         ['complex_cmd1', 'complex_cmd2']],
    ])
    def test_cmd(osvc_path_tests, service_kws, success_flags):
        svcname = "pytest"
        assert_run_cmd_success(svcname, ['create'] + service_kws)
        assert_run_cmd_success(svcname, ['run', '--rid', 'task#1', '--local'])
        for name in success_flags:
            assert os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, name))

    @staticmethod
    @pytest.mark.parametrize('task_kws, expected_preexec_factory_args', [
        (["group=1"], [(0, 1, {}, None)]),
        (["umask=0644"], [(0, 0, {}, int('0644', 8))]),
        (["user=1", "group=2"], [(1, 2, {}, None)]),
        (["user=1", "group=2", "umask=0600"], [(1, 2, {}, int("0600", 8))]),
    ])
    def test_cmd_use_expected_prexec_function(
            preexec_factory,
            task_kws,
            expected_preexec_factory_args):
        svcname = "pytest"
        service_kws = ['--kw', 'task#1.command=%s' % Env.syspaths.true]
        for kw in task_kws:
            service_kws.extend(['--kw', 'task#1.%s' % kw])
        assert_run_cmd_success(svcname, ['create'] + service_kws)
        assert_run_cmd_success(svcname, ['run', '--rid', 'task#1', '--local'])
        assert preexec_factory.call_args_list[0] == expected_preexec_factory_args
        assert preexec_factory.call_count == 1

    @staticmethod
    @pytest.mark.parametrize('service_hooks, success_flags', [
        [['--kw', 'task#1.pre_run=/usr/bin/touch {private_var}/pre_run', ], ['task_cmd', 'pre_run']],
        [['--kw', 'task#1.post_run=/usr/bin/touch {private_var}/post_run1'
                  ' &&  /usr/bin/touch {private_var}/post_run2', ],
         ['task_cmd', 'post_run1', 'post_run2']],
        [['--kw', 'task#1.blocking_pre_run=%s' % Env.syspaths.false], []],
        [['--kw', 'task#1.blocking_post_run=/usr/bin/touch {private_var}/post_run && %s' % Env.syspaths.false],
         ['task_cmd', 'post_run']],  # because optional is True by default cmd will succeed
    ])
    @pytest.mark.usefixtures('preexec_factory')
    def test_hooks(osvc_path_tests, service_hooks, success_flags):
        svcname = "pytest"

        # noinspection PyTypeChecker
        args = ['create', '--kw', 'task#1.command=/usr/bin/touch {private_var}/task_cmd'] + service_hooks
        assert_run_cmd_success(svcname, args)
        assert_run_cmd_success(svcname, ['run', '--rid', 'task#1', '--local'])
        if len(success_flags) == 0:
            assert not os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, 'task_cmd'))
        else:
            for name in success_flags:
                assert os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, str(name)))

    @staticmethod
    def test_ensure_service_default_hook_is_run_only_once_even_multiple_resources(
            mocker,
    ):
        svcname = "pytest"
        lcall_svc = mocker.patch('core.objects.svc.lcall', return_value=0)
        lcall_task = mocker.patch('drivers.resource.task.lcall', return_value=0)
        mocker.patch.dict(os.environ, {'OSVC_DETACHED': '1'})
        args = ['create',
                '--kw', 'DEFAULT.pre_run=/default_pre_run',
                '--kw', 'DEFAULT.post_run=/default_post_run',
                '--kw', 'task#1.command=/task1',
                '--kw', 'task#2.command=/task2',
                ]
        assert_run_cmd_success(svcname, args)
        assert_run_cmd_success(svcname, ['run', '--local'])
        assert lcall_svc.call_count == 2
        assert lcall_svc.call_args_list[0][0] == (['/default_pre_run'],)
        assert lcall_svc.call_args_list[1][0] == (['/default_post_run'],)
        assert lcall_task.call_count == 2
        assert set([lcall_task.call_args_list[0][0][0][0],
                    lcall_task.call_args_list[1][0][0][0]]) == set(['/task1', '/task2'])

    @staticmethod
    def test_define_correct_schedule_with_next_run_immediate_when_no_last_run(tmp_file, capture_stdout):
        svcname = "pytest"
        assert_run_cmd_success(svcname, ["create",
                                         "--kw", "task#1.command=/usr/bin/date",
                                         "--kw", "task#1.schedule=@3"])
        assert_run_cmd_success(svcname, ["print", "schedule"])
        with capture_stdout(tmp_file):
            assert Mgr()(argv=["-s", svcname, "print", "schedule", "--format", "json"]) == 0
        now = datetime.datetime.now()
        schedule_run = [schedule for schedule in json.load(open(tmp_file, "r")) if schedule["action"] == "run"][0]
        assert schedule_run == {
            "action": "run",
            "config_parameter": "task#1.schedule",
            "last_run": "-",
            "next_run": ANY,
            "schedule_definition": "@3"
        }
        delta = now - datetime.datetime.strptime(schedule_run['next_run'], "%Y-%m-%d %H:%M:%S")
        assert delta < datetime.timedelta(seconds=1)

    @staticmethod
    def test_define_correct_schedule_when_has_last_run(mocker, capsys):
        svcname = "pytest"
        with capsys.disabled():
            assert_run_cmd_success(svcname, ["create",
                                             "--kw", "task#1.command=/usr/bin/date",
                                             "--kw", "task#1.schedule=@3"])
        now = datetime.datetime.now()
        mocker.patch('core.scheduler.Scheduler.get_last', return_value=now)
        assert Mgr()(argv=["-s", svcname, "print", "schedule", "--format", "json"]) == 0
        schedule_run = [schedule for schedule in json.loads(capsys.readouterr().out) if schedule["action"] == "run"][0]
        assert schedule_run == {
            "action": "run",
            "config_parameter": "task#1.schedule",
            "last_run": now.strftime("%Y-%m-%d %H:%M:%S"),
            "next_run": ANY,
            "schedule_definition": "@3"
        }
        delta = datetime.datetime.strptime(schedule_run['next_run'], "%Y-%m-%d %H:%M:%S") - now
        assert datetime.timedelta(seconds=2 * 60) < delta < datetime.timedelta(seconds=3 * 60, microseconds=1)

    @staticmethod
    @pytest.mark.parametrize('schedule_def', ["@%ss" % s for s in range(2, 13)])
    def test_define_correct_schedule_when_has_last_run_and_schedule_at_seconds(mocker, capsys, schedule_def):
        svcname = "pytest"
        with capsys.disabled():
            assert_run_cmd_success(svcname, ["create",
                                             "--kw", "task#1.command=/usr/bin/date",
                                             "--kw", "task#1.schedule=%s" % schedule_def])
        now = datetime.datetime.now()
        seconds = int(schedule_def[1:-1])
        mocker.patch('core.scheduler.Scheduler.get_last', return_value=now)
        assert Mgr()(argv=["-s", svcname, "print", "schedule", "--format", "json"]) == 0
        schedule_run = [schedule for schedule in json.loads(capsys.readouterr().out) if schedule["action"] == "run"][0]
        delta = datetime.datetime.strptime(schedule_run['next_run'], "%Y-%m-%d %H:%M:%S") - now
        assert datetime.timedelta(seconds=seconds-1) <= delta
        assert delta < datetime.timedelta(seconds=seconds, microseconds=1)
