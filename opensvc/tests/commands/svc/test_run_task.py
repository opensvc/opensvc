import os
import pytest

from commands.svc import Mgr
from env import Env


def assert_run_cmd_success(svcname, svc_cmd_args):
    cmd_args = ["-s", svcname] + svc_cmd_args
    print('--------------')
    print('run Mgr()(argv=%s)' % cmd_args)
    assert Mgr()(argv=cmd_args) == 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('has_euid_0')
class TestRun:
    @staticmethod
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
    def test_cmd(mocker, osvc_path_tests, service_kws, success_flags):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {'OSVC_DETACHED': '1'})
        assert_run_cmd_success(svcname, ['create'] + service_kws)
        assert_run_cmd_success(svcname, ['run', '--rid', 'task#1', '--local'])
        for name in success_flags:
            assert os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, name))

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
    def test_hooks(mocker, osvc_path_tests, service_hooks, success_flags):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {'OSVC_DETACHED': '1'})
        # noinspection PyTypeChecker
        args = ['create', '--kw', 'task#1.command=/usr/bin/touch {private_var}/task_cmd'] + service_hooks
        assert_run_cmd_success(svcname, args)
        assert_run_cmd_success(svcname, ['run', '--rid', 'task#1', '--local'])
        if len(success_flags) == 0:
            assert not os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, 'task_cmd'))
        else:
            for name in success_flags:
                assert os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, str(name)))
