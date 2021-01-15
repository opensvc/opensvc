import os
import pytest

from commands.svc import Mgr
import utilities.proc


def assert_run_cmd_success(svcname, svc_cmd_args):
    cmd_args = ["-s", svcname] + svc_cmd_args
    print('--------------')
    print('run Mgr()(argv=%s)' % cmd_args)
    assert Mgr()(argv=cmd_args) == 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('has_euid_0')
class TestSvcHooks:
    @staticmethod
    @pytest.mark.parametrize(
        'prefix',
        ['pre', 'post', 'blocking_pre', 'blocking_post']
    )
    @pytest.mark.parametrize(
        'action',
        [
            'provision',
            'unprovision',
            'start',
            'startstandby',
            'stop',
            'sync_nodes',
            'sync_drp',
            # 'sync_all', no sync_all keyword defined yet
            'sync_resync',
            'sync_update',
            'sync_restore',
            'run',
        ]
    )
    def test_call_one_action_hook(mocker, osvc_path_tests, action, prefix):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {'OSVC_DETACHED': '1'})
        hook = '%s_%s' % (prefix, action)
        assert_run_cmd_success(svcname, ['create', '--kw', 'DEFAULT.%s=/usr/bin/touch {private_var}/%s' % (hook, hook)])
        assert_run_cmd_success(svcname, [action, '--local', '--debug'])
        assert os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, hook))

    @staticmethod
    @pytest.mark.parametrize(
        'prefix',
        ['pre', 'post', 'blocking_pre', 'blocking_post']
    )
    def test_call_shutdown_action_stop_hook(mocker, osvc_path_tests, prefix):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {'OSVC_DETACHED': '1'})
        hook = '%s_stop' % prefix
        assert_run_cmd_success(svcname, ['create', '--kw', 'DEFAULT.%s=/usr/bin/touch {private_var}/%s' % (hook, hook)])
        assert_run_cmd_success(svcname, ['shutdown', '--local', '--debug'])
        assert os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, hook))

    @staticmethod
    @pytest.mark.parametrize(
        'action',
        ['start', 'stop', 'startstandby', 'provision', 'unprovision']
    )
    def test_cmd_all_action_hooks(mocker, osvc_path_tests, action):
        svcname = "pytest"
        mocker.patch.dict(os.environ, {'OSVC_DETACHED': '1'})
        service_hooks = ["%s_%s" % (prefix, action) for prefix in ['pre', 'blocking_pre', 'post', 'blocking_post']]
        create_args = ['create']
        for name in service_hooks:
            create_args.extend(['--kw', 'DEFAULT.%s=/usr/bin/touch {private_var}/%s' % (name, name)])
        assert_run_cmd_success(svcname, create_args)
        assert_run_cmd_success(svcname, [action, '--local', '--debug'])
        for name in service_hooks:
            assert os.path.exists(os.path.join(str(osvc_path_tests), 'var', 'svc', svcname, str(name)))
