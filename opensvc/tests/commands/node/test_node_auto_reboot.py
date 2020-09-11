import os

import pytest
import commands.node
from core.node import Env, Node


@pytest.mark.ci
@pytest.mark.usefixtures('has_euid_0', 'osvc_path_tests')
class TestNodemgrAutoReboot:
    @staticmethod
    @pytest.mark.parametrize(
        'hook, value, reboot_counts, expected_exit_code', [
            ['blocking_pre', Env.syspaths.true, 1, 0],
            ['blocking_pre', Env.syspaths.false, 0, 1],
            ['blocking_pre', "%s; %s" % (Env.syspaths.true, Env.syspaths.true), 1, 0],
            ['blocking_pre', "%s || %s" % (Env.syspaths.false, Env.syspaths.true), 1, 0],
            ['blocking_pre', "%s && %s" % (Env.syspaths.true, Env.syspaths.true), 1, 0],
            ['blocking_pre', "%s && %s" % (Env.syspaths.true, Env.syspaths.false), 0, 1],
            ['blocking_pre', "%s; %s" % (Env.syspaths.true, Env.syspaths.false), 0, 1],
            ['pre', Env.syspaths.true, 1, 0],
            ['pre', Env.syspaths.false, 1, 0],
            ['pre', "%s && %s" % (Env.syspaths.true, Env.syspaths.false), 1, 0],
        ])
    def test_respect_hook_result(
            mocker,
            has_node_config,
            hook,
            value,
            reboot_counts,
            expected_exit_code):
        _reboot = mocker.patch.object(Node, '_reboot')
        open(Node().paths.reboot_flag, 'w+')
        mocker.patch('core.node.node.assert_file_is_root_only_writeable')
        assert commands.node.main(argv=["set", "--kw", "reboot.%s=%s" % (hook, value)]) == 0
        assert commands.node.main(argv=["auto", "reboot"]) == expected_exit_code
        assert _reboot.call_count == reboot_counts

    @staticmethod
    @pytest.mark.parametrize(
        'once_value, remove_reboot_flag', [
            [None, True],
            [True, True],
            [False, False]])
    def test_respect_reboot_once_keyword(mocker, once_value, remove_reboot_flag):
        mocker.patch.object(Node, '_reboot')
        reboot_flag = Node().paths.reboot_flag
        open(reboot_flag, 'w+')
        mocker.patch('core.node.node.assert_file_is_root_only_writeable')
        if once_value is not None:
            assert commands.node.main(argv=["set", "--kw", "reboot.once=%s" % once_value]) == 0
        assert commands.node.main(argv=["auto", "reboot"]) == 0
        if remove_reboot_flag:
            assert not os.path.exists(reboot_flag)
        else:
            assert os.path.exists(reboot_flag)
