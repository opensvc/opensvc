# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import sys
import json
import logging
import pytest

try:
    # noinspection PyCompatibility
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import nodemgr

UNICODE_STRING = "bêh"
logging.disable(logging.CRITICAL)


@pytest.fixture(scope='function')
def has_privs(mocker):
    mocker.patch('node.check_privs', return_value=None)


@pytest.fixture(scope='function')
def parse_args(mocker):
    parse_args = mocker.patch('nodemgr.NodemgrOptParser.parse_args',
                              return_value=(mocker.Mock(symcli_db_file=None),
                                            'my_action'))
    return parse_args


@pytest.fixture(scope='function')
def node(mocker):
    node = mocker.patch('nodemgr.node_mod.Node', autospec=True).return_value
    node.options = dict()
    node.action.return_value = 0
    return node


@pytest.mark.ci
@pytest.mark.usefixtures('has_privs', 'osvc_path_tests')
class TestNodemgr:
    @staticmethod
    @pytest.mark.parametrize('action_return_value', [0, 13])
    def test_it_call_once_node_action_and_returns_node_action_return_value(node, parse_args, action_return_value):
        node.action.return_value = action_return_value

        ret = nodemgr.main(argv=["my_action", "--format", "json"])

        assert ret == action_return_value
        node.action.assert_called_once_with('my_action')

    @staticmethod
    def test_get_extra_argv():
        assert nodemgr.get_extra_argv(["hello", 'world']) == (['hello', 'world'], [])

    @staticmethod
    def test_get_extra_argv_when_array():
        assert nodemgr.get_extra_argv(["array", '--', 'value=1']) == (['array', '--'], ['value=1'])
        assert nodemgr.get_extra_argv(["array", 'value=1']) == (['array'], ['value=1'])
        assert nodemgr.get_extra_argv(["myaction", 'value=1']) == (['myaction', 'value=1'], [])

    @staticmethod
    def test_print_schedule():
        """
        Print node schedules
        """
        ret = nodemgr.main(argv=["print", "schedule"])
        assert ret == 0

    @staticmethod
    def test_print_schedule_json():
        """
        Print node schedules (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["print", "schedule", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        schedules = json.loads(output)

        assert ret == 0
        assert isinstance(schedules, list)
        assert len(schedules) > 0

    def test_print_config(self):
        """
        Print node config
        """
        ret = nodemgr.main(argv=["print", "config"])
        assert ret == 0

    @staticmethod
    def test_print_config_json():
        """
        Print node config (json format)
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["print", "config", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        config = json.loads(output)

        assert ret == 0
        assert isinstance(config, dict)

    @staticmethod
    @pytest.mark.parametrize('get_set_arg', ['--param', '--kw'])
    def test_set_get_unset_some_env_value(get_set_arg):
        ret = nodemgr.main(argv=["set", "--param", "env.this_is_test", "--value", "true"])
        assert ret == 0

        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["get", get_set_arg, "env.this_is_test"])
            assert ret == 0
            from rcUtilities import try_decode
            output = out.getvalue().strip()
            assert try_decode(output) == "true"

            ret = nodemgr.main(argv=["unset", get_set_arg, "env.this_is_test"])
            assert ret == 0
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["get", get_set_arg, "env.this_is_test"])
            assert ret == 0
            assert out.getvalue().strip() == "None"
        finally:
            sys.stdout = _stdout

    @staticmethod
    def test_set_env_comment():
        """
        Set node env.comment to a unicode string
        """
        ret = nodemgr.main(argv=["set", "--param", "env.comment", "--value", UNICODE_STRING])
        assert ret == 0

    @pytest.mark.skip
    def test_get_env_comment(self):
        """
        Get node env.comment
        """
        _stdout = sys.stdout

        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["get", "--param", "env.comment"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        from rcUtilities import try_decode
        print(output)

        assert ret == 0
        assert try_decode(output) == UNICODE_STRING

    @staticmethod
    def test_043_unset():
        """
        Unset env.comment
        """
        ret = nodemgr.main(argv=["unset", "--param", "env.comment"])
        assert ret == 0

    @pytest.mark.skip
    def test_044_get_not_found(self):
        """
        Get an unset keyword
        """
        _stderr = sys.stdout

        try:
            err = StringIO()
            sys.stderr = err
            ret = nodemgr.main(argv=["get", "--param", "env.comment"])
        finally:
            sys.stderr = _stderr

        assert ret == 1

    @staticmethod
    def test_checks_return_0():
        """
        Run node checks
        """
        ret = nodemgr.main(argv=["checks"])
        assert ret == 0

    @staticmethod
    def test_sysreport():
        ret = nodemgr.main(argv=["sysreport"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_pushasset_return_0():
        ret = nodemgr.main(argv=["pushasset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_08_nodemgr_collect_stats():
        """
        Run node collect stats
        """
        ret = nodemgr.main(argv=["collect_stats"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_09_nodemgr_pushstats():
        """
        Run node pushstats
        """
        ret = nodemgr.main(argv=["pushstats"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_10_nodemgr_pushpkg():
        """
        Run node pushpkg
        """
        ret = nodemgr.main(argv=["pushpkg"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_11_nodemgr_pushpatch():
        """
        Run node pushpatch
        """
        ret = nodemgr.main(argv=["pushpatch"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_12_nodemgr_pushdisks():
        """
        Run node pushdisks
        """
        ret = nodemgr.main(argv=["pushdisks"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_131_nodemgr_schedule_reboot():
        """
        Run schedule reboot
        """
        ret = nodemgr.main(argv=["schedule", "reboot"])
        assert ret == 0

    @staticmethod
    # @pytest.mark.skip
    def test_132_nodemgr_unschedule_reboot():
        """
        Run unschedule reboot
        """
        ret = nodemgr.main(argv=["unschedule", "reboot"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_133_nodemgr_print_reboot_status():
        """
        Print reboot schedule status
        """
        ret = nodemgr.main(argv=["schedule", "reboot", "status"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_14_nodemgr_logs():
        """
        Print node logs
        """
        ret = nodemgr.main(argv=["logs"])
        assert ret == 0

    @staticmethod
    def test_network_ls():
        """
        List node networks
        """
        ret = nodemgr.main(argv=["network", "ls"])
        assert ret == 0

    @staticmethod
    def test_network_ls_json():
        """
        List node networks (json format)
        """
        _stdout = sys.stdout
        nodemgr.main(argv=["network", "ls", "--format", "json", "--color", "no"])
        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["network", "ls", "--format", "json", "--color", "no"])
            output = out.getvalue().strip()
        finally:
            sys.stdout = _stdout

        assert ret == 0
        assert isinstance(json.loads(output), dict)

    @staticmethod
    @pytest.mark.skip
    def test_161_nodemgr_print_devs():
        """
        Print node device tree
        """
        ret = nodemgr.main(argv=["print", "devs"])
        assert ret == 0

    @staticmethod
    def test_prkey_create_initial_value_when_absent():
        _stdout = sys.stdout
        try:
            out = StringIO()
            sys.stdout = out
            ret = nodemgr.main(argv=["prkey"])
            assert ret == 0
            assert out.getvalue().startswith('0x')
        finally:
            sys.stdout = _stdout

    @staticmethod
    def test_prkey_show_existing_prkey():
        _stdout = sys.stdout
        nodemgr.main(argv=['set', '--kw', 'node.prkey=0x8796759710111'])
        try:
            out = StringIO()
            sys.stdout = out
            assert nodemgr.main(argv=["prkey"]) == 0
            assert out.getvalue().strip() == '0x8796759710111'
        finally:
            sys.stdout = _stdout

    @staticmethod
    @pytest.mark.skip
    def test_163_nodemgr_dequeue_actions():
        """
        Dequeue actions
        """
        ret = nodemgr.main(argv=["dequeue", "actions"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_164_nodemgr_scan_scsi():
        """
        Scan scsi buses
        """
        ret = nodemgr.main(argv=["scanscsi"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_164_nodemgr_collector_networks():
        """
        Collector networks
        """
        ret = nodemgr.main(argv=["collector", "networks"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_164_nodemgr_collector_search():
        """
        Collector search
        """
        ret = nodemgr.main(argv=["collector", "search", "--like", "safe:%"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0251_compliance():
        """
        Node compliance auto
        """
        ret = nodemgr.main(argv=["compliance", "auto"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0252_compliance():
        """
        Node compliance check
        """
        ret = nodemgr.main(argv=["compliance", "check"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0253_compliance():
        """
        Node compliance fix
        """
        ret = nodemgr.main(argv=["compliance", "fix"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0254_compliance():
        """
        Node compliance show moduleset
        """
        ret = nodemgr.main(argv=["compliance", "show", "moduleset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0255_compliance():
        """
        Node compliance list moduleset
        """
        ret = nodemgr.main(argv=["compliance", "list", "moduleset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0256_compliance():
        """
        Node compliance show ruleset
        """
        ret = nodemgr.main(argv=["compliance", "show", "ruleset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0257_compliance():
        """
        Node compliance list ruleset
        """
        ret = nodemgr.main(argv=["compliance", "list", "ruleset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0258_compliance():
        """
        Node compliance attach
        """
        ret = nodemgr.main(argv=["compliance", "attach", "--ruleset", "abcdef", "--moduleset", "abcdef"])
        assert ret == 1

    @staticmethod
    @pytest.mark.skip
    def test_0259_compliance():
        """
        Node compliance detach
        """
        ret = nodemgr.main(argv=["compliance", "detach", "--ruleset", "abcdef", "--moduleset", "abcdef"])
        assert ret == 0
