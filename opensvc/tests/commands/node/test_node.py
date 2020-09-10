# coding: utf8

from __future__ import print_function
from __future__ import unicode_literals

import json

import pytest
import commands.node

from utilities.string import try_decode

UNICODE_STRING = "bêh"


@pytest.fixture(scope='function')
def node(mocker):
    node = mocker.patch.object(commands.node, 'Node', autospec=True).return_value
    node.options = dict()
    node.action.return_value = 0
    return node


@pytest.mark.ci
@pytest.mark.usefixtures('has_euid_0', 'osvc_path_tests')
class TestNodemgr:
    @staticmethod
    @pytest.mark.parametrize('action_return_value', [0, 13])
    def test_it_call_once_node_action_and_returns_node_action_return_value(mocker, node, action_return_value):
        node.action.return_value = action_return_value
        mocker.patch.object(commands.node.NodeOptParser,
                            'parse_args',
                            return_value=(mocker.Mock(symcli_db_file=None), 'my_action'))

        result = commands.node.main(argv=["my_action", "--format", "json"])
        assert result == action_return_value
        node.action.assert_called_once_with('my_action')

    @staticmethod
    def test_get_extra_argv():
        assert commands.node.get_extra_argv(["hello", 'world']) == (['hello', 'world'], [])

    @staticmethod
    def test_get_extra_argv_when_array():
        assert commands.node.get_extra_argv(["array", '--', 'value=1']) == (['array', '--'], ['value=1'])
        assert commands.node.get_extra_argv(["array", 'value=1']) == (['array'], ['value=1'])
        assert commands.node.get_extra_argv(["myaction", 'value=1']) == (['myaction', 'value=1'], [])

    @staticmethod
    def test_print_schedule():
        """
        Print node schedules
        """
        ret = commands.node.main(argv=["print", "schedule"])
        assert ret == 0

    @staticmethod
    def test_print_schedule_json(tmp_file, capture_stdout):
        """
        Print node schedules (json format)
        """
        with capture_stdout(tmp_file):
            ret = commands.node.main(argv=["print", "schedule", "--format", "json", "--color", "no"])

        assert ret == 0
        with open(tmp_file) as json_file:
            schedules = json.load(json_file)
        assert isinstance(schedules, list)
        assert len(schedules) > 0

    def test_print_config(self):
        """
        Print node config
        """
        ret = commands.node.main(argv=["print", "config"])
        assert ret == 0

    @staticmethod
    def test_print_config_json(tmp_file, capture_stdout):
        """
        Print node config (json format)
        """
        with capture_stdout(tmp_file):
            ret = commands.node.main(argv=["print", "config", "--format", "json", "--color", "no"])

        with open(tmp_file) as json_file:
            config = json.load(json_file)

        assert ret == 0
        assert isinstance(config, dict)

    @staticmethod
    @pytest.mark.parametrize('get_set_arg', ['--param', '--kw'])
    def test_set_get_unset_some_env_value(tmp_file, capture_stdout, get_set_arg):
        assert commands.node.main(argv=["set", "--param", "env.this_is_test", "--value", "true"]) == 0

        with capture_stdout(tmp_file):
            ret = commands.node.main(argv=["get", get_set_arg, "env.this_is_test"])
            assert ret == 0
        with open(tmp_file) as output_file:
            assert try_decode(output_file.read()).strip() == "true"

        ret = commands.node.main(argv=["unset", get_set_arg, "env.this_is_test"])
        assert ret == 0

        tmp_file_1 = tmp_file + '-1'
        with capture_stdout(tmp_file_1):
            ret = commands.node.main(argv=["get", get_set_arg, "env.this_is_test"])
            assert ret == 0
        with open(tmp_file_1) as output_file:
            assert output_file.read().strip() == "None"

    @staticmethod
    def test_set_env_comment():
        """
        Set node env.comment to a unicode string
        """
        ret = commands.node.main(argv=["set", "--param", "env.comment", "--value", UNICODE_STRING])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_get_env_comment(tmp_file, capture_stdout):
        """
        Get node env.comment
        """

        with capture_stdout(tmp_file):
            ret = commands.node.main(argv=["get", "--param", "env.comment"])

        assert ret == 0
        with open(tmp_file) as output_file:
            assert try_decode(output_file.read()) == UNICODE_STRING
            # assert try_decode(output_file.read()).strip() == UNICODE_STRING

    @staticmethod
    def test_unset_env_comment():
        assert commands.node.main(argv=["unset", "--param", "env.comment"]) == 0

    @pytest.mark.skip
    def test_044_get_not_found(self):
        """
        Get an unset keyword
        """
        assert commands.node.main(argv=["get", "--param", "env.comment"]) == 1

    @staticmethod
    def test_checks_return_0():
        """
        Run node checks
        """
        assert commands.node.main(argv=["checks"]) == 0

    @staticmethod
    def test_sysreport(mocker):
        from core.sysreport.sysreport import BaseSysReport
        send_sysreport = mocker.patch.object(BaseSysReport, 'sysreport')
        ret = commands.node.main(argv=["sysreport"])
        assert ret == 0
        assert send_sysreport.call_count == 1

    @staticmethod
    @pytest.mark.skip
    def test_pushasset_return_0():
        ret = commands.node.main(argv=["pushasset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_08_node_collect_stats():
        """
        Run node collect stats
        """
        ret = commands.node.main(argv=["collect_stats"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_09_node_pushstats():
        """
        Run node pushstats
        """
        ret = commands.node.main(argv=["pushstats"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_10_node_pushpkg():
        """
        Run node pushpkg
        """
        ret = commands.node.main(argv=["pushpkg"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_11_node_pushpatch():
        """
        Run node pushpatch
        """
        ret = commands.node.main(argv=["pushpatch"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_12_node_pushdisks():
        """
        Run node pushdisks
        """
        ret = commands.node.main(argv=["pushdisks"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_131_node_schedule_reboot():
        """
        Run schedule reboot
        """
        ret = commands.node.main(argv=["schedule", "reboot"])
        assert ret == 0

    @staticmethod
    # @pytest.mark.skip
    def test_132_node_unschedule_reboot():
        """
        Run unschedule reboot
        """
        ret = commands.node.main(argv=["unschedule", "reboot"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_133_node_print_reboot_status():
        """
        Print reboot schedule status
        """
        ret = commands.node.main(argv=["schedule", "reboot", "status"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_14_node_logs():
        """
        Print node logs
        """
        ret = commands.node.main(argv=["logs"])
        assert ret == 0

    @staticmethod
    def test_node_print_devs():
        """
        Print node device tree
        """
        assert commands.node.main(argv=["print", "devs"]) == 0

    @staticmethod
    def test_prkey_create_initial_value_when_absent(tmp_file, capture_stdout):
        with capture_stdout(tmp_file):
            ret = commands.node.main(argv=["prkey"])
        assert ret == 0
        with open(tmp_file) as std_out:
            assert std_out.read().startswith('0x')

    @staticmethod
    def test_prkey_show_existing_prkey(tmp_file, capture_stdout):
        commands.node.main(argv=['set', '--kw', 'node.prkey=0x8796759710111'])
        with capture_stdout(tmp_file):
            assert commands.node.main(argv=["prkey"]) == 0
        with open(tmp_file) as output_file:
            assert output_file.read().strip() == '0x8796759710111'

    @staticmethod
    @pytest.mark.skip
    def test_163_node_dequeue_actions():
        """
        Dequeue actions
        """
        ret = commands.node.main(argv=["dequeue", "actions"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_164_node_scan_scsi():
        """
        Scan scsi buses
        """
        ret = commands.node.main(argv=["scanscsi"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_164_node_collector_networks():
        """
        Collector networks
        """
        ret = commands.node.main(argv=["collector", "networks"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_164_node_collector_search():
        """
        Collector search
        """
        ret = commands.node.main(argv=["collector", "search", "--like", "safe:%"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0251_compliance():
        """
        Node compliance auto
        """
        ret = commands.node.main(argv=["compliance", "auto"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0252_compliance():
        """
        Node compliance check
        """
        ret = commands.node.main(argv=["compliance", "check"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0253_compliance():
        """
        Node compliance fix
        """
        ret = commands.node.main(argv=["compliance", "fix"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0254_compliance():
        """
        Node compliance show moduleset
        """
        ret = commands.node.main(argv=["compliance", "show", "moduleset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0255_compliance():
        """
        Node compliance list moduleset
        """
        ret = commands.node.main(argv=["compliance", "list", "moduleset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0256_compliance():
        """
        Node compliance show ruleset
        """
        ret = commands.node.main(argv=["compliance", "show", "ruleset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0257_compliance():
        """
        Node compliance list ruleset
        """
        ret = commands.node.main(argv=["compliance", "list", "ruleset"])
        assert ret == 0

    @staticmethod
    @pytest.mark.skip
    def test_0258_compliance():
        """
        Node compliance attach
        """
        ret = commands.node.main(argv=["compliance", "attach", "--ruleset", "abcdef", "--moduleset", "abcdef"])
        assert ret == 1

    @staticmethod
    @pytest.mark.skip
    def test_0259_compliance():
        """
        Node compliance detach
        """
        ret = commands.node.main(argv=["compliance", "detach", "--ruleset", "abcdef", "--moduleset", "abcdef"])
        assert ret == 0
