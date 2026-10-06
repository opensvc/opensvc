import os

import pytest

from core.node import Node
from env import Env


def write_node_conf(txt):
    if not os.path.exists(Env.paths.pathetc):
        os.makedirs(Env.paths.pathetc)
    with open(Env.paths.nodeconf, "w") as f:
        f.write("[node]\n" + txt)


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestCollectorEnv:
    @staticmethod
    def test_oc3_only():
        write_node_conf("collector = https://collector.localdomain\n")
        env = Node().collector_env
        assert env.dbopensvc is None
        assert env.feeder == "https://collector.localdomain/feeder"
        assert env.server == "https://collector.localdomain/server"
        assert env.has_oc2 is False
        assert env.has_oc3 is True
        assert env.enabled is True

    @staticmethod
    def test_oc3_feeder_only():
        write_node_conf("collector_feeder = https://feeder.localdomain/feeder\n")
        env = Node().collector_env
        assert env.feeder == "https://feeder.localdomain/feeder"
        assert env.has_oc2 is False
        assert env.has_oc3 is True
        assert env.enabled is True

    @staticmethod
    def test_oc2_only():
        write_node_conf("dbopensvc = https://collector.localdomain\n")
        env = Node().collector_env
        assert env.dbopensvc == "https://collector.localdomain:443/feed/default/call/xmlrpc"
        assert env.feeder == ""
        assert env.server == ""
        assert env.has_oc2 is True
        assert env.has_oc3 is False
        assert env.enabled is True

    @staticmethod
    def test_oc2_and_oc3_are_independent():
        write_node_conf("dbopensvc = https://oc2.localdomain\n"
                        "collector = https://oc3.localdomain\n")
        env = Node().collector_env
        assert env.dbopensvc == "https://oc2.localdomain:443/feed/default/call/xmlrpc"
        assert env.dbcompliance == "https://oc2.localdomain:443/init/compliance/call/xmlrpc"
        assert env.feeder == "https://oc3.localdomain/feeder"
        assert env.server == "https://oc3.localdomain/server"
        assert env.has_oc2 is True
        assert env.has_oc3 is True
        assert env.enabled is True

    @staticmethod
    @pytest.mark.parametrize("value", ["none", "None"])
    def test_oc2_disabled_with_none(value):
        write_node_conf("dbopensvc = %s\ncollector = https://collector.localdomain\n" % value)
        env = Node().collector_env
        assert env.has_oc2 is False
        assert env.has_oc3 is True
        assert env.enabled is True

    @staticmethod
    def test_no_collector():
        write_node_conf("")
        env = Node().collector_env
        assert env.dbopensvc is None
        assert env.feeder == ""
        assert env.has_oc2 is False
        assert env.has_oc3 is False
        assert env.enabled is False


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestCollectorSection:
    @staticmethod
    def test_collector_url():
        write_node_conf("\n[collector]\nurl = https://collector.localdomain\n")
        env = Node().collector_env
        assert env.collector == "https://collector.localdomain"
        assert env.feeder == "https://collector.localdomain/feeder"
        assert env.server == "https://collector.localdomain/server"
        assert env.timeout == 5
        assert env.has_oc2 is False
        assert env.has_oc3 is True
        assert env.enabled is True

    @staticmethod
    def test_collector_feeder_server_timeout():
        write_node_conf("\n[collector]\n"
                        "url = https://collector.localdomain\n"
                        "feeder = https://feeder.localdomain/feeder\n"
                        "server = https://server.localdomain/server\n"
                        "timeout = 10s\n")
        env = Node().collector_env
        assert env.feeder == "https://feeder.localdomain/feeder"
        assert env.server == "https://server.localdomain/server"
        assert env.timeout == 10

    @staticmethod
    def test_collector_timeout_max():
        write_node_conf("\n[collector]\nurl = https://collector.localdomain\ntimeout = 1m\n")
        assert Node().collector_env.timeout == 20

    @staticmethod
    def test_collector_section_overrides_deprecated_node_keywords():
        write_node_conf("collector = https://old.localdomain\n"
                        "collector_feeder = https://old.localdomain/feeder\n"
                        "collector_server = https://old.localdomain/server\n"
                        "collector_timeout = 7s\n"
                        "\n[collector]\n"
                        "url = https://new.localdomain\n"
                        "feeder = https://new.localdomain/feeder\n"
                        "server = https://new.localdomain/server\n"
                        "timeout = 9s\n")
        env = Node().collector_env
        assert env.collector == "https://new.localdomain"
        assert env.feeder == "https://new.localdomain/feeder"
        assert env.server == "https://new.localdomain/server"
        assert env.timeout == 9

    @staticmethod
    def test_deprecated_node_keywords_fallback():
        write_node_conf("collector_timeout = 7s\n"
                        "\n[collector]\nurl = https://new.localdomain\n")
        env = Node().collector_env
        assert env.collector == "https://new.localdomain"
        assert env.feeder == "https://new.localdomain/feeder"
        assert env.timeout == 7


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestCollectorOk:
    @staticmethod
    @pytest.mark.parametrize("conf, expected", [
        ("", {False: True, True: False, "oc2": False, "oc3": False}),
        ("dbopensvc = https://oc2.localdomain\n",
         {False: True, True: True, "oc2": True, "oc3": False}),
        ("collector = https://oc3.localdomain\n",
         {False: True, True: True, "oc2": False, "oc3": True}),
        ("dbopensvc = https://oc2.localdomain\ncollector = https://oc3.localdomain\n",
         {False: True, True: True, "oc2": True, "oc3": True}),
    ], ids=["none", "oc2", "oc3", "oc2+oc3"])
    def test_collector_ok(conf, expected):
        write_node_conf(conf)
        node = Node()
        for req_collector, result in expected.items():
            assert node.collector_ok(req_collector) is result, req_collector

    @staticmethod
    def test_oc3_only_schedules_oc3_capable_tasks_only():
        write_node_conf("collector = https://oc3.localdomain\n")
        node = Node()
        scheduled = set(
            action for action, parms in node.sched.actions.items()
            for p in parms if p.req_collector and node.collector_ok(p.req_collector)
        )
        assert {"pushasset", "pushpkg", "pushdisks", "sysreport", "dequeue_actions"} <= scheduled
        assert not {"checks", "pushstats", "pushpatch", "compliance_auto", "rotate_root_pw"} & scheduled


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestCollectorRpcOc3Only:
    @staticmethod
    def test_call_is_noop_without_oc2(capsys):
        write_node_conf("collector = https://oc3.localdomain\nuuid = abcd\n")
        ret = Node().collector.call("push_checks", {})
        assert ret == {"ret": 1, "msg": "no oc2 collector defined. set 'dbopensvc' in node.conf"}
        assert "not registered" not in capsys.readouterr().err


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestSysreportOc3Only:
    @staticmethod
    def test_force_sends_with_oc3_without_oc2(mocker):
        from core.sysreport.sysreport import BaseSysReport as SysReport
        from utilities.semver import Semver
        write_node_conf("collector = https://oc3.localdomain\nuuid = abcd\n")
        node = Node()
        mocker.patch.object(Node, "oc3_version", return_value=Semver(3, 0, 1))
        rpc_call = mocker.patch.object(node.collector, "call")
        sr = SysReport(node=node)
        for name in ("collect", "delete_collected", "write_stat"):
            mocker.patch.object(sr, name)
        mocker.patch.object(sr, "archive", return_value=None)
        oc3_send = mocker.patch.object(sr, "_oc3_send")

        assert sr.sysreport(force=True) is None

        rpc_call.assert_not_called()
        oc3_send.assert_called_once_with(None, [])

    @staticmethod
    def test_abort_without_oc2_and_old_oc3(mocker, capsys):
        from core.sysreport.sysreport import BaseSysReport as SysReport
        from utilities.semver import Semver
        write_node_conf("collector = https://oc3.localdomain\nuuid = abcd\n")
        node = Node()
        mocker.patch.object(Node, "oc3_version", return_value=Semver(3, 0, 0))
        sr = SysReport(node=node)
        collect = mocker.patch.object(sr, "collect")

        assert sr.sysreport() == 1
        collect.assert_not_called()
        assert "no collector connexion" in capsys.readouterr().out


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestPushChecks:
    data = {
        "fs_u": [
            {"instance": "/var", "value": "94%", "path": "", "driver": "generic"},
            {"instance": "/srv", "value": "12 %", "path": "svc1", "driver": "generic"},
        ],
        "mpath": [
            {"instance": "36000c29a", "value": 2, "path": "", "driver": "generic"},
            {"instance": "36000c29b", "value": "n/a", "path": "", "driver": "generic"},
        ],
    }

    @staticmethod
    def test_oc3_checks_data():
        write_node_conf("collector = https://oc3.localdomain\nuuid = abcd\n")
        l = Node().oc3_checks_data(TestPushChecks.data)
        assert sorted(l, key=lambda d: d["instance"]) == [
            {"type": "fs_u", "driver": "generic", "path": "svc1", "instance": "/srv", "unit": "%", "value": 12},
            {"type": "fs_u", "driver": "generic", "path": "", "instance": "/var", "unit": "%", "value": 94},
            {"type": "mpath", "driver": "generic", "path": "", "instance": "36000c29a", "unit": "", "value": 2},
        ]

    @staticmethod
    def test_push_to_oc3_feed(mocker):
        import core.oc3path as oc3path
        from utilities.semver import Semver
        write_node_conf("collector = https://oc3.localdomain\nuuid = abcd\n")
        node = Node()
        mocker.patch.object(Node, "oc3_version", return_value=Semver(3, 0, 6))
        rpc_call = mocker.patch.object(node.collector, "call")
        feed = mocker.patch.object(node, "oc3_request_feed", return_value=(202, None))

        node.push_checks(TestPushChecks.data)

        rpc_call.assert_not_called()
        feed.assert_called_once()
        args, kwargs = feed.call_args
        assert args == ("POST", oc3path.FEED_NODE_CHECKS)
        assert len(kwargs["data"]["data"]) == 3

    @staticmethod
    def test_push_falls_back_to_rpc_before_oc3_3_0_6(mocker):
        from utilities.semver import Semver
        write_node_conf("collector = https://oc3.localdomain\nuuid = abcd\n")
        node = Node()
        mocker.patch.object(Node, "oc3_version", return_value=Semver(3, 0, 5))
        rpc_call = mocker.patch.object(node.collector, "call")
        feed = mocker.patch.object(node, "oc3_request_feed")

        node.push_checks(TestPushChecks.data)

        feed.assert_not_called()
        rpc_call.assert_called_once_with("push_checks", TestPushChecks.data)
