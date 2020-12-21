import json
import os
import time
import uuid
from copy import deepcopy
from subprocess import Popen

import pytest

import env
from core.node import Node
from daemon.main import Daemon
from daemon.monitor import Monitor, shared
from utilities.asset import Asset
from utilities.naming import svc_pathvar

boot_id = str(time.time())


def str_uuid():
    return str(uuid.uuid4())


SERVICES = {
    "parent-flex-min-2": """
[DEFAULT]
id = %s
nodes = *
topology = flex
orchestrate = ha
flex_min = 2
""" % str_uuid(),

    "parent-flex-min-1": """
[DEFAULT]
id = %s
nodes = *
topology = flex
orchestrate = ha
flex_min = 1
""" % str_uuid(),

    "parent": """
[DEFAULT]
id = %s
nodes = *""" % str_uuid(),

    "s-depend-on-parent": """[DEFAULT]
id = %s
nodes = *
parents = parent
orchestrate = ha""" % str_uuid(),

    "s-depend-on-parent-flex-min-2": """[DEFAULT]
id = %s
nodes = *
parents = parent-flex-min-2
orchestrate = ha""" % str_uuid(),

    "s-depend-on-parent-flex-min-1": """[DEFAULT]
id = %s
nodes = *
parents = parent-flex-min-1
orchestrate = ha""" % str_uuid(),

    "s-depend-on-local-parent": """[DEFAULT]
id = %s
nodes = *
parents = parent@{nodename}
orchestrate = ha""" % str_uuid(),

    "ha1": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha"]),
    "ha2": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha"]),
    "ha3": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha"]),
    "s9-prio-1": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 1"]),
    "s8-prio-2": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 2"]),
    "s7-prio-3": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 3"]),
    "s6-prio-4": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 4"]),
    "s5-prio-5": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 5"]),
    "s4-prio-6": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 6"]),
    "s3-prio-7": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 7"]),
    "s2-prio-8": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 8"]),
    "s1-prio-9": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "priority = 9"]),
}


@pytest.fixture(scope='function')
def has_service_child_with_local_parent(osvc_path_tests):
    pathetc = env.Env.paths.pathetc
    if not os.path.exists(pathetc):
        os.mkdir(pathetc)
    with open(os.path.join(pathetc, 'child-with-local-parent.conf'), mode='w+') as svc_file:
        config_txt = """
[DEFAULT]
id = child-with-parents
nodes = *
parents = parent@{nodename}
orchestrate = ha
"""
        svc_file.write(config_txt)


@pytest.fixture(scope='function')
def get_boot_id(mocker, osvc_path_tests):
    mocker.patch.object(Asset, 'get_boot_id', return_value=boot_id)


@pytest.fixture(scope='function')
def mock_daemon(mocker, osvc_path_tests):
    """
    mock some of shared daemon structures
    """
    mocker.patch.dict(shared.CLUSTER_DATA, {})
    mocker.patch.dict(shared.AGG, {})
    mocker.patch.dict(shared.SMON_DATA, {})
    mocker.patch.dict(shared.NMON_DATA, {})
    mocker.patch.dict(shared.SERVICES, {})
    shared.NODE = Node()
    shared.DAEMON = Daemon()


class MonitorTest(object):
    def __init__(self, mocker=None, cluster_nodes=None):
        self.mocker = mocker
        if mocker:
            self.mocker.patch.object(Monitor, 'monitor_period', 0.000001)
        self.monitor = Monitor()
        self.monitor._lazy_cluster_nodes = cluster_nodes or []
        self.execs = {"service_command_exe": env.Env.syspaths.true}
        self.service_command = None

    def set_service_command_cmd(self, cmd):
        self.execs["service_command_exe"] = cmd

    def service_command_factory(self):
        def service_command_mock(*args, **kwargs):
            self.log('detect mock service_command(%s, %s)' % (args, kwargs))
            return Popen(self.execs["service_command_exe"])

        self.service_command = self.mocker.patch.object(Monitor,
                                                        'service_command',
                                                        side_effect=service_command_mock)
        return self.service_command

    def log(self, *args, **kwargs):
        if self.monitor and getattr(self.monitor, 'log'):
            self.monitor.log.info(*args, **kwargs)
        else:
            shared.NODE.log.info(*args, **kwargs)

    def create_svc_config(self, s):
        pathetc = env.Env.paths.pathetc
        if not os.path.exists(pathetc):
            os.mkdir(pathetc)
        with open(os.path.join(pathetc, '%s.conf' % s), mode='w+') as svc_file:
            svc_file.write(SERVICES[s])
        self.log('COMMENT: created %s service with config \n%s\n', s, SERVICES[s])

    def do(self):
        self.log('\nCOMMENT: monitor.do()')
        time.sleep(0.01)  # Simulate time elapsed between 2 do() iteration
        self.monitor.do()
        self.log('COMMENT: proc ps count = %s', len(self.monitor.procs))

    def assert_nmon_status(self, status, message=None):
        if message:
            self.log('COMMENT: ASSERT monitor status is "%s" (reason: %s), full value is %s',
                     status, message, json.dumps(shared.NMON_DATA))
        assert shared.NMON_DATA['status'] == status

    def assert_smon_status(self, svcname, status, message=None):
        if message:
            self.log('COMMENT: ASSERT smon status for svc %s is "%s" (reason: %s), full value is %s',
                     svcname, status, message, json.dumps(shared.SMON_DATA[svcname]))
        assert shared.SMON_DATA[svcname]['status'] == status

    def assert_smon_local_expect(self, svcname, local_expect, message=None):
        if message:
            self.log('COMMENT: ASSERT smon local_expect for svc %s is "%s" (reason: %s), full value is %s',
                     svcname, local_expect, message, json.dumps(shared.SMON_DATA[svcname]))
        assert shared.SMON_DATA[svcname]['local_expect'] == local_expect

    def assert_command_has_been_launched(self, call_list):
        for call in call_list:
            self.log('COMMENT: ASSERT call %s has been called', call)
            assert call in self.service_command.call_args_list

    def assert_command_has_not_been_launched(self, call_list):
        for call in call_list:
            self.log('COMMENT: ASSERT call %s has not been called', call)
            assert call not in self.service_command.call_args_list

    def create_cluster_status(self):
        self.log('COMMENT: create ccfg/cluster status.json')
        if not os.path.exists(svc_pathvar("ccfg/cluster")):
            os.makedirs(svc_pathvar("ccfg/cluster"))
        open(svc_pathvar("ccfg/cluster", "status.json"), 'w'). \
            write(json.dumps({"updated": time.time() + 1, "kind": "ccfg"}))

    def create_service_status(
            self,
            path,
            status="up",
            overall="up",
            topology="failover",
            frozen=0
    ):
        if not os.path.exists(svc_pathvar(path)):
            os.makedirs(svc_pathvar(path))
        status = {
            "avail": status,
            "overall": overall,
            "topology": topology,
            "frozen": frozen,
            "monitor": {
                "status": status,
                "overall": overall
            },
            "updated": time.time(),
        }
        if "prio-" in path:
            priority = int(path[path.find("prio-") + 5:])
            status['priority'] = priority

        self.log("COMMENT: create %s status.json, with %s", path, status)
        open(svc_pathvar(path, "status.json"), 'w').write(json.dumps(status))

    def prepare_monitor_idle(self):
        self.log('COMMENT: Prepare monitor in idle status')
        self.log('COMMENT: hack rejoin_grace_period to 0.001')
        self.monitor._lazy_rejoin_grace_period = 0.001

        self.log('COMMENT: monitor.init()')
        self.monitor.init()
        time.sleep(0.01)  # simulate time elapsed
        self.do()
        self.create_cluster_status()
        self.do()
        self.assert_nmon_status('idle')


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('has_cluster_config')
@pytest.mark.usefixtures('get_boot_id')
@pytest.mark.usefixtures('mock_daemon')
class TestMonitorOrchestratorStart(object):
    @staticmethod
    @pytest.mark.parametrize(
        'title, svcnames, svcname, other_node_status, transition_status1, transition_status2, final_status',
        [
            ("parent avail - /has start/, /start failed/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                        "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'ready', 'starting', 'start failed'),

            ("local frozen, simple parent avail - stay idle, /no start/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent": {"avail": "down", "overall": "down", "updated": time.time(), # "frozen": time.time(),
                                        "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'idle', 'idle', 'idle'),

            ("simple parent avail - /has start/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                        "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'ready', 'starting', 'idle'),

            ("parent avail unknown - stay in wait parents /no start/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "unknown", "overall": "unknown", "updated": time.time(),
                            "topology": "failover",
                            "monitor": {"status": "unknown", "overall": "unknown"}},
                 "s-depend-on-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                        "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'wait parents', 'wait parents', 'wait parents'),

            ("/no node2 status/, plop must stay in wait parent, /no start/, /node frozen/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "unknown", "overall": "unknown", "updated": time.time(),
                            "topology": "failover", "monitor": {"status": "unknown", "overall": "unknown"}},
                 "s-depend-on-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                        "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'idle', 'idle', 'idle'),

            ("not enough flex - stay in wait parents, /no start/",
             ['parent-flex-min-2', ],
             's-depend-on-parent-flex-min-2',
             {
                 "parent-flex-min-2": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "flex",
                                       "flex_min": 2, "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent-flex-min-2": {"avail": "down", "overall": "down", "updated": time.time(),
                                                   "topology": "failover",
                                                   "monitor": {"status": "down", "overall": "down"}}
             },
             'wait parents', 'wait parents', 'wait parents'),

            ("enough flex instances - will start, /has start/",
             ['parent-flex-min-1', ],
             's-depend-on-parent-flex-min-1',
             {
                 "parent-flex-min-1": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "flex",
                                       "flex_min": 1, "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent-flex-min-1": {"avail": "down", "overall": "down", "updated": time.time(),
                                                   "topology": "failover",
                                                   "monitor": {"status": "down", "overall": "down"}}
             },
             'ready', 'starting', 'idle'),

            ("local parent not avail - stay in wait parents, /no start/",
             ['parent', ],
             's-depend-on-local-parent',
             {
                 "parent": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-local-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                              "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'wait parents', 'wait parents', 'wait parents'),

            ("local parent is avail - will start, /has start/",
             ['parent', ],
             's-depend-on-local-parent',
             {
                 "parent": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-local-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                              "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'ready', 'starting', 'idle'),
        ])
    def test_orchestrator_from_init_to_run_loop(
            mocker,
            title,
            svcnames,
            svcname,
            other_node_status,
            transition_status1,
            transition_status2,
            final_status):

        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename, "node2"])
        service_command = monitor_test.service_command_factory()
        monitor = monitor_test.monitor
        log = monitor_test.log

        if '/no node2 status/' not in title:
            monitor_node2 = deepcopy(shared.NMON_DATA)
            monitor_node2["status"] = "idle"
            shared.CLUSTER_DATA["node2"] = {
                "compat": shared.COMPAT_VERSION,
                "api": shared.API_VERSION,
                "agent": shared.NODE.agent_version,
                "monitor": monitor_node2,
                "labels": shared.NODE.labels,
                "targets": shared.NODE.targets,
                "services": {"status": {k: v for k, v in other_node_status.items()},  "monitor": {"status": "up"}, }
            }
        else:
            log('COMMENT: hack rejoin_grace_period to 0.001')
            monitor._lazy_rejoin_grace_period = 0.001

        log('\n===============================')
        log('COMMENT: starting test for %s', title)
        log('COMMENT: audited service "%s" expected transition 1: %s, 2: %s, final: %s',
                         svcname, transition_status1, transition_status2, final_status)
        monitor_test.create_svc_config(svcname)
        log('COMMENT: other services')
        for s in svcnames:
            monitor_test.create_svc_config(s)

        log('COMMENT: monitor.init()')
        monitor.init()
        time.sleep(0.01)  # simulate time elapsed
        monitor_test.assert_nmon_status('init', 'after init()')
        log('COMMENT: asserting all services are refreshed: (service_command.call_args_list is correct: %s)',
            service_command.call_args_list[0])
        assert service_command.call_count == 1
        for s in svcnames + [svcname, 'cluster']:
            assert s in service_command.call_args_list[0][0][0].split(',')
        assert service_command.call_args_list[0][0][1] == ['status', '--parallel', '--refresh']
        assert service_command.call_args_list[0][1] == {"local": False}

        monitor_test.create_cluster_status()

        for i in range(3):
            monitor_test.do()
            monitor_test.assert_nmon_status('init', 'after do(), when all status.json not yet present')

        for s in svcnames + [svcname]:
            monitor_test.do()
            monitor_test.assert_nmon_status('init', 'after do(), when all status.json not yet present')
            if "local parent is avail" in title and s in svcnames:
                avail = "up"
            else:
                avail = "down"
            if 'local frozen' in title and s == svcname:
                frozen = time.time()
                log('COMMENT: create frozen flag for %s', s)
                open(svc_pathvar(s, "frozen"), 'w').close()
            else:
                frozen = 0
            log('COMMENT: create local status.json for %s with avail: %s', s, avail)
            status = deepcopy(other_node_status.get(s, {}))
            status['avail'] = avail
            status['updated'] = time.time()
            status['monitor'] = {}
            status['frozen'] = frozen
            open(svc_pathvar(s, "status.json"), 'w').write(json.dumps(status))

        svc = monitor.get_service(svcname)
        assert svc

        monitor_test.do()
        monitor_test.assert_nmon_status('idle', 'after status.json created')
        monitor_test.assert_smon_status(svcname, transition_status1)

        log('COMMENT: hack ready_period to 0.001')
        monitor._lazy_ready_period = 0.001

        if "/start failed/" in title:
            log('COMMENT: hack service command mock to %s', env.Env.syspaths.false)
            monitor_test.set_service_command_cmd(env.Env.syspaths.false)
        monitor_test.do()
        monitor_test.assert_smon_status(svcname, transition_status2)

        if "/has start/" in title:
            monitor_test.assert_command_has_been_launched([((svcname, ['start']), {}), ])
            if "/start failed/" not in title:
                for svcname in [svcname]:
                    log('COMMENT: create status.json for %s with avail up', svcname)
                    status = deepcopy(other_node_status[svcname])
                    status['avail'] = "up"
                    status['updated'] = time.time()
                    open(svc_pathvar(svcname, "status.json"), 'w').write(json.dumps(status))
        else:
            assert ((svcname, ['start']), {}) not in service_command.call_args_list

        monitor_test.do()
        monitor_test.assert_smon_status(svcname, final_status)

        for i in range(3):
            monitor_test.do()
            monitor_test.assert_smon_status(svcname, final_status)
            if '/no start/' in title:
                monitor_test.assert_command_has_not_been_launched([((svcname, ['start']), {}), ])
            if '/has start/' in title:
                monitor_test.assert_command_has_been_launched([((svcname, ['start']), {}), ])
                if "/start failed/" not in title:
                    monitor_test.assert_smon_local_expect(svcname, 'started')

        if "/node frozen/" in title:
            assert monitor.node_frozen > 0
        else:
            assert monitor.node_frozen == 0

    @staticmethod
    def test_monitor_does_not_call_extra_commands(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        service_command = monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        initial_service_commands_call_count = int(service_command.call_count)
        monitor_test.log('COMMENT: ensure no extra service commands')
        for _ in range(10):
            monitor_test.do()
        assert service_command.call_count == initial_service_commands_call_count

    @staticmethod
    def test_monitor_ensure_call_service_status_on_services_without_status(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        for svc in ["ha1", "ha2", "ha3"]:
            monitor_test.create_svc_config(svc)
        monitor_test.log('COMMENT: ensure call refresh on all service with no status')
        monitor_test.create_service_status("ha3", status="down", overall="down")
        monitor_test.do()
        monitor_test.assert_command_has_been_launched([
            (('ha1', ['status', '--refresh', '--waitlock=0']), {"local": False}),
            (('ha2', ['status', '--refresh', '--waitlock=0']), {"local": False}),
        ])
        monitor_test.assert_command_has_not_been_launched([
            (('ha3', ['status', '--refresh', '--waitlock=0']), {"local": False}),
        ])

    @staticmethod
    def test_monitor_ensure_start_is_called_on_non_up_services(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        for svc in ["ha1", "ha2", "ha3"]:
            monitor_test.create_svc_config(svc)
        for svc in ["ha1", "ha2"]:
            monitor_test.create_service_status(svc, status="down", overall="down")
        monitor_test.create_service_status("ha3", status="up", overall="up")
        monitor_test.do()
        monitor_test.do()
        monitor_test.assert_command_has_been_launched([
            (('ha1', ['start']), {}),
            (('ha2', ['start']), {}),
        ])
        monitor_test.assert_command_has_not_been_launched([
            (('ha3', ['start']), {}),
        ])

    @staticmethod
    def test_monitor_ensure_no_start_call_when_services_are_up(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        service_command = monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        service_command_initial_call_count = int(service_command.call_count)
        for svc in ["ha1", "ha2", "ha3"]:
            monitor_test.create_svc_config(svc)
            monitor_test.create_service_status(svc, status="up", overall="up")
        for _ in range(5):
            monitor_test.do()
        assert service_command.call_count == service_command_initial_call_count

    @staticmethod
    def test_monitor_respect_priority_and_max_parallel(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        max_parallel = 3
        shared.NODE._lazy_max_parallel = max_parallel
        count = int(monitor_test.service_command.call_count)
        # service list ordered by priority
        services = [
            "s9-prio-1", "s8-prio-2", "s7-prio-3", "s6-prio-4",
            "s5-prio-5", "s4-prio-6", "s3-prio-7", "s2-prio-8",
            "s1-prio-9", "ha1", "ha2", "ha3",
        ]
        for svc in services:
            monitor_test.create_svc_config(svc)
            monitor_test.create_service_status(svc, status="down", overall="down")

        monitor_test.log('COMMENT: one do() call to become ready')
        monitor_test.do()

        for expected_service_start in [services[0:4], services[4:8], services[8:12]]:
            monitor_test.log('COMMENT: ensure start of %s', expected_service_start)
            monitor_test.do()
            count += max_parallel + 1
            for svc in expected_service_start:
                monitor_test.assert_command_has_been_launched([((svc, ['start']), {})])
                monitor_test.create_service_status(svc, status="up", overall="up")
            assert monitor_test.service_command.call_count == count
