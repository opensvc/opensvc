import json
import os
import time
import uuid
from copy import deepcopy
from subprocess import Popen

import pytest

import env
from daemon.handlers.object.status.post import Handler as HandlerObjectStatusPost
from daemon.handlers.object.monitor.post import Handler as HandlerObjectMonitorPost
from daemon.handlers.object.clear.post import Handler as HandlerObjectClearPost
from daemon.main import Daemon
# noinspection PyUnresolvedReferences
from daemon.monitor import Monitor, shared, queue
from tests.helpers import ANY, call
from utilities.asset import Asset
from utilities.naming import svc_pathvar

boot_id = str(time.time())
post_object_status_handler = HandlerObjectStatusPost()
post_object_monitor_handler = HandlerObjectMonitorPost()
post_object_clear_handler = HandlerObjectClearPost()


def str_uuid():
    return str(uuid.uuid4())


SERVICES = {
    "parent-flex-min-2": """[DEFAULT]
id = %s
nodes = *
topology = flex
orchestrate = ha
flex_min = 2
""" % str_uuid(),

    "parent-flex-min-1": """[DEFAULT]
id = %s
nodes = *
topology = flex
orchestrate = ha
flex_min = 1
""" % str_uuid(),

    "parent": """[DEFAULT]
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

    "restart-0": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha",
                            "[fs#1]", "type = flag"]),
    "restart-1": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha",
                            "[fs#1]", "type = flag", "restart = 1"]),
    "restart-2": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha",
                            "[fs#1]", "type = flag", "restart = 2"]),
    "restart-3": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha",
                            "[fs#1]", "type = flag", "restart = 3"]),
    "restart-3-stdby": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha",
                                  "[fs#1]", "type = flag", "restart = 3", "standby = True"]),

    "restart-multiple-3": "\n".join([
        "[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha",
        "[fs#1]", "type = flag", "restart = 3",
        "[fs#2]", "type = flag", "restart = 5",
        "[fs#3]", "type = flag", "restart = 2",
    ]),

    "restart-delay": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha",
                                "[fs#1]", "type = flag", "restart = 3", "restart_delay = 1s"]),
    "restart-delay-stdby": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha",
                                      "[fs#1]", "type = flag", "restart = 3", "restart_delay = 1s", "standby = true"]),

    "restart-toc": "\n".join(["[DEFAULT]", "id = %s" % str_uuid(), "nodes = *", "orchestrate = ha", "monitor = true",
                              "[fs#1]", "type = flag", "restart = 3"]),
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
    return mocker.patch.object(Asset, 'get_boot_id', return_value=boot_id)


@pytest.fixture(scope='function')
def last_boot_id(mocker, osvc_path_tests):
    return mocker.patch.object(shared.NODE, 'last_boot_id', return_value=str_uuid())


@pytest.fixture(scope='function')
def popen_communicate(mocker):
    mocker.patch.object(Asset, 'get_boot_id', return_value=boot_id)
    return mocker.patch.object(Popen, 'communicate', return_value=("", ""))


@pytest.fixture(scope='function')
def wait_listener(mocker):
    mocker.patch.object(Monitor, 'wait_listener')


@pytest.fixture(scope='function')
def mock_daemon(mocker):
    mocker.patch.object(shared, 'DAEMON', Daemon())


class MonitorTest(object):
    def __init__(self, mocker=None, cluster_nodes=None):
        self.mocker = mocker
        if mocker:
            self.mocker.patch.object(Monitor, 'monitor_period', 0.000001)
            # needed hack when monitor thread is used to simulate listener client thread
            self.mocker.patch.object(Monitor, 'get_user_info', create=True, return_value="tester@local")
            self.mocker.patch.object(Monitor, 'log_request', create=True, return_value="mock log_request result")
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
        self.log('COMMENT: create service %s.conf config\n%s',
                 s, "\n".join(["      " + line for line in SERVICES[s].split("\n")]))
        pathetc = env.Env.paths.pathetc
        if not os.path.exists(pathetc):
            os.mkdir(pathetc)
        with open(os.path.join(pathetc, '%s.conf' % s), mode='w+') as svc_file:
            svc_file.write(SERVICES[s])

    def delete_svc_config(self, s):
        f_name = os.path.join(env.Env.paths.pathetc, '%s.conf' % s)
        if os.path.exists(f_name):
            self.log("COMMENT: delete service %s.conf config file: %s", s, f_name)
            os.unlink(f_name)

    def do(self):
        self.log('\nCOMMENT: monitor.do()')
        time.sleep(0.01)  # Simulate time elapsed between 2 do() iteration
        self.monitor.do()
        self.log('COMMENT: proc ps count = %s', len(self.monitor.procs))

    def assert_nmon_status(self, status, message=None):
        local_monitor = self.monitor.get_node_monitor()
        if message:
            self.log('COMMENT: ASSERT monitor status is "%s" (reason: %s), full value is %s',
                     status, message, json.dumps(local_monitor))
        assert local_monitor.status == status

    def assert_smon_status(self, svcname, status, message=None):
        smon = self.monitor.get_service_monitor(svcname)
        if message:
            self.log('COMMENT: ASSERT smon status for svc %s is "%s" (reason: %s), full value is %s',
                     svcname, status, message, json.dumps(smon))
        assert smon.status == status

    def assert_smon_local_expect(self, svcname, local_expect, message=None):
        smon = self.monitor.get_service_monitor(svcname)
        if message:
            self.log('COMMENT: ASSERT smon local_expect for svc %s is "%s" (reason: %s), full value is %s',
                     svcname, local_expect, message, json.dumps(smon))
        assert smon.local_expect == local_expect

    def assert_smon_global_expect(self, svcname, global_expect, message=None):
        smon = self.monitor.get_service_monitor(svcname)
        if message:
            self.log('COMMENT: ASSERT smon global_expect for svc %s is "%s" (reason: %s), full value is %s',
                     svcname, global_expect, message, json.dumps(smon))
        assert smon.global_expect == global_expect

    def assert_command_has_been_launched(self, call_list):
        for call_element in call_list:
            self.log('COMMENT: ASSERT %s has been called', call_element)
            assert call_element in self.service_command.call_args_list

    def assert_count_of_matching_calls_is(self, calls, count):
        found = 0
        self.log('COMMENT: ASSERT count of %s calls is %d', calls, count)
        for called_element in self.service_command.call_args_list:
            for c in calls:
                if c == called_element:
                    found = found + 1
        assert found == count, "found %s calls" % self.service_command.call_args_list

    def assert_a_command_has_been_launched_x_times(self, call, count):
        found = 0
        self.log('COMMENT: ASSERT %s has been called %d times', call, count)
        for called_element in self.service_command.call_args_list:
            if call == called_element:
                found = found + 1
        assert found == count, "found %s calls" % self.service_command.call_args_list

    def assert_command_has_not_been_launched(self, call_list):
        for call_element in call_list:
            self.log('COMMENT: ASSERT %s has not been called', call_element)
            assert call_element not in self.service_command.call_args_list

    def post_object_monitor(self, path, global_expect=None, status=None):
        self.log("\nCOMMENT: post object monitor %s global_expect: %s", path, global_expect)
        req = {"options": {"path": path}}
        if status:
            req["options"]["status"] = status
        if global_expect:
            req["options"]["global_expect"] = global_expect
        post_object_monitor_handler.action(
            env.Env.nodename,
            thr=self.monitor,
            **req)

    def post_object_clear(self, path):
        self.log("\nCOMMENT: post object clear %s", path)
        req = {"options": {"path": path}}
        post_object_clear_handler.action(
            env.Env.nodename,
            thr=self.monitor,
            **req)

    def create_cluster_status(self):
        self.log('\nCOMMENT: create ccfg/cluster status.json')
        path = "ccfg/cluster"
        if not os.path.exists(svc_pathvar(path)):
            os.makedirs(svc_pathvar(path))
        status = {"updated": time.time(), "kind": "ccfg"}
        open(svc_pathvar(path, "status.json"), 'w').write(json.dumps(status))
        post_object_status(thr=self.monitor, path='cluster', status=status)

    def create_service_status(
            self,
            path,
            status="up",
            overall="up",
            topology="failover",
            frozen=0,
            flex_min=None,
            resources=None,
    ):
        if not os.path.exists(svc_pathvar(path)):
            os.makedirs(svc_pathvar(path))
        # ensure status time is newer than config
        # without this delay, another automatic need for status may be called
        # if config time == status time
        time.sleep(0.00001)
        now = time.time()
        status = {
            "avail": status,
            "overall": overall,
            "topology": topology,
            "frozen": frozen,
            "monitor": {
                "status": status,
                "overall": overall,
                "status_updated": now
            },
            "updated": now,
        }
        if flex_min is not None:
            status["flex_min"] = flex_min
        if "prio-" in path:
            priority = int(path[path.find("prio-") + 5:])
            status['priority'] = priority

        if resources:
            status["resources"] = resources

        self.log("\nCOMMENT: create %s status.json, with %s", path, json.dumps(status, indent=2))
        open(svc_pathvar(path, "status.json"), 'w').write(json.dumps(status))
        post_object_status(thr=self.monitor, path=path, status=status)

    def prepare_monitor(self):
        self.log('\nCOMMENT: Prepare monitor in idle status')
        self.log('COMMENT: hack rejoin_grace_period to 0.001')
        self.monitor._lazy_rejoin_grace_period = 0.001

        self.log('\nCOMMENT: monitor.init()')
        self.monitor.init()
        time.sleep(0.01)  # simulate time elapsed
        self.do()
        self.create_cluster_status()
        self.do()

    def prepare_monitor_idle(self):
        self.prepare_monitor()
        self.assert_nmon_status('idle')

    def init_other_node_states(self, states, nodename):
        monitor_node2_services = {"status": {k: v for k, v in states.items()}, "monitor": {"status": "up"}, }
        self.log('\nCOMMENT: hack initial status of %s with services: %s',
                 nodename, monitor_node2_services)
        self.monitor.daemon_status_data.view(["monitor", "nodes", nodename]).set(
            [],
            value={
                "compat": shared.COMPAT_VERSION,
                "api": shared.API_VERSION,
                "agent": shared.NODE.agent_version,
                "monitor": {"status": "idle", "status_updated": time.time()},
                "labels": shared.NODE.labels,
                "targets": shared.NODE.targets,
                "services": monitor_node2_services,
            })


def post_object_status(thr, path, status):
    req = {"options": {"path": path, "data": status}}
    post_object_status_handler.action(env.Env.nodename,
                                      thr=thr,
                                      **req)


PARENT_STATUS = {
    "avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
    "monitor": {"status": "up", "overall": "up"}
}
S_DEPEND_ON_PARENT_STATUS = {
    "avail": "down", "overall": "down", "updated": time.time(), "topology": "failover",
    "monitor": {"status": "down", "overall": "down"}
}


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('has_node_config')
@pytest.mark.usefixtures('shared_data')
@pytest.mark.usefixtures('mock_daemon')
@pytest.mark.usefixtures('has_cluster_config')
@pytest.mark.usefixtures('wait_listener')
@pytest.mark.usefixtures('get_boot_id')
@pytest.mark.usefixtures('mock_daemon')
@pytest.mark.usefixtures('popen_communicate')
class TestMonitorOrchestratorStart(object):
    @staticmethod
    @pytest.mark.parametrize(
        'title, svcnames, svcname, other_node_states, transition_status1, transition_status2, final_status',
        [
            ("parent avail - /has start/, /start failed/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": deepcopy(PARENT_STATUS),
                 "s-depend-on-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'ready', 'starting', 'start failed'),

            ("local frozen, simple parent avail - stay idle, /no start/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": deepcopy(PARENT_STATUS),
                 "s-depend-on-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'idle', 'idle', 'idle'),

            ("simple parent avail - /has start/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": deepcopy(PARENT_STATUS),
                 "s-depend-on-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'ready', 'starting', 'idle'),

            ("parent avail unknown - /must stay in wait parent/, /no start/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "unknown", "overall": "unknown", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "unknown", "overall": "unknown"}},
                 "s-depend-on-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'wait parents', 'wait parents', 'wait parents'),

            ("/no node2 status/, must stay idle parent, /no start/, /node frozen/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "unknown", "overall": "unknown", "updated": time.time(),
                            "topology": "failover", "monitor": {"status": "unknown", "overall": "unknown"}},
                 "s-depend-on-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'idle', 'idle', 'idle'),

            ("not enough flex - /local parent frozen/, /must stay in wait parent/, /no start/",
             ['parent-flex-min-2', ],
             's-depend-on-parent-flex-min-2',
             {
                 "parent-flex-min-2": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "flex",
                                       "flex_min": 2, "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent-flex-min-2": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'wait parents', 'wait parents', 'wait parents'),

            ("enough flex instances - will start, /has start/",
             ['parent-flex-min-1', ],
             's-depend-on-parent-flex-min-1',
             {
                 "parent-flex-min-1": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "flex",
                                       "flex_min": 1, "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent-flex-min-1": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'ready', 'starting', 'idle'),

            ("local parent not avail - /must stay in wait parent/, /no start/",
             ['parent', ],
             's-depend-on-local-parent',
             {
                 "parent": deepcopy(PARENT_STATUS),
                 "s-depend-on-local-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'wait parents', 'wait parents', 'wait parents'),

            ("local parent is avail - will start, /has start/",
             ['parent', ],
             's-depend-on-local-parent',
             {
                 "parent": deepcopy(PARENT_STATUS),
                 "s-depend-on-local-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'ready', 'starting', 'idle'),
        ])
    def test_orchestrator_from_init_to_run_loop(
            mocker,
            title,
            svcnames,
            svcname,
            other_node_states,
            transition_status1,
            transition_status2,
            final_status):

        parent_svcs = [name for name in svcnames if 'parent' in name]

        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename, "node2"])
        service_command = monitor_test.service_command_factory()
        monitor = monitor_test.monitor
        log = monitor_test.log

        log('\n===============================')
        log('COMMENT: starting test for %s', title)
        log('COMMENT: audited service "%s" expected transition 1: %s, 2: %s, final: %s',
            svcname, transition_status1, transition_status2, final_status)

        log('COMMENT: hack ready_period to 0.001')
        monitor._lazy_ready_period = 0.001

        if '/no node2 status/' in title:
            log('COMMENT: hack rejoin_grace_period to 0.001')
            monitor._lazy_rejoin_grace_period = 0.001

        monitor_test.create_svc_config(svcname)
        for s in svcnames:
            monitor_test.create_svc_config(s)

        log('\nCOMMENT: monitor.init()')
        monitor.init()
        time.sleep(0.01)  # simulate time elapsed
        monitor_test.assert_nmon_status('init', 'after init()')
        log('COMMENT: asserting all services are refreshed, current service commands:\n%s)',
            "\n".join(["        %s" % str(c) for c in service_command.call_args_list]))
        path_to_check = set(svcnames + [svcname, "cluster"])
        assert service_command.call_count == 1
        monitor_test.assert_command_has_been_launched([
            call(ANY, ["status", "--parallel", "--refresh"], local=False),
        ])
        path_checked = set(monitor_test.service_command.call_args_list[0][0][0].split(","))
        assert path_checked == path_to_check, "path checked vs expected path to check"

        monitor_test.create_cluster_status()

        for i in range(3):
            monitor_test.do()
            monitor_test.assert_nmon_status('init', 'after do(), when all status.json not yet present')
            if i == 1:
                if '/no node2 status/' not in title:
                    monitor_test.init_other_node_states(states=deepcopy(other_node_states), nodename="node2")

        for s in svcnames + [svcname]:
            monitor_test.do()
            monitor_test.assert_nmon_status('init', 'after do(), when all status.json not yet present')
            if "local parent is avail" in title and s in svcnames:
                avail = "up"
            else:
                avail = "down"
            if ('local frozen' in title and s == svcname) or ('/local parent frozen/' in title and s in parent_svcs):
                frozen = time.time()
                log('\nCOMMENT: create frozen flag for %s', s)
                open(svc_pathvar(s, "frozen"), 'w').close()
            else:
                frozen = 0
            topology = other_node_states.get(s, {}).get('topology', "failover")
            kwargs = {"path": s, "status": avail, "overall": avail, "topology": topology, "frozen": frozen}
            if s == 'parent-flex-min-2':
                kwargs['flex_min'] = 2
            monitor_test.create_service_status(**kwargs)

        svc = monitor.get_service(svcname)
        assert svc

        monitor_test.do()
        monitor_test.assert_nmon_status('idle', 'after status.json created')
        for i in range(1, 4):
            try:
                monitor_test.assert_smon_status(svcname, transition_status1,
                                                'after status.json created and %s x do()' % i)
                break
            except:
                monitor_test.do()
                if i == 4:
                    monitor_test.assert_smon_status(svcname, transition_status1, 'after last chance x do()')

        if "/start failed/" in title:
            log('\nCOMMENT: hack service command mock to %s', env.Env.syspaths.false)
            monitor_test.set_service_command_cmd(env.Env.syspaths.false)
        monitor_test.do()
        monitor_test.assert_smon_status(svcname, transition_status2, "expected transition_status2")

        if "/has start/" in title:
            monitor_test.assert_command_has_been_launched([
                call(svcname, ["start"]),
            ])
            if "/start failed/" not in title:
                for svcname in [svcname]:
                    topology = other_node_states.get(svcname, {}).get('topology', "failover")
                    monitor_test.create_service_status(path=svcname, status="up", overall="up", topology=topology,
                                                       frozen=0)
        else:
            monitor_test.assert_command_has_not_been_launched([
                call(svcname, ["start"]),
            ])

        monitor_test.do()
        monitor_test.assert_smon_status(svcname, final_status)

        service_command_call_count = int(monitor_test.service_command.call_count)

        log("\nCOMMENT: Ensure nothing happen on service %s", svcname)
        for i in range(3):
            monitor_test.do()
            monitor_test.assert_smon_status(svcname, final_status)
            if '/no start/' in title:
                monitor_test.assert_command_has_not_been_launched([
                    call(svcname, ["start"]),
                ])
            if '/has start/' in title:
                log("COMMENT: no more service command has been launched: count=%s",
                    monitor_test.service_command.call_count)
                assert monitor_test.service_command.call_count == service_command_call_count
                if "/start failed/" not in title:
                    monitor_test.assert_smon_local_expect(svcname, 'started',
                                                          "service %s stay in started state" % svcname)
            if '/must stay in wait parent/' in title:
                monitor_test.assert_smon_status(svcname, 'wait parents',
                                                "service %s stay in wait parents state" % svcname)
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
    def test_during_boot_monitor_stay_init_waiting_for_status(
            mocker,
            last_boot_id,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.create_svc_config("parent")
        monitor_test.prepare_monitor()
        for _ in range(3):
            monitor_test.assert_nmon_status("init", "status stay in init because it waits for service status")
            monitor_test.do()

    @staticmethod
    @pytest.mark.parametrize('services', [
        [],  # may call boot with empty selectors => 'no match'
        ["parent"],
        ["parent", "s-depend-on-parent"],
    ])
    def test_during_boot_monitor_call_commands_that_create_status(
            mocker,
            last_boot_id,
            services,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        for service in services:
            monitor_test.create_svc_config(service)
        expected_calls = [
            call(ANY, ["boot", "--parallel"]),
            call("cluster", ["status", "--parallel", "--refresh"], local=False),
        ]
        monitor_test.prepare_monitor()
        monitor_test.assert_command_has_been_launched(expected_calls)
        assert monitor_test.service_command.call_args_list == expected_calls
        expected_services_boot = set(services or [''])
        services_with_boot = set(monitor_test.service_command.call_args_list[0][0][0].split(","))

        assert expected_services_boot == services_with_boot

    @staticmethod
    def test_during_boot_monitor_become_idle_after_all_status_created(
            mocker,
            last_boot_id,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.create_svc_config("parent")
        monitor_test.create_svc_config("s-depend-on-parent")
        monitor_test.prepare_monitor()
        for _ in range(3):
            monitor_test.do()
            monitor_test.assert_nmon_status("init", "status stay in init because it waits for service status")
        monitor_test.create_service_status("parent")
        for _ in range(3):
            monitor_test.do()
            monitor_test.assert_nmon_status("init", "status stay in init because it waits for service status")
        monitor_test.create_service_status("s-depend-on-parent")
        monitor_test.do()
        monitor_test.assert_nmon_status("idle", "status now idle because it has all service status")

    @staticmethod
    @pytest.mark.skip
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
        monitor_test.do()
        monitor_test.assert_command_has_been_launched([
            call("cluster", ["status", "--parallel", "--refresh"], local=False),
            call("ha1,ha2", ["status", "--parallel", "--refresh"], local=False),
        ])
        monitor_test.assert_command_has_not_been_launched([
            call("ha3", ["status", "--refresh"], ANY),
        ])
        monitor_test.do()
        monitor_test.do()

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
            call("ha1", ["start"]),
            call("ha2", ["start"]),
        ])
        monitor_test.assert_command_has_not_been_launched([
            call("ha3", ["start"]),
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
        max_parallel = 4
        shared.NODE._lazy_max_parallel = max_parallel
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

        monitor_test.log('COMMENT: ensure no start actions called yet')
        for svc in services:
            monitor_test.assert_command_has_not_been_launched([
                call(svc, ["start"]),
            ])
        # next calls should be starts, define current call count value
        count = int(monitor_test.service_command.call_count)

        for expected_service_start in [services[0:4], services[4:8], services[8:12]]:
            monitor_test.log('COMMENT: ensure start of %s', expected_service_start)
            monitor_test.do()
            count += max_parallel
            for svc in expected_service_start:
                monitor_test.assert_command_has_been_launched([
                    call(svc, ["start"]),
                ])
                monitor_test.create_service_status(svc, status="up", overall="up")
            assert monitor_test.service_command.call_count == count


def _resources(status, standby=False, monitor=False, rid="fs#1"):
    """
    test helper that prepare resources data for fs#1 with standby and monitor
    for create_service_status()
    """
    resources = {
        rid: {
            "status": "stdby %s" % status if standby else status,
            "type": "fs.flag",
            "label": "fs.flag",
            "provisioned": {
                "state": True,
                "mtime": time.time()
            }
        }
    }
    if standby:
        resources[rid]["standby"] = True
    if monitor:
        resources[rid]["monitor"] = True
    return resources


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('has_node_config')
@pytest.mark.usefixtures('shared_data')
@pytest.mark.usefixtures('mock_daemon')
@pytest.mark.usefixtures('has_cluster_config')
@pytest.mark.usefixtures('wait_listener')
@pytest.mark.usefixtures('get_boot_id')
@pytest.mark.usefixtures('mock_daemon')
@pytest.mark.usefixtures('popen_communicate')
class TestMonitorOrchestratorResourcesOrchestrate(object):
    @staticmethod
    @pytest.mark.parametrize("restart", [0, 1, 2, 3])
    def test_monitor_ensure_restart_is_called_x_times_when_rid_failure_is_detected(
            mocker,
            restart
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        svc = "restart-%s" % restart
        monitor_test.create_svc_config(svc)
        monitor_test.create_service_status(svc, status="up", overall="up", resources=_resources("down"))
        for _ in range(10):
            monitor_test.do()

        monitor_test.assert_command_has_not_been_launched([
            call(svc, ["start"]),
        ])
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), restart)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == restart + 1

    @staticmethod
    @pytest.mark.parametrize("svc", ["restart-3", "restart-3-stdby"])
    def test_monitor_ensure_retry_count_is_reset_when_a_restart_succeed_after_rid_failure(
            mocker,
            svc,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        monitor_test.create_svc_config(svc)

        def create_service_status(status):
            monitor_test.create_service_status(svc, status="up", overall="up",
                                               resources=_resources(status, "stdby" in svc))

        monitor_test.log('COMMENT: ensure some restart is tried')
        create_service_status("down")
        for _ in range(2):
            monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 2)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 2

        monitor_test.log('COMMENT: ensure no more restart is tried, when rid is restarted successfully')
        create_service_status("up")
        for _ in range(10):
            monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 2)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 0

        monitor_test.log('COMMENT: ensure restart is retried after new failure detected')
        create_service_status("down")
        for _ in range(10):
            monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 2 + 3)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 4

    @staticmethod
    @pytest.mark.parametrize("svc", ["restart-delay", "restart-delay-stdby"])
    def test_monitor_respect_restart_delay_before_restart_rid_failure(
            mocker,
            svc
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        monitor_test.create_svc_config(svc)

        def create_service_status(status):
            monitor_test.create_service_status(svc, status="up", overall="up",
                                               resources=_resources(status, "stdby" in svc))

        monitor_test.log('')
        monitor_test.log('COMMENT: set do max_shortloops to 0 for full do()')

        monitor_test.monitor.max_shortloops = 0

        monitor_test.log('')
        monitor_test.log('COMMENT: ensure 1st restart is tried without delay')
        create_service_status("down")
        monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 1)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 1

        monitor_test.log('')
        monitor_test.log('COMMENT: ensure no other restarts before delay')
        monitor_test.do()
        monitor_test.do()
        monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 1)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 1

        monitor_test.log('')
        monitor_test.log('COMMENT: ensure restart if delay reached')
        monitor_test.monitor.node_data.set(["services", "status", svc, "monitor", "restart", "fs#1", "updated"],
                                           time.time() - 4)
        monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 2)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 2

        monitor_test.log('')
        monitor_test.log('COMMENT: ensure no other restart tries before delay')
        monitor_test.do()
        monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 2)

        monitor_test.log('')
        monitor_test.log('COMMENT: delay reached now')
        monitor_test.monitor.node_data.set(["services", "status", svc, "monitor", "restart", "fs#1", "updated"],
                                           time.time() - 4)
        monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 3)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 3

        monitor_test.do()
        monitor_test.do()
        monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 3)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 4, "max retries is not reached !"

    @staticmethod
    def test_monitor_toc_monitored_object_with_failed_rids(
            mocker,
    ):
        mocker.patch.object(Monitor, 'placement_candidates', return_value=["other_node1"])
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        svc = "restart-toc"
        monitor_test.create_svc_config(svc)

        def create_service_status(status):
            monitor_test.create_service_status(svc, status="up", overall="up",
                                               resources=_resources(status, monitor=True))

        monitor_test.log('')
        monitor_test.log('COMMENT: set do max_shortloops to 0 for full do()')

        monitor_test.monitor.max_shortloops = 0

        monitor_test.log('')
        monitor_test.log('COMMENT: ensure restart retries until max restarts reached')
        create_service_status("down")
        for i in [1, 2, 3]:
            monitor_test.do()
            monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), i)
            assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == i

        monitor_test.log('')
        monitor_test.log('COMMENT: max retries reached, expect service toc now')
        monitor_test.do()
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#1']), 3)
        assert monitor_test.monitor.get_smon_retries(svc, "fs#1") == 4
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['toc']), 1)
        assert monitor_test.monitor.node_data.get(["services", "status", svc, "monitor", "status"]) == "tocing"

        monitor_test.log('')
        monitor_test.log('COMMENT: expect no other toc after max retries exceed restart')
        for _ in range(4):
            monitor_test.do()
            monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['toc']), 1)
            assert monitor_test.monitor.node_data.get(["services", "status", svc, "monitor", "status"]) == "idle"

    @staticmethod
    @pytest.mark.parametrize("restart", [3])
    def test_monitor_ensure_restart_retries_are_correct_when_multiple_rids_have_retries(
            mocker,
            restart
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor._lazy_ready_period = 0.001
        svc = "restart-multiple-%s" % restart
        monitor_test.create_svc_config(svc)
        resources = _resources("down")
        resources.update(_resources("down", rid="fs#2"))
        resources.update(_resources("up", rid="fs#3"))
        monitor_test.create_service_status(svc, status="up", overall="up", resources=resources)
        for _ in range(10):
            monitor_test.do()

        expected_retries = {
            "fs#1": restart + 1,
            "fs#2": 6,
            "fs#3": 0,
        }
        monitor_test.log('COMMENT: ASSERT retries are %s' % expected_retries)
        assert {
            "fs#1": monitor_test.monitor.get_smon_retries(svc, "fs#1"),
            "fs#2": monitor_test.monitor.get_smon_retries(svc, "fs#2"),
            "fs#3": monitor_test.monitor.get_smon_retries(svc, "fs#3"),
        } == expected_retries, "expected retries mismatch"

        monitor_test.assert_command_has_not_been_launched([
            call(svc, ["start"]),
            call(svc, ["start", "--rid", "fs#1,fs#2,fs#3"]),
            call(svc, ["start", "--rid", "fs#3"]),
            call(svc, ["start", "--rid", "fs#1"]),
        ])
        monitor_test.assert_count_of_matching_calls_is(
            [
                # order of rid may differ, so search both rid order
                call(svc, ['start', '--rid', 'fs#1,fs#2']),
                call(svc, ['start', '--rid', 'fs#2,fs#1']),
            ],
            restart)
        monitor_test.assert_a_command_has_been_launched_x_times(call(svc, ['start', '--rid', 'fs#2']), 5 - restart)


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('has_node_config')
@pytest.mark.usefixtures('shared_data')
@pytest.mark.usefixtures('mock_daemon')
@pytest.mark.usefixtures('has_cluster_config')
@pytest.mark.usefixtures('wait_listener')
@pytest.mark.usefixtures('get_boot_id')
@pytest.mark.usefixtures('mock_daemon')
@pytest.mark.usefixtures('popen_communicate')
class TestMonitorVsPostObject(object):
    @staticmethod
    def test_post_object_status_are_deleted_by_monitor_if_config_file_is_absent(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor.max_shortloops = 0
        monitor_test.monitor._lazy_ready_period = 0.001
        svc = "ha1"
        monitor_test.create_svc_config(svc)

        def assert_smon_has_states(status="idle", local_expect="started"):
            monitor_test.assert_smon_status(svc, status, "ensure existing smon")
            monitor_test.assert_smon_local_expect(svc, local_expect, "ensure existing smon")

        def assert_smon_has_no_states():
            monitor_test.log("COMMENT: assert %s has no states", svc)
            assert monitor_test.monitor.node_data.get(["services", "status", svc], default=None) is None, \
                "%s has smon states" % svc

        monitor_test.log('\n------\nCOMMENT: initial status %s is no states', svc)
        assert_smon_has_no_states()

        monitor_test.log('\n------\nCOMMENT: simulate post object status on %s', svc)
        monitor_test.create_service_status(svc, status="up", overall="up", resources=_resources("up"))
        monitor_test.do()
        assert_smon_has_states(status="idle", local_expect="started")

        monitor_test.log('\n------\nCOMMENT: simulate deletion of config file for %s', svc)
        monitor_test.delete_svc_config(svc)
        assert_smon_has_states(status="idle", local_expect="started")
        monitor_test.do()
        assert_smon_has_no_states()

        monitor_test.log('\n------\nCOMMENT: inline set_smon %s to idle', svc)
        monitor_test.monitor.set_smon(svc, status="idle", local_expect="started")
        assert_smon_has_states(status="idle", local_expect="started")
        monitor_test.do()
        assert_smon_has_no_states()

    @staticmethod
    def test_post_object_monitor_global_expect_not_deleted_when_no_config_file(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor.max_shortloops = 0
        monitor_test.monitor._lazy_ready_period = 0.001
        svc = "ha1"

        monitor_test.log('\n------\nCOMMENT: posting global expect %s to frozen', svc)
        monitor_test.post_object_monitor(svc, global_expect="frozen")
        for _ in range(4):
            monitor_test.do()
        monitor_test.assert_smon_global_expect(
            svc,
            "frozen",
            "persist even if no config file")

    @staticmethod
    def test_post_object_monitor_global_expect_are_deleted_when_too_old_and_no_config_file(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor.max_shortloops = 0
        monitor_test.monitor._lazy_ready_period = 0.001
        svc = "ha1"

        def assert_smon_has_no_states():
            monitor_test.log("COMMENT: assert %s has no states", svc)
            assert monitor_test.monitor.node_data.get(["services", "status", svc], default=None) is None, \
                "%s has smon states" % svc

        monitor_test.log('\n------\nCOMMENT: posting global expect %s to frozen', svc)
        monitor_test.post_object_monitor(svc, global_expect="frozen")
        monitor_test.do()
        monitor_test.assert_smon_global_expect(
            svc,
            "frozen",
            "persist even if no config file")

        monitor_test.log('\n------\nCOMMENT: make %s global_expect_updated to past 5 seconds', svc)
        monitor_test.monitor.node_data.set(["services", "status", svc, "monitor", "global_expect_updated"],
                                           time.time() - 5)
        monitor_test.assert_smon_global_expect(svc, "frozen", "initial state")
        monitor_test.do()

        monitor_test.log('\n------\nCOMMENT: expect purged status if global_expect is old')
        monitor_test.do()
        assert_smon_has_no_states()


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('has_node_config')
@pytest.mark.usefixtures('shared_data')
@pytest.mark.usefixtures('mock_daemon')
@pytest.mark.usefixtures('has_cluster_config')
@pytest.mark.usefixtures('wait_listener')
@pytest.mark.usefixtures('get_boot_id')
@pytest.mark.usefixtures('mock_daemon')
@pytest.mark.usefixtures('popen_communicate')
class TestMonitorVsPostObjectClear(object):
    @staticmethod
    def test_post_object_clear_remove_failing_status(
            mocker,
    ):
        monitor_test = MonitorTest(mocker=mocker, cluster_nodes=[env.Env.nodename])
        monitor_test.service_command_factory()
        monitor_test.prepare_monitor_idle()
        monitor_test.monitor.max_shortloops = 0
        monitor_test.monitor._lazy_ready_period = 0.001
        svc = "ha1"
        monitor_test.create_svc_config(svc)
        monitor_test.create_service_status(svc, status="up", overall="up", resources=_resources("up"))

        monitor_test.log("\n------\nCOMMENT: post initial %s status 'stop failed', verify status after monitor.do()",
                         svc)
        monitor_test.post_object_monitor(svc, status="stop failed")
        monitor_test.log("\n------\nCOMMENT: verify monitor.do() has applied deferred %s state to 'stop failed'", svc)
        monitor_test.do()
        monitor_test.assert_smon_status(svc, "stop failed", "ensure monitor apply deferred smon changes")

        for i in range(1, 3):
            monitor_test.log("\n------\nCOMMENT: verify %s status 'failed stop' preserved after monitor.do()", svc)
            monitor_test.do()
            monitor_test.assert_smon_status(svc, "stop failed", "ensure smon.status keep its status: 'stop failed'")

        monitor_test.log("\n------\nCOMMENT: simulate post object clear on %s, then ensure status is 'idle'", svc)
        monitor_test.post_object_clear(svc)
        monitor_test.do()
        monitor_test.assert_smon_status(svc, "idle", "ensure initial smon.status stay 'idle' after clear")

        monitor_test.log("\n------\nCOMMENT: ensure %s status stay 'idle' after monitor.do()", svc)
        for _ in range(3):
            monitor_test.do()
            monitor_test.assert_smon_status(svc, "idle", "ensure initial smon.status stay idle after clear")
