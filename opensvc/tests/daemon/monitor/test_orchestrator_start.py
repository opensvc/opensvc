import json
import os
import time
import uuid
from copy import deepcopy
from subprocess import Popen

import pytest

import env
from daemon.handlers.object.status.post import Handler as HandlerObjectStatusPost
from daemon.main import Daemon
# noinspection PyUnresolvedReferences
from daemon.monitor import Monitor, shared, queue
from daemon.shared import OsvcJournaledData
from utilities.asset import Asset
from utilities.naming import svc_pathvar

boot_id = str(time.time())
post_object_status_handler = HandlerObjectStatusPost()
SERVICES = {
    "parent-flex-min-2": """
[DEFAULT]
id = %s
nodes = *
topology = flex
orchestrate = ha
flex_min = 2
""" % str(uuid.uuid4()),

    "parent-flex-min-1": """
[DEFAULT]
id = %s
nodes = *
topology = flex
orchestrate = ha
flex_min = 1
""" % str(uuid.uuid4()),

    "parent": """
[DEFAULT]
id = %s
nodes = *""" % str(uuid.uuid4()),

    "s-depend-on-parent": """[DEFAULT]
id = %s
nodes = *
parents = parent
orchestrate = ha""" % str(uuid.uuid4()),

    "s-depend-on-parent-flex-min-2": """[DEFAULT]
id = %s
nodes = *
parents = parent-flex-min-2
orchestrate = ha""" % str(uuid.uuid4()),

    "s-depend-on-parent-flex-min-1": """[DEFAULT]
id = %s
nodes = *
parents = parent-flex-min-1
orchestrate = ha""" % str(uuid.uuid4()),

    "s-depend-on-local-parent": """[DEFAULT]
id = %s
nodes = *
parents = parent@{nodename}
orchestrate = ha""" % str(uuid.uuid4()),
}
EXECS = {}


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
def wait_listener(mocker):
    mocker.patch.object(Monitor, 'wait_listener')


@pytest.fixture(scope='function')
def mock_daemon(mocker):
    mocker.patch.object(shared, 'DAEMON', Daemon())


def assert_nmon_status(monitor, status, message='reason'):
    local_monitor = monitor.get_node_monitor()
    log_test('COMMENT: ASSERT monitor status is "%s" (reason: %s), full value is %s',
             status, message, json.dumps(local_monitor))
    assert local_monitor.status == status


def assert_smon_status(monitor, svcname, status, message='reason'):
    smon = monitor.get_service_monitor(svcname)
    log_test('COMMENT: ASSERT smon status for svc %s is "%s" (reason: %s), full value is %s',
             svcname, status, message, json.dumps(smon))
    assert smon.status == status


def assert_smon_local_expect(monitor, svcname, local_expect, message='reason'):
    smon = monitor.get_service_monitor(svcname)
    log_test('COMMENT: ASSERT smon local_expect for svc %s is "%s" (reason: %s), full value is %s',
             svcname, local_expect, message, json.dumps(smon))
    assert smon.local_expect == local_expect


def assert_command_has_been_launched(call_list, call_args_list):
    for call in call_list:
        log_test('COMMENT: ASSERT call %s has been called', call)
        assert call in call_args_list


def assert_command_has_not_been_launched(call_list, call_args_list):
    for call in call_list:
        log_test('COMMENT: ASSERT call %s has not been called', call)
        assert call not in call_args_list


def post_object_status(thr, path, status):
    req = {"options": {"path": path, "data": status}}
    post_object_status_handler.action(env.Env.nodename,
                                      thr=thr,
                                      **req)


def create_cluster_status(monitor):
    log_test('COMMENT: create ccfg/cluster status.json')
    if not os.path.exists(svc_pathvar("ccfg/cluster")):
        os.makedirs(svc_pathvar("ccfg/cluster"))
    cluster_status = {"updated": time.time() + 1, "kind": "ccfg"}
    open(svc_pathvar("ccfg/cluster", "status.json"), 'w').write(json.dumps(cluster_status))
    post_object_status(thr=monitor, path="ccfg/cluster", status=cluster_status)


def create_svc_config(svcname):
    pathetc = env.Env.paths.pathetc
    if not os.path.exists(pathetc):
        os.mkdir(pathetc)
    with open(os.path.join(pathetc, '%s.conf' % svcname), mode='w+') as svc_file:
        svc_file.write(SERVICES[svcname])
    log_test('COMMENT: created %s service with config \n%s\n', svcname, SERVICES[svcname])


def init_other_node_states(states, nodename, monitor):
    log_test('COMMENT: hack initial status of %s', nodename)
    monitor.daemon_status_data.view(["monitor", "nodes", nodename]).set(
        [],
        value={
            "compat": shared.COMPAT_VERSION,
            "api": shared.API_VERSION,
            "agent": shared.NODE.agent_version,
            "monitor": {"status": "idle", "status_updated": time.time()},
            "labels": shared.NODE.labels,
            "targets": shared.NODE.targets,
            "services": {"status": {k: v for k, v in states.items()}, "monitor": {"status": "up"}, }
        })


def log_test(*args, **kwargs):
    shared.NODE.log.info(*args, **kwargs)


def set_service_command_cmd(cmd=None):
    EXECS["service_command_exe"] = cmd


def service_command_mock(*args, **kwargs):
    log_test('detect mock service_command(%s, %s)' % (args, kwargs))
    return Popen(EXECS["service_command_exe"])


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

            ("parent avail unknown - stay in wait parents /no start/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "unknown", "overall": "unknown", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "unknown", "overall": "unknown"}},
                 "s-depend-on-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'wait parents', 'wait parents', 'wait parents'),

            ("/no node2 status/, plop must stay in wait parent, /no start/, /node frozen/",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "unknown", "overall": "unknown", "updated": time.time(),
                            "topology": "failover", "monitor": {"status": "unknown", "overall": "unknown"}},
                 "s-depend-on-parent": deepcopy(S_DEPEND_ON_PARENT_STATUS),
             },
             'idle', 'idle', 'idle'),

            ("not enough flex - stay in wait parents, /no start/",
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

            ("local parent not avail - stay in wait parents, /no start/",
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

        mocker.patch.object(Monitor, 'monitor_period', 0.000001)

        set_service_command_cmd(env.Env.syspaths.true)
        service_command = mocker.patch.object(Monitor, 'service_command', side_effect=service_command_mock)
        monitor = Monitor()

        if '/no node2 status/' in title:
            log_test('COMMENT: hack rejoin_grace_period to 0.001')
            monitor._lazy_rejoin_grace_period = 0.001

        def do():
            log_comment('\nCOMMENT: monitor.do()')
            time.sleep(0.01)  # Simulate time elapsed between 2 do() iteration
            monitor.do()
            log_test('COMMENT: proc ps count = %s', len(monitor.procs))

        log_test('\n===============================')
        log_test('COMMENT: starting test for %s', title)
        log_test('COMMENT: audited service "%s" expected transition 1: %s, 2: %s, final: %s',
                 svcname, transition_status1, transition_status2, final_status)
        create_svc_config(svcname)
        log_test('COMMENT: other services')
        for s in svcnames:
            create_svc_config(s)
        monitor._lazy_cluster_nodes = [env.Env.nodename, "node2"]

        log_test('COMMENT: monitor.init()')
        monitor.init()
        time.sleep(0.01)  # simulate time elapsed
        assert_nmon_status(log_comment, 'init', 'after init()')
        log_comment('COMMENT: asserting all services are refreshed: (service_command.call_args_list is correct: %s)',
                    service_command.call_args_list[0])
        assert service_command.call_count == 1
        for s in svcnames + [svcname, 'cluster']:
            assert s in service_command.call_args_list[0][0][0].split(',')
        assert service_command.call_args_list[0][0][1] == ['status', '--parallel', '--refresh']
        assert service_command.call_args_list[0][1] == {"local": False}

        create_cluster_status(monitor)

        for i in range(3):
            do()
            assert_nmon_status(monitor, 'init', 'after do(), when all status.json not yet present')
            if i == 1:
                if '/no node2 status/' not in title:
                    init_other_node_states(states=deepcopy(other_node_states), nodename="node2", monitor=monitor)

        for s in svcnames + [svcname]:
            do()
            assert_nmon_status(monitor, 'init', 'after do(), when all status.json not yet present')
            if "local parent is avail" in title and s in svcnames:
                avail = "up"
            else:
                avail = "down"
            if 'local frozen' in title and s == svcname:
                frozen = time.time()
                log_test('COMMENT: create frozen flag for %s', s)
                open(svc_pathvar(s, "frozen"), 'w').close()
            else:
                frozen = 0
            log_test('COMMENT: create local status.json for %s with avail: %s', s, avail)
            status = deepcopy(other_node_states.get(s, {}))
            status['avail'] = avail
            status['updated'] = time.time()
            status['monitor'] = {"status": avail, "status_updated": time.time()}
            status['frozen'] = frozen
            open(svc_pathvar(s, "status.json"), 'w').write(json.dumps(status))
            post_object_status(thr=monitor, path=s, status=status)

        svc = monitor.get_service(svcname)
        assert svc

        do()
        assert_nmon_status(monitor, 'idle', 'after status.json created')
        assert_smon_status(monitor, svcname, transition_status1)

        log_test('COMMENT: hack ready_period to 0.001')
        monitor._lazy_ready_period = 0.001

        if "/start failed/" in title:
            log_test('COMMENT: hack service command mock to %s', env.Env.syspaths.false)
            set_service_command_cmd(env.Env.syspaths.false)
        do()
        assert_smon_status(monitor, svcname, transition_status2)

        if "/has start/" in title:
            assert_command_has_been_launched([((svcname, ['start']), {}), ], service_command.call_args_list)
            if "/start failed/" not in title:
                for svcname in [svcname]:
                    log_test('COMMENT: create status.json for %s with avail up', svcname)
                    status = deepcopy(other_node_states[svcname])
                    status['avail'] = "up"
                    status['updated'] = time.time()
                    open(svc_pathvar(svcname, "status.json"), 'w').write(json.dumps(status))
                    post_object_status(thr=monitor, path=svcname, status=status)
        else:
            assert ((svcname, ['start']), {}) not in service_command.call_args_list

        do()
        assert_smon_status(monitor, svcname, final_status)

        for i in range(3):
            do()
            assert_smon_status(monitor, svcname, final_status)
            if '/no start/' in title:
                assert_command_has_not_been_launched([((svcname, ['start']), {}), ], service_command.call_args_list)
            if '/has start/' in title:
                assert_command_has_been_launched([((svcname, ['start']), {}), ], service_command.call_args_list)
                if "/start failed/" not in title:
                    assert_smon_local_expect(monitor, svcname, 'started')

        if "/node frozen/" in title:
            assert monitor.node_frozen > 0
        else:
            assert monitor.node_frozen == 0
