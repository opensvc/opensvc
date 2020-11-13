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

try:
    import utilities.journaled_data
    ready = False
except:
    ready = True

if not ready:
    pytest.skip("not yet ready for tests", allow_module_level=True)

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


def assert_nmon_status(log_info, status, message='reason'):
    log_info('COMMENT: ASSERT monitor status is "%s" (reason: %s), full value is %s',
             status, message, json.dumps(shared.NMON_DATA))
    assert shared.NMON_DATA['status'] == status


def assert_smon_status(log_info, svcname, status, message='reason'):
    log_info('COMMENT: ASSERT smon status for svc %s is "%s" (reason: %s), full value is %s',
             svcname, status, message, json.dumps(shared.SMON_DATA[svcname]))
    assert shared.SMON_DATA[svcname]['status'] == status


def assert_smon_local_expect(log_info, svcname, local_expect, message='reason'):
    log_info('COMMENT: ASSERT smon local_expect for svc %s is "%s" (reason: %s), full value is %s',
             svcname, local_expect, message, json.dumps(shared.SMON_DATA[svcname]))
    assert shared.SMON_DATA[svcname]['local_expect'] == local_expect


def assert_command_has_been_launched(log_info, call_list, call_args_list):
    for call in call_list:
        log_info('COMMENT: ASSERT call %s has been called', call)
        assert call in call_args_list

def assert_command_has_not_been_launched(log_info, call_list, call_args_list):
    for call in call_list:
        log_info('COMMENT: ASSERT call %s has not been called', call)
        assert call not in call_args_list


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
        execs = {"service_command_exe": env.Env.syspaths.true}
        mocker.patch.object(Monitor, 'monitor_period', 0.000001)

        def set_service_command_cmd(cmd):
            execs["service_command_exe"] = cmd

        def service_command_mock(*args, **kwargs):
            log_comment('detect mock service_command(%s, %s)' % (args, kwargs))
            return Popen(execs["service_command_exe"])

        def log_comment(*args, **kwargs):
             if monitor and getattr(monitor, 'log'):
                 monitor.log.info(*args, **kwargs)
             else:
                 shared.NODE.log.info(*args, **kwargs)

        def create_svc_config(s):
            pathetc = env.Env.paths.pathetc
            if not os.path.exists(pathetc):
                os.mkdir(pathetc)
            with open(os.path.join(pathetc, '%s.conf' % s), mode='w+') as svc_file:
                svc_file.write(SERVICES[s])
            log_comment('COMMENT: created %s service with config \n%s\n', s, SERVICES[s])

        service_command = mocker.patch.object(Monitor, 'service_command', side_effect=service_command_mock)
        monitor = Monitor()

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
            log_comment('COMMENT: hack rejoin_grace_period to 0.001')
            monitor._lazy_rejoin_grace_period = 0.001

        def do():
            log_comment('\nCOMMENT: monitor.do()')
            monitor.do()
            log_comment('COMMENT: proc ps count = %s', len(monitor.procs))

        log_comment('\n===============================')
        log_comment('COMMENT: starting test for %s', title)
        log_comment('COMMENT: audited service "%s" expected transition 1: %s, 2: %s, final: %s',
                    svcname, transition_status1, transition_status2, final_status)
        create_svc_config(svcname)
        log_comment('COMMENT: other services')
        for s in svcnames:
            create_svc_config(s)
        monitor._lazy_cluster_nodes = [env.Env.nodename, "node2"]

        log_comment('COMMENT: monitor.init()')
        monitor.init()
        assert_nmon_status(log_comment, 'init', 'after init()')
        log_comment('COMMENT: asserting all services are refreshed: (service_command.call_args_list is correct: %s)',
                    service_command.call_args_list[0])
        assert service_command.call_count == 1
        for s in svcnames + [svcname, 'cluster']:
            assert s in service_command.call_args_list[0][0][0].split(',')
        assert service_command.call_args_list[0][0][1] == ['status', '--parallel', '--refresh']
        assert service_command.call_args_list[0][1] == {"local": False}

        log_comment('COMMENT: create ccfg/cluster status.json')
        if not os.path.exists(svc_pathvar("ccfg/cluster")):
            os.makedirs(svc_pathvar("ccfg/cluster"))
        open(svc_pathvar("ccfg/cluster", "status.json"), 'w').\
            write(json.dumps({"updated": time.time() + 1, "kind": "ccfg"}))

        for i in range(3):
            do()
            assert_nmon_status(log_comment, 'init', 'after do(), when all status.json not yet present')

        for s in svcnames + [svcname]:
            do()
            assert_nmon_status(log_comment, 'init', 'after do(), when all status.json not yet present')
            if "local parent is avail" in title and s in svcnames:
                avail = "up"
            else:
                avail = "down"
            if 'local frozen' in title and s == svcname:
                frozen = time.time()
                log_comment('COMMENT: create frozen flag for %s', s)
                open(svc_pathvar(s, "frozen"), 'w').close()
            else:
                frozen = 0
            log_comment('COMMENT: create local status.json for %s with avail: %s', s, avail)
            status = deepcopy(other_node_status.get(s, {}))
            status['avail'] = avail
            status['updated'] = time.time()
            status['monitor'] = {}
            status['frozen'] = frozen
            open(svc_pathvar(s, "status.json"), 'w').write(json.dumps(status))

        svc = monitor.get_service(svcname)
        assert svc

        time.sleep(0.001)
        do()
        assert_nmon_status(log_comment, 'idle', 'after status.json created')
        assert_smon_status(log_comment, svcname, transition_status1)

        log_comment('COMMENT: hack ready_period to 0.001')
        monitor._lazy_ready_period = 0.001
        time.sleep(0.001)

        if "/start failed/" in title:
            log_comment('COMMENT: hack service command mock to %s', env.Env.syspaths.false)
            set_service_command_cmd(env.Env.syspaths.false)
        do()
        assert_smon_status(log_comment, svcname, transition_status2)

        if "/has start/" in title:
            assert_command_has_been_launched(log_comment, [((svcname, ['start']), {}), ], service_command.call_args_list)
            if "/start failed/" not in title:
                for svcname in [svcname]:
                    log_comment('COMMENT: create status.json for %s with avail up', svcname)
                    status = deepcopy(other_node_status[svcname])
                    status['avail'] = "up"
                    status['updated'] = time.time()
                    open(svc_pathvar(svcname, "status.json"), 'w').write(json.dumps(status))
        else:
            assert ((svcname, ['start']), {}) not in service_command.call_args_list

        do()
        assert_smon_status(log_comment, svcname, final_status)

        for i in range(3):
            do()
            assert_smon_status(log_comment, svcname, final_status)
            if '/no start/' in title:
                assert_command_has_not_been_launched(
                    log_comment,
                    [((svcname, ['start']), {}), ],
                    service_command.call_args_list)
            if '/has start/' in title:
                assert_command_has_been_launched(
                    log_comment,
                    [((svcname, ['start']), {}), ],
                    service_command.call_args_list)
                if "/start failed/" not in title:
                    assert_smon_local_expect(log_comment, svcname, 'started')

        if "/node frozen/" in title:
            assert monitor.node_frozen > 0
        else:
            assert monitor.node_frozen == 0
