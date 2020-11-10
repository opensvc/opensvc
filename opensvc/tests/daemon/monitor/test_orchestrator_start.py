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


def assert_nmon_status(monitor, status, message='reason'):
    monitor.log.info('==> assert monitor status is "%s" (reason: %s), full value is %s',
                     status, message, json.dumps(shared.NMON_DATA))
    assert shared.NMON_DATA['status'] == status


def assert_smon_status(monitor, svcname, status, message='reason'):
    monitor.log.info('==> assert smon status for svc %s is "%s" (reason: %s), full value is %s',
                     svcname, status, message, json.dumps(shared.SMON_DATA[svcname]))
    assert shared.SMON_DATA[svcname]['status'] == status


def assert_smon_local_expect(monitor, svcname, local_expect, message='reason'):
    monitor.log.info('==> assert smon local_expect for svc %s is "%s" (reason: %s), full value is %s',
                     svcname, local_expect, message, json.dumps(shared.SMON_DATA[svcname]))
    assert shared.SMON_DATA[svcname]['local_expect'] == local_expect


def assert_command_has_been_launched(monitor, call_list, call_args_list):
    for call in call_list:
        monitor.log.info('==> assert call %s has been called', call)
        assert call in call_args_list


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
            ("parent avail - but start failed",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                        "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'ready', 'starting', 'start failed'),

            ("simple parent avail - will start",
             ['parent', ],
             's-depend-on-parent',
             {
                 "parent": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                        "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'ready', 'starting', 'idle'),

            ("parent avail unknown - stay in wait parents",
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

            ("no parent remote status, must stay in wait parent",
             ['parent', ],
             's-depend-on-parent',
             {
                 "s-depend-on-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                        "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             # Should be 'wait parents', 'wait parents', 'wait parents'
             'ready', 'starting', 'idle'),

            ("not enough flex - stay in wait parents",
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

            ("enough flex instances - will start",
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

            ("local parent not avail - stay in wait parents",
             ['parent', ],
             's-depend-on-local-parent',
             {
                 "parent": {"avail": "up", "overall": "up", "updated": time.time(), "topology": "failover",
                            "monitor": {"status": "up", "overall": "up"}},
                 "s-depend-on-local-parent": {"avail": "down", "overall": "down", "updated": time.time(),
                                              "topology": "failover", "monitor": {"status": "down", "overall": "down"}}
             },
             'wait parents', 'wait parents', 'wait parents'),
        ])
    def test_flex_service_has_correct_return_value(
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
            monitor.log.info('calling service_command(%s, %s)' % (args, kwargs))
            return Popen(execs["service_command_exe"])

        pathetc = env.Env.paths.pathetc

        for s in svcnames + [svcname]:
            if not os.path.exists(pathetc):
                os.mkdir(pathetc)
            with open(os.path.join(pathetc, '%s.conf' % s), mode='w+') as svc_file:
                svc_file.write(SERVICES[s])

        service_command = mocker.patch.object(Monitor, 'service_command', side_effect=service_command_mock)
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

        def do():
            monitor.log.info('\n==> monitor.do()')
            monitor.do()
            monitor.log.info('proc ps count = %s', len(monitor.procs))

        monitor = Monitor()
        shared.NODE.log.info('starting test for %s', title)
        monitor._lazy_cluster_nodes = [env.Env.nodename, "node2"]

        shared.NODE.log.info('\n==> monitor.init()')
        monitor.init()
        assert_nmon_status(monitor, 'init', 'after init()')
        monitor.log.info('==> asserting all services are refreshed: (service_command.call_args_list is correct: %s)',
                         service_command.call_args_list[0])
        assert service_command.call_count == 1
        for s in svcnames + [svcname, 'cluster']:
            assert s in service_command.call_args_list[0][0][0].split(',')
        assert service_command.call_args_list[0][0][1] == ['status', '--parallel', '--refresh']
        assert service_command.call_args_list[0][1] == {"local": False}

        monitor.log.info('==> create ccfg/cluster status.json')
        if not os.path.exists(svc_pathvar("ccfg/cluster")):
            os.makedirs(svc_pathvar("ccfg/cluster"))
        open(svc_pathvar("ccfg/cluster", "status.json"), 'w').\
            write(json.dumps({"updated": time.time() + 1, "kind": "ccfg"}))

        for i in range(3):
            do()
            assert_nmon_status(monitor, 'init', 'after do(), when status.json not yet present')

        svc = monitor.get_service(svcname)
        assert svc

        for svcname in svcnames + [svcname]:
            monitor.log.info('=> create status.json for %s with avail: down', svcname)
            status = deepcopy(other_node_status.get(svcname, {}))
            status['avail'] = "down"
            status['updated'] = time.time()
            status['monitor'] = {}
            open(svc_pathvar(svcname, "status.json"), 'w').write(json.dumps(status))

        do()
        assert_nmon_status(monitor, 'idle', 'after status.json created')
        assert_smon_status(monitor, svcname, transition_status1)

        monitor.log.info('set ready period to 0.00000001')
        monitor._lazy_ready_period = 0.00000001
        if 'start failed' in title:
            monitor.log.info('set service command mock to %s', env.Env.syspaths.false)
            set_service_command_cmd(env.Env.syspaths.false)
        do()
        assert_smon_status(monitor, svcname, transition_status2)
        if 'start failed' in title:
            assert_command_has_been_launched(monitor, [((svcname, ['start']), {}), ], service_command.call_args_list)
        elif 'starting' in transition_status2:
            assert_command_has_been_launched(monitor, [((svcname, ['start']), {}), ], service_command.call_args_list)
            for svcname in [svcname]:
                monitor.log.info('=> create status.json for %s with avail up', svcname)
                status = deepcopy(other_node_status[svcname])
                status['avail'] = "up"
                status['updated'] = time.time()
                open(svc_pathvar(svcname, "status.json"), 'w').write(json.dumps(status))
        else:
            assert ((svcname, ['start']), {}) not in service_command.call_args_list

        do()
        assert_smon_status(monitor, svcname, final_status)

        for i in range(3):
            do()
            assert_smon_status(monitor, svcname, final_status)
            if 'idle' in final_status:
                assert_smon_local_expect(monitor, svcname, 'started')
