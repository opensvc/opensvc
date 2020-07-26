import os

import pytest

import daemon.shared as shared
from core.node import Node
from core.objects.ccfg import Ccfg
from env import Env


@pytest.fixture(scope='function')
def suicide(mocker):
    return mocker.patch.object(Node, 'suicide')


@pytest.fixture(scope='function')
@pytest.mark.usefixtures('osvc_path_tests')
def thr(osvc_path_tests, mocker):
    shared_thr = shared.OsvcThread()
    shared.NODE = Node()
    shared_thr.log = mocker.MagicMock()
    return shared_thr


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestSharedAddClusterNode:
    @staticmethod
    def test_update_cluster_config_file_with_added_node(osvc_path_tests, thr):
        thr.add_cluster_node('node2')
        with open(os.path.join(str(osvc_path_tests), 'etc', 'cluster.conf'), 'r') as f:
            assert 'nodes = %s node2' % Env.nodename in f.read()

    @staticmethod
    def test_update_cluster_nodes(thr):
        thr.add_cluster_node('node3')
        assert Ccfg().cluster_nodes == [Env.nodename, 'node3']

    @staticmethod
    @pytest.mark.parametrize('nodename', ['', None])
    def test_log_warning_if_called_with_empty_nodename(thr, nodename):
        thr.add_cluster_node(nodename)
        thr.log.warning.assert_called_once_with('add_cluster_node called with empty nodename')


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestSharedRemoveClusterNode:
    @staticmethod
    @pytest.mark.parametrize('nodename', ['', None])
    def test_log_warning_if_called_with_empty_nodename(thr, nodename):
        thr.remove_cluster_node(nodename)
        thr.log.warning.assert_called_once_with('remove_cluster_node called with empty nodename')


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestSplitHandlerWhenQuorumKwIsTrue:
    @staticmethod
    @pytest.mark.parametrize('split_action, expected', [
        [None, 'crash'],
        ['crash', 'crash'],
        ['reboot', 'reboot'],
    ])
    def test_when_need_suicide_use_correct_split_action(mocker, suicide, thr, split_action, expected):
        if split_action:
            setattr(thr, '_lazy_split_action', split_action)
        thr._lazy_quorum = True
        thr._lazy_cluster_nodes = thr.cluster_nodes + ['node2']
        mocker.patch.object(thr, 'arbitrators_votes', return_value=[])
        thr.split_handler()
        suicide.assert_called_once_with(delay=2, method=expected)

    @staticmethod
    @pytest.mark.parametrize('cfg_nodes, cfg_arbitrators, live_nodes, arbitrator_votes, split_action_count', [
        # 2 nodes, 0 arbitrator: total votes = 2
        [2, 0, 2, 0, 0],  # 2 nodes alive, 0 arbitrator vote, no suicide
        [2, 0, 1, 0, 1],  # 1 nodes alive, 0 arbitrator vote, suicide

        # 2 nodes, 1 arbitrator: total votes = 3
        [2, 1, 2, 0, 0],  # 2 nodes alive, 0 arbitrator vote, no suicide
        [2, 1, 2, 1, 0],  # 2 nodes alive, 1 arbitrator vote, no suicide
        [2, 1, 1, 1, 0],  # 1 node alive, 1 arbitrator vote => no suicide
        [2, 1, 1, 0, 1],  # 1 node alive, no arbitrator vote => suicide

        # 3 nodes, 1 arbitrator: total votes = 4
        [3, 1, 3, 1, 0],  # 3 nodes alive, 1 arbitrator vote, no suicide
        [3, 1, 3, 0, 0],  # 3 nodes alive, 0 arbitrator vote, no suicide
        [3, 1, 2, 1, 0],  # 2 nodes alive, 1 arbitrator vote, no suicide
        [3, 1, 1, 1, 1],  # 1 node alive, 1 arbitrator vote, suicide
        [3, 1, 1, 0, 1],  # 1 node alive, 0 arbitrator vote, suicide

        # 3 nodes, 2 arbitrator: total votes = 5
        [3, 2, 3, 2, 0],  # 3 nodes alive, 2 arbitrator vote, no suicide
        [3, 2, 3, 1, 0],  # 3 nodes alive, 1 arbitrator vote, no suicide
        [3, 2, 3, 0, 0],  # 3 nodes alive, 0 arbitrator vote, no suicide
        [3, 2, 2, 2, 0],  # 2 nodes alive, 2 arbitrator vote, no suicide
        [3, 2, 2, 1, 0],  # 2 nodes alive, 1 arbitrator vote, no suicide
        [3, 2, 2, 0, 1],  # 2 nodes alive, 0 arbitrator vote, suicide
        [3, 2, 1, 2, 0],  # 1 nodes alive, 2 arbitrator vote, no suicide
        [3, 2, 1, 1, 1],  # 1 nodes alive, 1 arbitrator vote, suicide
        [3, 2, 1, 0, 1],  # 1 nodes alive, 0 arbitrator vote, suicide
    ])
    def test_call_suicide(
            mocker,
            suicide,
            thr,
            cfg_nodes,
            cfg_arbitrators,
            live_nodes,
            arbitrator_votes,
            split_action_count):
        thr._lazy_quorum = True
        thr._lazy_cluster_nodes = thr.cluster_nodes + ['node' + str(i) for i in range(1, cfg_nodes)]
        mocker.patch.object(thr, 'arbitrators_config_count', return_value=cfg_arbitrators)
        mocker.patch.object(thr, 'live_nodes_count', return_value=live_nodes)
        mocker.patch.object(thr, 'arbitrators_votes',
                            return_value=["arb" + str(i) for i in range(0, arbitrator_votes)])
        thr.split_handler()
        assert suicide.call_count == split_action_count
