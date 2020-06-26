import os

import pytest

import daemon.shared as shared
from core.node import Node
from core.objects.ccfg import Ccfg
from env import Env


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
