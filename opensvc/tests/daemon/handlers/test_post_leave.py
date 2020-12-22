import pytest

from core.node import Node
from core.objects.ccfg import Ccfg
import daemon.shared as shared
from daemon.handlers.leave.post import Handler as PostLeave
from env import Env
from utilities.lazy import unset_lazy


@pytest.fixture(scope='function')
@pytest.mark.usefixtures('osvc_path_tests')
def thr(mocker, osvc_path_tests):
    shared_thr = shared.OsvcThread()
    shared_thr.log = mocker.MagicMock()
    shared.NODE = Node()
    return shared_thr


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestPostLeave:
    @staticmethod
    def test_remove_node_from_cluster_config(thr):
        thr.add_cluster_node('node3')
        unset_lazy(thr, "cluster_nodes")
        unset_lazy(thr, "sorted_cluster_nodes")
        assert Ccfg().cluster_nodes == [Env.nodename, 'node3']
        response = PostLeave().action('node3', thr=thr)
        assert Ccfg().cluster_nodes == [Env.nodename]
        assert response == {'status': 0}

    @staticmethod
    def test_accept_remove_non_cluster_node(thr):
        assert Ccfg().cluster_nodes == [Env.nodename]
        response = PostLeave().action('node3', thr=thr)
        assert response == {'status': 0}
        assert Ccfg().cluster_nodes == [Env.nodename]