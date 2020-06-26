import pytest

import daemon.shared as shared

from core.node import Node
from core.objects.ccfg import Ccfg
from daemon.clusterlock import LockMixin
from daemon.handlers.join.post import Handler as PostJoin
from env import Env


@pytest.fixture(scope='function')
@pytest.mark.usefixtures('osvc_path_tests')
def thr(mocker, osvc_path_tests):
    mocker.patch.object(LockMixin, 'lock_acquire')
    shared_thr = shared.OsvcThread()
    shared_thr.log = mocker.MagicMock()
    shared.NODE = Node()
    return shared_thr


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestPostJoin:
    @staticmethod
    def test_update_cluster_nodes(thr):
        PostJoin().action('node3', thr=thr)
        assert Ccfg().cluster_nodes == [Env.nodename, 'node3']

    @staticmethod
    def test_return_dict_with_status_0(thr):
        assert PostJoin().action('node3', thr=thr)['status'] == 0

    @staticmethod
    def test_return_dict_that_contain_new_nodes(thr):
        response = PostJoin().action('node3', thr=thr)
        assert response['data']['cluster']['data']['cluster']['nodes'] == Env.nodename + ' node3'

    @staticmethod
    def test_return_dict_with_cluster_config_data(thr):
        response = PostJoin().action('node3', thr=thr)
        assert response['data']['cluster']['data'] == Ccfg().print_config_data()
