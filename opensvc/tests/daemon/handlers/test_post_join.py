import pytest

import daemon.shared as shared
from core.exceptions import HTTP

from core.node import Node
from core.objects.ccfg import Ccfg
from daemon.handlers.join.post import Handler as PostJoin
from env import Env


@pytest.fixture(scope='function')
def thr(mocker):
    mocker.patch('daemon.handlers.join.post.LOCK_TIMEOUT', 0.001)
    mocker.patch('daemon.clusterlock.DELAY_TIME', 0.001)
    shared_thr = shared.OsvcThread()
    shared_thr.log = mocker.MagicMock()
    return shared_thr


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('shared_data')
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

    @staticmethod
    def test_raise_503_when_lock_not_available(thr):
        post_join = PostJoin()
        post_join.lock_acquire(Env.nodename, "join", timeout=120, thr=thr)
        with pytest.raises(HTTP, match='status 503: Lock not acquired'):
            post_join.action('node3', thr=thr)

    @staticmethod
    def test_dont_update_cluster_config_when_lock_not_available(thr):
        post_join = PostJoin()
        post_join.lock_acquire(Env.nodename, "join", timeout=120, thr=thr)
        try:
            post_join.action('node3', thr=thr)
        except:
            pass
        assert Ccfg().cluster_nodes == [Env.nodename]

    @staticmethod
    def test_succeed_when_lock_is_not_join_lock(thr):
        post_join = PostJoin()
        post_join.lock_acquire(Env.nodename, "something", timeout=120, thr=thr)
        post_join.action('node3', thr=thr)
