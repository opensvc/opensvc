import time

import pytest

import daemon.shared as shared
from daemon.handlers.cluster.lock.get import Handler as GetLock
from env import Env


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('shared_data')
class TestGetLock:
    @staticmethod
    def test_return_empty_cluster_locks_if_no_locks(thr):
        assert GetLock().action(None, thr=thr) == {'data': {}, 'status': 0}

    @staticmethod
    def test_return_copy_of_current_cluster_locks(mocker, thr):
        mocked_locks = {
            "name1": {
                "requested": time.time(),
                "requester": Env.nodename,
                "id": "id1"
            },
            "name2": {
                "requested": time.time(),
                "requester": Env.nodename,
                "id": "id1"
            }
        }
        mocker.patch.object(shared, 'LOCKS', mocked_locks)
        result = GetLock().action(None, thr=thr)
        assert result == {'data': mocked_locks, 'status': 0}
        assert result['data'] is not shared.LOCKS
