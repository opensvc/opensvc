import pytest

import daemon.shared as shared
from core.node import Node
from daemon.clusterlock import LockMixin


@pytest.fixture(scope='function')
@pytest.mark.usefixtures('osvc_path_tests')
def thr(mocker, osvc_path_tests):
    mocker.patch.object(LockMixin, 'lock_acquire')
    shared_thr = shared.OsvcThread()
    shared_thr.log = mocker.MagicMock()
    shared.NODE = Node()
    return shared_thr
