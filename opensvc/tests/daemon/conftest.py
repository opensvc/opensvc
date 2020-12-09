import pytest

import daemon.shared as shared
from core.node import Node
from env import Env


@pytest.fixture(scope='function')
@pytest.mark.usefixtures('osvc_path_tests')
def shared_data(mocker):
    """
    mock some of shared structures
    """
    mocker.patch.object(shared, 'RX', shared.queue.Queue())
    mocker.patch.object(shared, 'DAEMON_STATUS', shared.OsvcJournaledData())
    mocker.patch.object(shared, 'SERVICES', {})
    mocker.patch.object(shared, 'LOCKS', {})
    shared.DAEMON_STATUS.set([], {"monitor": {"nodes": {Env.nodename: {}}}})
    mocker.patch.object(shared, 'NODE', Node())
