import json
import socket
import uuid
import time

import pytest

from core.comm import Crypt, PAUSE, SOCK_TMO_REQUEST
from core.node import Node
from env import Env

MSG_TIMEOUT_CONNECT = 'timeout daemon request (connect error)'
MSG_TIMEOUT_RECV = 'timeout daemon request (recv_message error)'


@pytest.fixture()
def get_cluster_name(mocker):
    return mocker.patch.object(Crypt, 'get_cluster_name', return_value='demo')


@pytest.fixture()
def get_secret(mocker):
    return mocker.patch.object(Crypt, 'get_secret', return_value='secret-value')


@pytest.fixture()
def recv_message(mocker):
    return mocker.patch.object(Crypt, 'recv_message',
                               return_value={'uuid': uuid.uuid4()})


@pytest.fixture()
def crypt(mocker):
    crypt = Crypt()
    crypt.log = mocker.Mock(name='log')
    return crypt


# Mock external socket
@pytest.fixture()
def created_socket(mocker):
    return mocker.patch.object(socket, 'socket').return_value


# Mock external time.sleep
@pytest.fixture()
def time_sleep(mocker):
    return mocker.patch.object(time, 'sleep')


@pytest.mark.ci
@pytest.mark.usefixtures('get_cluster_name')
@pytest.mark.usefixtures('get_secret')
@pytest.mark.usefixtures('time_sleep')
class TestRawDaemonRequestWithNoTimeout:
    @staticmethod
    @pytest.mark.usefixtures('created_socket')
    def test_returns_received_message(recv_message, crypt):
        assert crypt.raw_daemon_request(data={}) == recv_message.return_value

    @staticmethod
    @pytest.mark.usefixtures('created_socket', 'recv_message')
    def test_no_extra_sleep(time_sleep, crypt):
        crypt.raw_daemon_request(data={})
        assert time_sleep.call_count == 0

    @staticmethod
    @pytest.mark.usefixtures('recv_message')
    def test_cleanup_connexion(created_socket, crypt):
        assert crypt.raw_daemon_request(data={})
        assert created_socket.close.call_count == 1

    @staticmethod
    @pytest.mark.usefixtures('created_socket')
    def test_returns_success_without_tries_to_recv_results_when_with_result_param_is_false(
            recv_message,
            crypt):
        assert crypt.raw_daemon_request(data={}, with_result=False) == {"status": 0}
        assert recv_message.call_count == 0

    @staticmethod
    @pytest.mark.parametrize('method', ['GET', 'POST'])
    @pytest.mark.usefixtures('recv_message')
    def test_it_sends_correct_message(created_socket, crypt, method):
        crypt.raw_daemon_request(data={'todo': 'start'}, method=method)
        expected_payload = {'GET': {"todo": "start", "method": "GET"},
                            'POST': {"todo": "start", "method": "POST"}}
        sendall_string = created_socket.sendall.call_args[0][0].decode()
        sendall_dict = json.loads(sendall_string.rstrip('\x00'))
        assert sendall_dict == expected_payload[method]

    @staticmethod
    def test_retries_connect_until_succeed(time_sleep, created_socket, recv_message, crypt):
        connect_timeouts = 3
        created_socket.connect.side_effect = [socket.timeout] * connect_timeouts + [None]
        assert crypt.raw_daemon_request(data={}) == recv_message.return_value
        assert created_socket.connect.call_count == connect_timeouts + 1
        assert time_sleep.call_count == 3

    @staticmethod
    @pytest.mark.usefixtures('created_socket')
    def test_retries_recv_until_succeed(time_sleep, recv_message, crypt):
        recv_timeouts = 3
        recv_message.side_effect = [socket.timeout] * recv_timeouts + [{'status': 0}]
        assert crypt.raw_daemon_request(data={}) == {'status': 0}
        assert recv_message.call_count == recv_timeouts + 1
        assert time_sleep.call_count == 3

    @staticmethod
    def test_returns_status_1_when_connect_error(time_sleep, created_socket, crypt):
        created_socket.connect.side_effect = socket.error('connect_error')
        expected_result = {'errno': None, 'error': 'connect_error', 'retryable': False, 'status': 1}
        assert crypt.raw_daemon_request(data={}) == expected_result

    @staticmethod
    @pytest.mark.usefixtures('created_socket')
    def test_returns_status_1_when_recv_error(time_sleep, recv_message, crypt):
        recv_message.side_effect = [socket.timeout] * 2 + [socket.error('recv_error')]
        expected_result = {'errno': None, 'error': 'recv_error', 'retryable': False, 'status': 1}
        assert crypt.raw_daemon_request(data={}) == expected_result
        assert time_sleep.call_count == 2

    @staticmethod
    def test_always_retry_when_connect_or_recv_error(created_socket, recv_message, crypt):
        created_socket.connect.side_effect = [socket.timeout] * 15 + [None]
        recv_message.side_effect = [socket.timeout] * 13 + [socket.error]
        crypt.raw_daemon_request(data={})
        assert created_socket.connect.call_count > 15
        assert recv_message.call_count > 13

    @staticmethod
    def test_result_is_status_1_with_when_send_message_fails(created_socket, crypt):
        created_socket.sendall.side_effect = socket.error('sendall error')
        assert crypt.raw_daemon_request(data={}) == {
            'errno': None,
            'error': 'sendall error',
            'retryable': False,
            'status': 1
        }


@pytest.mark.ci
@pytest.mark.usefixtures('get_cluster_name')
@pytest.mark.usefixtures('get_secret')
@pytest.mark.usefixtures('time_sleep')
class TestRawDaemonRequestWithTimeoutIsPositive:
    @staticmethod
    @pytest.mark.usefixtures('created_socket')
    def test_returns_received_message(recv_message, crypt):
        assert crypt.raw_daemon_request(data={}, timeout=3) == recv_message.return_value

    @staticmethod
    def test_fails_when_duration_exceed_timeout_because_of_connect_timeouts(created_socket, crypt):
        nb_connect_timeouts = 10
        timeout = 9 * (SOCK_TMO_REQUEST + PAUSE)
        created_socket.connect.side_effect = [socket.timeout] * nb_connect_timeouts + [None]
        expected_result = {'err': MSG_TIMEOUT_CONNECT, 'status': 1}
        assert crypt.raw_daemon_request(data={}, timeout=timeout) == expected_result

    @staticmethod
    @pytest.mark.usefixtures('created_socket')
    def test_fails_when_duration_exceed_timeout(recv_message, crypt):
        recv_message.side_effect = [socket.timeout] * 10 + [{'status': 0}]
        timeout = 9 * (SOCK_TMO_REQUEST + PAUSE)
        expected_result = {'status': 1, 'err': MSG_TIMEOUT_RECV}
        assert crypt.raw_daemon_request(data={}, timeout=timeout) == expected_result

    @staticmethod
    @pytest.mark.usefixtures('created_socket')
    def test_fails_when_duration_exceed_timeout(recv_message, crypt):
        recv_message.side_effect = [socket.timeout] * 10 + [{'status': 0}]
        timeout = 9 * (SOCK_TMO_REQUEST + PAUSE)
        expected_result = {'status': 1, 'err': MSG_TIMEOUT_RECV}
        assert crypt.raw_daemon_request(data={}, timeout=timeout) == expected_result


@pytest.mark.ci
@pytest.mark.usefixtures('get_cluster_name')
@pytest.mark.usefixtures('get_secret')
@pytest.mark.usefixtures('time_sleep')
class TestRawDaemonRequestWithTimeoutParamZero:
    @staticmethod
    def test_failfast_when_connect_error(time_sleep, created_socket, crypt):
        created_socket.connect.side_effect = socket.timeout
        assert crypt.raw_daemon_request(data={}, timeout=0) == {'status': 1, 'err': MSG_TIMEOUT_CONNECT}
        assert time_sleep.call_count == 0

    @staticmethod
    @pytest.mark.usefixtures('created_socket')
    def test_failfast_when_recv_message_fails(time_sleep, recv_message, crypt):
        recv_message.side_effect = socket.timeout
        assert crypt.raw_daemon_request(data={}, timeout=0) == {'status': 1, 'err': MSG_TIMEOUT_RECV}
        assert time_sleep.call_count == 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestClusterNodes:
    @staticmethod
    def test_is_array_with_nodename(mocker):
        mocker.patch.object(Crypt, 'get_node', return_value=Node())
        assert Crypt().cluster_nodes == [Env.nodename]
