import pytest

from core.exceptions import Error
from core.network import NetworksMixin


class TestNetworksMixinFindNodeIp(object):
    loopback_addr_list = ["127.0.0.1", "127.0.1.1", "::1", "fe80:00"]

    @staticmethod
    def test_find_node_ip_raise_error_not_resolvable_when_not_resolvable():
        nodename = 'i-am-not-resolvable-test'
        with pytest.raises(Error, match='node %s is not resolvable' % nodename):
            NetworksMixin().find_node_ip(nodename)

    @staticmethod
    @pytest.mark.parametrize('loopback_addr', loopback_addr_list)
    def test_find_node_ip_raise_error_when_only_loopback_addr_found(mocker, loopback_addr):
        mocker.patch('core.network.socket.getaddrinfo',
                     return_value=[
                         ("family1", "", "", "", (loopback_addr, 0)),
                         ("family2", "", "", "", ("addr", 0)),
                     ])
        with pytest.raises(Error, match="node node1 has no family1 address"):
            NetworksMixin().find_node_ip("node1", "family1")

    @staticmethod
    def test_find_node_ip_return_first_addr_when_resolvable(mocker):
        mocker.patch('core.network.socket.getaddrinfo',
                     return_value=[
                         ("family1", "", "", "", ("addr1", 0)),
                         ("family2", "", "", "", ("addr2", 0)),
                         ("family2", "", "", "", ("addr3", 0)),
                     ])
        assert NetworksMixin().find_node_ip("nodename", "family2") == "addr2"

    @staticmethod
    @pytest.mark.parametrize('loopback_addr', loopback_addr_list)
    def test_find_node_ip_return_first_non_loopback_resolved_addr(
            mocker,
            loopback_addr):
        mocker.patch('core.network.socket.getaddrinfo',
                     return_value=[
                         ("family2", "", "", "", (loopback_addr, 0)),
                         ("family2", "", "", "", ("addr2", 0)),
                     ])
        assert NetworksMixin().find_node_ip("nodename", "family2") == "addr2"
