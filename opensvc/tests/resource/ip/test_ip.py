import time

import pytest

import core.exceptions as ex
from core.objects.svc import Svc
from utilities.drivers import driver_import, driver_class

OS_LIST = set(['AIX', 'Darwin', 'FreeBSD', 'HP-UX', 'Linux', 'OSF1', 'SunOS', 'Windows'])

IP_0 = {
    "DEFAULT": {"env": "tst"},
    "ip#0": {
        "ipname": "192.168.0.149",
        "ipdev": "eth0",
        "netmask": "24",
        "wait_dns": "40"
    }
}


@pytest.fixture()
def svc():
    svc = Svc(name="foo", cd=dict(IP_0))
    svc.options.debug = True
    svc.options.master = True
    svc.get_node()  # Some ip methods need svc.node
    return svc


@pytest.fixture()
def node_wait_get_time(mocker):
    now = int(time.time())
    path = 'core.node.node'
    mocker.patch('%s._wait_delay' % path)
    return mocker.patch('%s._wait_get_time' % path,
                        side_effect=range(now, now + 100))


@pytest.fixture()
def node_daemon_get(mocker, svc):
    return mocker.patch.object(svc.node, 'daemon_get')


@pytest.fixture(params=OS_LIST)
def ip_class(request, mock_sysname):
    mock_sysname(request.param)
    return driver_class(driver_import('resource', 'ip', 'Host'))


# Mock ip methods
@pytest.fixture()
def startip_cmd(mocker, ip_class):
    return mocker.patch.object(ip_class, 'startip_cmd', return_value=(0, '', ''))


@pytest.fixture()
def dns_update(mocker, ip_class):
    return mocker.patch.object(ip_class, 'dns_update')


@pytest.fixture()
def wait_dns_records(mocker, ip_class):
    return mocker.patch.object(ip_class, 'wait_dns_records')


@pytest.fixture()
def arp_announce(mocker, ip_class):
    return mocker.patch.object(ip_class, 'arp_announce')


@pytest.fixture()
def current_time(mocker, ip_class):
    now = int(time.time())
    return mocker.patch.object(ip_class, '_current_time',
                               side_effect=range(now, now + 100))

@pytest.fixture()
def wait_dns_records_delay(mocker, ip_class):
    return mocker.patch('drivers.resource.ip.wait_dns_records_delay_func')


# mock external utilities.ping.check_ping
@pytest.fixture()
def check_ping(mocker):
    return mocker.patch('utilities.ping.check_ping', return_value=False)


@pytest.fixture()
def check_ping_is_false(check_ping):
    check_ping.return_value = False


@pytest.fixture()
def check_ping_is_true(check_ping):
    check_ping.return_value = True


# mock external utilities.ifconfig.Ifconfig
@pytest.fixture()
def get_ifconfig(mocker):
    return mocker.patch('utilities.ifconfig.Ifconfig')


@pytest.fixture()
def ifconfig_has_ip_addr_local(get_ifconfig):
    get_ifconfig.return_value.has_param.return_value.ipaddr = ['192.168.0.149']
    get_ifconfig.return_value.has_param.return_value.mask = ['255.255.255.0']
    return get_ifconfig.return_value


@pytest.fixture()
def ifconfig_has_not_ip_local(get_ifconfig):
    get_ifconfig.return_value.has_param.return_value = None
    get_ifconfig.return_value.interface.return_value.flag_no_carrier = False
    return get_ifconfig.return_value


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('ifconfig_has_not_ip_local', 'check_ping_is_false')
@pytest.mark.usefixtures('startip_cmd', 'dns_update', 'arp_announce')
class TestIpStartWhenNoIpLocalAndNoPing:
    """
    Test a start when we need actions callst ping

    Following methods need extra tests depending on sysname value:
        startip_cmd
        dns_update
        wait_dns_records
        arp_announce
    """

    @staticmethod
    @pytest.mark.usefixtures('wait_dns_records')
    def test_no_exceptions(svc):
        svc.start()

    @staticmethod
    def test_calls_startip_cmd_and_dns_update_and_wait_dns_records(
            startip_cmd,
            arp_announce,
            dns_update,
            wait_dns_records,
            svc):
        svc.start()

        assert startip_cmd.call_count == 1
        assert arp_announce.call_count == 1
        assert dns_update.call_count == 1
        assert wait_dns_records.call_count == 1

    @staticmethod
    @pytest.mark.usefixtures('node_wait_get_time')
    @pytest.mark.usefixtures('wait_dns_records_delay')
    def test_raise_when_all_daemon_get_calls_fails_also_ensure_no_call_daemon_get_call_with_negative_timeout(
            node_daemon_get,
            current_time,
            svc):
        node_daemon_get.side_effect = [{"status": 1, "errors": {}, "info": {}}] * 50
        with pytest.raises(ex.Error):
            svc.start()

        assert node_daemon_get.call_count > 5
        timeout_args = [args[1].get('timeout') for args in node_daemon_get.call_args_list]
        assert len([timeout for timeout in timeout_args if timeout <= 0]) == 0

    @staticmethod
    @pytest.mark.usefixtures('node_wait_get_time')
    def test_wait_dns_make_retries_on_daemon_get_until_daemon_get_succeed(
            node_daemon_get,
            svc):
        daemon_result = [{"status": 1, "errors": {}, "info": {}}] * 3  # for action wait
        daemon_result += [{"status": 0, "errors": {}, "info": {}, "data": {"satisfied": True}}]  # for action wait
        daemon_result += [{"status": 0, "errors": {}, "info": {}}]  # for action sync
        node_daemon_get.side_effect = daemon_result
        svc.start()
        assert node_daemon_get.call_count == 5


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('ifconfig_has_not_ip_local', 'check_ping_is_true')
class TestIpStartWhenNoIpLocalAndPing:
    @staticmethod
    def test_raise_ip_conflict(svc):
        with pytest.raises(ex.Error, match='aborted.* conflict'):
            svc.start()

    @staticmethod
    def test_no_calls_startip_cmd_or_dns_commands(
            startip_cmd,
            dns_update,
            wait_dns_records,
            svc):
        with pytest.raises(Exception):
            svc.start()

        startip_cmd.assert_not_called()
        dns_update.assert_not_called()
        wait_dns_records.assert_not_called()


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.usefixtures('ifconfig_has_ip_addr_local')
class TestIpStartWhenIpLocal:
    @staticmethod
    def test_succeed(svc):
        svc.start()

    @staticmethod
    def test_no_calls_startip_cmd_or_dns_commands(
            startip_cmd,
            dns_update,
            wait_dns_records,
            svc):
        svc.start()

        startip_cmd.assert_not_called()
        dns_update.assert_not_called()
        wait_dns_records.assert_not_called()
