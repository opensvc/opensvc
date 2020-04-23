import pytest

import core.exceptions as ex
from core.objects.svc import Svc
from utilities.drivers import driver_import, driver_class

OS_LIST = set(['AIX', 'Darwin', 'FreeBSD', 'HP-UX', 'Linux', 'OSF1', 'SunOS', 'Windows'])

IP_0 = {
    "DEFAULT": {},
    "ip#0": {
        "ipname": "192.168.0.149",
        "ipdev": "eth0",
        "netmask": "24"
    }
}


@pytest.fixture()
def svc():
    svc = Svc(name="foo", cd=dict(IP_0))
    svc.options.debug = True
    svc.options.master = True
    return svc


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


# mock external utilities.ping.check_ping
@pytest.fixture()
def check_ping(mocker, ip_class):
    return mocker.patch('utilities.ping.check_ping', return_value=False)


@pytest.fixture()
def check_ping_is_false(check_ping):
    check_ping.return_value = False


@pytest.fixture()
def check_ping_is_true(check_ping):
    check_ping.return_value = True


# mock external utilities.ifconfig.Ifconfig
@pytest.fixture()
def get_ifconfig(mocker, ip_class):
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
class TestIpStartWhenNoIpLocalAndNoPing:
    @staticmethod
    @pytest.mark.usefixtures('startip_cmd', 'dns_update', 'wait_dns_records')
    def test_no_exceptions(svc):
        svc.start()

    @staticmethod
    def test_calls_startip_cmd_and_dns_update_and_wait_dns_records(
            startip_cmd,
            dns_update,
            wait_dns_records,
            svc):
        svc.start()

        startip_cmd.assert_called_once()
        dns_update.assert_called_once()
        wait_dns_records.assert_called_once()


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
