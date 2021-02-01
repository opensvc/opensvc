import pytest

from core.exceptions import Error
from utilities.ifconfig.linux import Ifconfig

try:
    # noinspection PyCompatibility
    from unittest.mock import call
except ImportError:
    # noinspection PyUnresolvedReferences
    from mock import call

from utilities.drivers import driver_import, driver_class


@pytest.fixture(scope="function")
def ip_class(mocker, mock_sysname):
    mock_sysname("Linux")
    klass = driver_class(driver_import("resource", "ip", "Host"))
    log = mocker.Mock("log", info=mocker.Mock("info"), debug=mocker.Mock("debug"), error=mocker.Mock("error"))
    mocker.patch.object(klass, "vcall", mocker.Mock("vcall", return_value=(0, "", "")))
    mocker.patch.object(klass, "log", log)
    return klass


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestIpAddLink(object):
    @staticmethod
    def test_add_link_create_macvtap_link_when_required(ip_class):
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.add_link()
        assert ip.vcall.call_args_list == [
            call(["/bin_ip_cmd_test", "link", "add", "link", "br-prd",
                  "name", "svc1", "type", "macvtap", 'mode', 'bridge']),
        ]

    @staticmethod
    def test_add_link_raise_if_create_macvtap_link_has_errors(ip_class):
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.vcall.return_value = 1, "", ""
        with pytest.raises(Error):
            ip.add_link()

    @staticmethod
    def test_add_link_does_not_create_macvtap_link_when_already_has_macvtap_link(ip_class, mocker):
        mocker.patch.object(ip_class, "has_macvtap_link", return_value=True)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.add_link()
        assert ip.vcall.call_count == 0

    @staticmethod
    def test_add_link_does_nothing_when_ipdev_is_not_a_macvtap_device(ip_class):
        ip = ip_class(ipname="192.168.0.149", ipdev="br-prd", netmask="24")
        ip.add_link()
        assert ip.vcall.call_count == 0


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestIpDelLink(object):
    @staticmethod
    def test_del_link_delete_macvtap_link_when_required(mocker, ip_class):
        mocker.patch.object(ip_class, "has_macvtap_link", return_value=True)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.del_link()
        assert ip.vcall.call_args_list == [
            call(["/bin_ip_cmd_test", "link", "del", "link", "br-prd", "name", "svc1", "type", "macvtap"]),
        ]

    @staticmethod
    def test_del_link_raise_when_delete_macvtap_link_fail(mocker, ip_class):
        mocker.patch.object(ip_class, "has_macvtap_link", return_value=True)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.vcall.return_value = 1, "", ""
        with pytest.raises(Error):
            ip.del_link()

    @staticmethod
    def test_del_link_skip_delete_if_link_is_not_present(ip_class, mocker):
        mocker.patch.object(ip_class, "has_macvtap_link", return_value=False)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.del_link()
        assert ip.has_macvtap_link.call_count == 1
        assert ip.vcall.call_count == 0

    @staticmethod
    def test_del_link_does_nothing_when_ipdev_is_not_a_macvtap_device(mocker, ip_class):
        mocker.patch.object(ip_class, "has_macvtap_link")
        ip = ip_class(ipname="192.168.0.149", ipdev="br-prd", netmask="24")
        ip.del_link()
        assert ip.has_macvtap_link.call_count == 0
        assert ip.vcall.call_count == 0


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestIpHasMacvtapLink(object):
    @staticmethod
    def test_has_macvtap_link_return_true_when_link_exists(mocker, ip_class):
        out = ("41: svc1@br-prd: <BROADCAST,MULTICAST,UP,LOWER_UP> "
               "mtu 1500 qdisc fq_codel state UP mode DEFAULT group default qlen 500"
               "    link/ether 62:f0:73:58:6b:f7 brd ff:ff:ff:ff:ff:ff""")
        just_call = mocker.patch("drivers.resource.ip.host.linux.justcall", return_value=(out, "", 0))
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        assert ip.has_macvtap_link() is True
        assert just_call.call_args_list == [
            call(["/bin_ip_cmd_test", "link", "ls", "dev", "svc1"]),
        ], "use correct command to verify if link exists"

    @staticmethod
    def test_has_macvtap_link_return_false_when_link_does_not_exists(mocker, ip_class):
        mocker.patch("drivers.resource.ip.host.linux.justcall", return_value=("", "", 1))
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        assert ip.has_macvtap_link() is False


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestIpStartLink(object):
    @staticmethod
    def test_start_link_use_ip_command_to_make_link_up_when_ip_cmd_exists(mocker, ip_class):
        mocker.patch("drivers.resource.ip.host.linux.which", return_value=True)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.start_link()
        assert ip.vcall.call_args_list == [
            call(["/bin_ip_cmd_test", "link", "set", "dev", "svc1", "up"]),
        ], "use correct command ip command to make link up"

    @staticmethod
    def test_start_link_use_ifconfig_command_to_make_link_up_when_ip_cmd_does_not_exist(mocker, ip_class):
        mocker.patch("drivers.resource.ip.host.linux.which", return_value=False)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.start_link()
        assert ip.vcall.call_args_list == [
            call(["ifconfig", "svc1", "up"]),
        ], "use correct command ifconfig command to make link up"


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestIpStartIpCmd(object):
    @staticmethod
    def test_startip_cmd_use_ip_command_to_add_addr_on_macvtap_dev(mocker, ip_class):
        mocker.patch("drivers.resource.ip.host.linux.which", return_value=True)
        mocker.patch("utilities.ping.check_ping", return_value=True)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.addr = "192.168.0.149"
        ip.startip_cmd()
        assert ip.vcall.call_args_list == [
            call(["/bin_ip_cmd_test", "addr", "add", "192.168.0.149/24", "dev", "svc1"]),
        ], "use correct command ip command to add addr"

    @staticmethod
    def test_startip_raise_if_addr_does_not_ping_after_start(mocker, ip_class):
        mocker.patch("drivers.resource.ip.host.linux.which", return_value=True)
        mocker.patch("utilities.ping.check_ping", return_value=False)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.addr = "192.168.0.149"
        with pytest.raises(Error):
            ip.startip_cmd()

    @staticmethod
    def test_startip_cmd_use_ifconfig_command_to_add_addr_on_non_macvtap_dev(mocker, ip_class):
        mocker.patch("drivers.resource.ip.host.linux.which", return_value=True)
        mocker.patch("utilities.ping.check_ping", return_value=True)
        ip = ip_class(ipname="192.168.0.149", ipdev="eth0", netmask="24")
        ip.addr = "192.168.0.149"
        ip.stacked_dev = "eth0:1"

        ip.startip_cmd()
        assert ip.vcall.call_args_list == [
            call(["ifconfig", "eth0:1", "192.168.0.149", "netmask", "255.255.255.0", "up"])
        ], "use correct command ifconfig command to add addr"


IP_NO_MACVTAP = """41: br-prd: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default qlen 1000
    link/ether 08:00:27:28:dc:5f brd ff:ff:ff:ff:ff:ff
    inet 192.168.0.10/24 brd 10.24.0.255 scope global br-prd
       valid_lft forever preferred_lft forever
    inet6 fd00:0:24::11/64 scope global
       valid_lft forever preferred_lft forever
    inet6 fe80::a00:27ff:fe28:dc5f/64 scope link
       valid_lft forever preferred_lft forever"""

IP_MACVTAP_DOWN = """42: svc1@br-prd: <BROADCAST,MULTICAST> mtu 1500 qdisc noop state DOWN group default qlen 500
    link/ether 82:d4:de:23:06:6f brd ff:ff:ff:ff:ff:ff"""

IP_MACVTAP_UP = """42: svc1@br-prd: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc fq_codel state UP ..."""


@pytest.mark.ci
@pytest.mark.usefixtures("osvc_path_tests")
class TestIpStart(object):
    @staticmethod
    def test_start_calls_correct_command_when_macvtap_dev(mocker, ip_class):
        mocker.patch.object(
            ip_class,
            'get_ifconfig',
            mocker.Mock('get_ifconfig',
                        side_effect=[
                            Ifconfig(ip_out=IP_NO_MACVTAP),
                            Ifconfig(ip_out=IP_MACVTAP_DOWN),
                            Ifconfig(ip_out=IP_MACVTAP_UP),
                        ]))

        mocker.patch("drivers.resource.ip.host.linux.which", return_value=True)
        mocker.patch("utilities.ping.check_ping", return_value=True)
        ip = ip_class(ipname="192.168.0.149", ipdev="svc1@br-prd", netmask="24")
        ip.addr = "192.168.0.149"
        ip.svc = mocker.Mock("svc", abort_start_done=True)
        ip.allow_start()
        assert ip.vcall.call_args_list == [
            call(["/bin_ip_cmd_test", "link", "add", "link", "br-prd",
                  "name", "svc1", "type", "macvtap", 'mode', 'bridge']),
            call(['/bin_ip_cmd_test', 'link', 'set', 'dev', 'svc1', 'up']),
        ]
        ip.start_locked()
        assert ip.vcall.call_args_list == [
            call(["/bin_ip_cmd_test", "link", "add", "link", "br-prd",
                  "name", "svc1", "type", "macvtap", 'mode', 'bridge']),
            call(['/bin_ip_cmd_test', 'link', 'set', 'dev', 'svc1', 'up']),
            call(['/bin_ip_cmd_test', 'addr', 'add', '192.168.0.149/24', 'dev', 'svc1'])
        ]
