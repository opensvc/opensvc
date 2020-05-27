import pytest

from utilities.drivers import driver_import, driver_class

IFCONFIG = """lo0: flags=2001000849<UP,LOOPBACK,RUNNING,MULTICAST,IPv4,VIRTUAL> mtu 8232 index 1
\tinet 127.0.0.1 netmask ff000000
net0: flags=100001004843<UP,BROADCAST,RUNNING,MULTICAST,DHCP,IPv4,PHYSRUNNING> mtu 1500 index 2
\tinet 10.0.2.10 netmask ffffff00 broadcast 10.0.2.255
\tether 2:8:20:b2:e8:3a
net1: flags=100001000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4,PHYSRUNNING> mtu 1500 index 2
\tinet 10.22.0.11 netmask ffff0000 broadcast 10.0.255.255
\tether 2:8:20:b2:e8:3b
net2: flags=100001000843<UP,BROADCAST,RUNNING,MULTICAST,IPv4,PHYSRUNNING> mtu 1500 index 2
\tinet 10.22.0.12 netmask ff000000 broadcast 10.0.2.255
\tether 2:8:20:b2:e8:3c
"""


@pytest.fixture(scope='function')
def ip_class(mocker, mock_sysname):
    mock_sysname('SunOS')
    klass = driver_class(driver_import('resource', 'ip', 'Host'))
    from utilities.ifconfig.sunos import Ifconfig
    ifconfig = Ifconfig(ifconfig=str(IFCONFIG))
    log = mocker.Mock('log', info=mocker.Mock('info'), debug=mocker.Mock('debug'))
    mocker.patch.object(klass, 'vcall', mocker.Mock('vcall'))
    mocker.patch.object(klass, 'log', log)
    mocker.patch.object(klass, 'get_ifconfig', mocker.Mock('get_ifconfig', return_value=ifconfig))
    return klass


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestIpStartCmd:
    @staticmethod
    @pytest.mark.parametrize('ipdev,expected_netmask',
                             [('net0', '255.255.255.0'),
                              ('net1', '255.255.0.0'),
                              ('net2', '255.0.0.0'),
                              ])
    def test_call_ifconfig_with_correct_netmask_value(ip_class, ipdev, expected_netmask):
        ip = ip_class(ipname='192.168.0.149', ipdev=ipdev)
        ip.addr = ip.ipname
        ip.get_stack_dev()
        ip.startip_cmd()

        expected_cmd = ["/usr/sbin/ifconfig", ipdev + ':1',
                        "plumb", '192.168.0.149',
                        "netmask", expected_netmask,
                        "broadcast", "+",
                        "up",
        ]
        ip.vcall.assert_called_once_with(expected_cmd)
