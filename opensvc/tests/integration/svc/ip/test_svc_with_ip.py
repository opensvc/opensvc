import os

import pytest

from core.objects.svc import Svc

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
    svc.dump_config_data(cd=svc.cd)
    return svc


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
class TestSvcWithIp:
    @staticmethod
    def test_print_config_data_is_valid(svc):
        config = svc.print_config_data()
        assert config['ip#0'] == IP_0['ip#0']

    @staticmethod
    def test_has_created_a_service_config_file(osvc_path_tests, svc):
        svc_file_name = os.path.join(str(osvc_path_tests), 'etc', 'foo.conf')
        assert os.path.exists(svc_file_name)
        with open(svc_file_name) as file:
            text_config = file.read()
        expected_lines = [
            "[ip#0]",
            "ipname = 192.168.0.149",
            "ipdev = eth0",
            "netmask = 24",
            "[DEFAULT]"
        ]
        for line in expected_lines:
            assert line in text_config

    @staticmethod
    def test_print_config_does_not_raise(svc):
        svc.print_config()
