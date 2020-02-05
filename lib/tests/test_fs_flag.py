from svc import Svc
from resFsFlagLinux import Fs as FsFlagLinux
from resFsFlagSunOS import Fs as FsFlagSunOS

import pytest


@pytest.mark.ci
class TestFsFlagFileName:
    @staticmethod
    @pytest.mark.parametrize('fs_flag_class,namespace,expected_flag_file', [
        (FsFlagLinux, 'ns1', '/dev/shm/opensvc/ns1/svc/svcname/fs#flag1.flag'),
        (FsFlagLinux, None, '/dev/shm/opensvc/svc/svcname/fs#flag1.flag'),
        (FsFlagSunOS, 'ns2', '/var/run/opensvc/ns2/svc/svcname/fs#flag1.flag'),
        (FsFlagSunOS, None, '/var/run/opensvc/svc/svcname/fs#flag1.flag'),
    ])
    def test_is_correctly_defined(fs_flag_class, namespace, expected_flag_file):
        svc = Svc('svcname', namespace=namespace, volatile=True)
        fs_flag = fs_flag_class(rid='fs#flag1')
        svc += fs_flag
        assert fs_flag.type == 'fs.flag'
        assert fs_flag.flag_f == expected_flag_file
