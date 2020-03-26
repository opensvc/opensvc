import pytest

from core.objects.svc import Svc
from drivers.resource.fs.flag.linux import FsFlag as FsFlagLinux
from drivers.resource.fs.flag.sunos import FsFlag as FsFlagSunOS


@pytest.mark.ci
class TestFsFlagFileName:
    @staticmethod
    @pytest.mark.parametrize('fs_flag_class,namespace,expected_flag_files', [
        (FsFlagLinux, 'ns1', ['/dev/shm/opensvc/ns1/svc/svcname/fs#flag1.flag']),
        (FsFlagLinux, None, ['/dev/shm/opensvc/svc/svcname/fs#flag1.flag']),
        (FsFlagSunOS, 'ns2', ['/var/run/opensvc/ns2/svc/svcname/fs#flag1.flag',
                              '/system/volatile/opensvc/ns2/svc/svcname/fs#flag1.flag']),
        (FsFlagSunOS, None, ['/var/run/opensvc/svc/svcname/fs#flag1.flag',
                             '/system/volatile/opensvc/svc/svcname/fs#flag1.flag']),
    ])
    def test_is_correctly_defined(fs_flag_class, namespace, expected_flag_files):
        svc = Svc('svcname', namespace=namespace, volatile=True)
        fs_flag = fs_flag_class(rid='fs#flag1')
        svc += fs_flag
        assert fs_flag.type == 'fs.flag'
        assert fs_flag.flag_f in expected_flag_files
