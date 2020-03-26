import pytest

from core.objects.svc import Svc


@pytest.fixture(scope='function', name='svc')
def factory_svc():
    return Svc(name='svc')


@pytest.mark.ci
@pytest.mark.usefixtures('has_service_lvm')
class TestSvcWithDiskLvm:
    @staticmethod
    def test_has_disk_vg_resource(svc):
        for name in ['simple', 'optional', 'scsireserv', 'scsireserv-optional']:
            rid = 'disk#' + name
            assert svc.get_resource(rid).type == "disk.vg"
            assert str(type(svc.get_resource(rid))) == "<class 'drivers.resource.disk.vg.linux.DiskVg'>"

    @staticmethod
    def test_disk_resource_is_optional_by_default(svc):
        for name in ['simple', 'scsireserv']:
            assert svc.get_resource('disk#' + name).is_optional() is False

    @staticmethod
    def test_has_no_disk_pr_resource(svc):
        for name in ['simple', 'optional']:
            assert svc.get_resource('disk#' + name + 'pr') is None

    @staticmethod
    def test_disk_resource_set_optional_from_config(svc):
        for name in ['optional', 'scsireserv-optional']:
            assert svc.get_resource('disk#' + name).is_optional() is True

    @staticmethod
    def test_automatic_define_disk_pr(svc):
        for name in ['scsireserv', 'scsireserv-optional']:
            assert svc.get_resource('disk#' + name + 'pr').type == 'disk.scsireserv'

    @staticmethod
    def test_disk_pr_is_non_optional_by_default(svc):
        assert svc.get_resource('disk#scsireservpr').is_optional() is False

    @staticmethod
    def test_disk_pr_set_optional_from_config(svc):
        assert svc.get_resource('disk#scsireserv-optionalpr').is_optional() is True


@pytest.mark.ci
@pytest.mark.usefixtures('has_service_with_fs_flag')
class TestSvcFsFlag:
    @staticmethod
    def test_has_fs_flag_resource(mock_sysname, svc):
        mock_sysname('Linux')
        flag_resource = svc.get_resource('fs#flag1')
        assert flag_resource.type == 'fs.flag'
