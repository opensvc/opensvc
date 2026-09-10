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


@pytest.mark.ci
class TestResourceHandlingDir:
    @staticmethod
    def test_resource_handling_dir(svc, mocker):
        class MockerRes:
            def __init__(self, v):
                self.mount_point = v

        mocker.patch.object(Svc, 'get_resources', return_value=[MockerRes("/a/b"), MockerRes("/a/b/c")])
        assert svc.resource_handling_dir("") is None
        assert svc.resource_handling_dir("/") is None
        assert svc.resource_handling_dir("/a") is None
        assert svc.resource_handling_dir("/a/b").mount_point is "/a/b"
        assert svc.resource_handling_dir("/a/b/c").mount_point is "/a/b/c"
        assert svc.resource_handling_dir("/a/b/c/d").mount_point is "/a/b/c"


@pytest.mark.ci
class TestSvcSurvivesPickling:
    """
    The parallel actions hand the service to a worker process. Python 3.14
    made "forkserver" the default way of starting one on linux, which pickles
    what it is handed instead of letting the child inherit it, so the service
    now travels through a pickle.
    """

    @staticmethod
    def test_the_keyword_store_is_still_the_one_of_the_process(svc):
        """
        kwstore has to be the KeywordStore of the process reading it.

        It used to be a lazy, so it was cached on the service and pickled with
        it, and the child unpickled a copy of the store its parent had. The
        keywords a driver registers when it is imported went to the store of
        the child, which nothing read, and every parallel action failed with
        "sync#i0.options not found in the keywords dictionary".
        """
        import pickle

        from core.objects.svcdict import KEYS

        assert svc.kwstore is KEYS
        assert pickle.loads(pickle.dumps(svc)).kwstore is KEYS

    @staticmethod
    def test_a_keyword_registered_after_the_pickle_is_seen(svc):
        """
        A driver imported after the service was pickled registers its
        keywords in the store the unpickled service reads.
        """
        import pickle

        clone = pickle.loads(pickle.dumps(svc))
        svc.load_driver("sync", "rsync")
        assert clone.kwstore["sync"].getkey("options", "rsync") is not None
