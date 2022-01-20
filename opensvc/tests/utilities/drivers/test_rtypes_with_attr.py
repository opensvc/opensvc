import pytest

from utilities.drivers import driver_import, rtypes_with_callable


@pytest.fixture(scope="function")
def drivers_cache(mocker):
    """Prevent from side effects of other driver loads during other tests"""
    mocker.patch("utilities.drivers._DRIVERS", set())


@pytest.mark.ci
@pytest.mark.usefixtures('drivers_cache')
class TestRtypesWithAttr:
    @staticmethod
    def test_when_func_is_not_a_callable():
        driver_import("resource", "sync", "netapp")
        assert rtypes_with_callable("DRIVER_BASENAME") == []
        assert rtypes_with_callable("DRIVER_DRIVER_GROUP") == []

    @staticmethod
    def test_when_func_does_not_exists():
        driver_import("resource", "sync", "netapp")
        assert rtypes_with_callable("foo_bar_func") == []

    @staticmethod
    @pytest.mark.parametrize("group, basename", [
        ("sync", "symsrdfs")
    ])
    def test_has_sync_establish(group, basename):
        driver_import("resource", group, basename)
        assert "%s.%s" % (group, basename) in rtypes_with_callable("sync_establish")


    @staticmethod
    @pytest.mark.parametrize("group, basename", [
        ("sync", "netapp"),
        ("sync", "hp3par"),
    ])
    def test_has_sync_resume(group, basename):
        driver_import("resource", group, basename)
        assert "%s.%s" % (group, basename) in rtypes_with_callable("sync_resume")

    @staticmethod
    def test_has_sync_resume_on_multiple_driver_imports():
        driver_import("resource", "sync", "netapp")
        driver_import("resource", "sync", "hp3par")
        assert "sync.netapp" in rtypes_with_callable("sync_resume")
        assert "sync.hp3par" in rtypes_with_callable("sync_resume")
