import pytest

from utilities.drivers import driver_import, rtypes_with_callable


ALL_DRIVERS = [
    ("disk", "md"),
    ("sync", "btrfs"),
    ("sync", "btrfssnap"),
    ("sync", "dds"),
    ("sync", "docker"),
    ("sync", "evasnap"),
    ("sync", "hp3par"),
    ("sync", "hp3parsnap"),
    ("sync", "ibmdssnap"),
    ("sync", "necismsnap"),
    ("sync", "netapp"),
    ("sync", "nexenta"),
    ("sync", "radosclone"),
    ("sync", "radossnap"),
    ("sync", "rsync"),
    ("sync", "s3"),
    ("sync", "symclone"),
    ("sync", "symsnap"),
    ("sync", "symsrdfs"),
    ("sync", "zfs"),
    ("sync", "zfssnap"),
]


@pytest.fixture(scope="function")
def drivers_cache(mocker):
    """Prevent from side effects of other driver loads during other tests"""
    mocker.patch("utilities.drivers._DRIVERS", set())


@pytest.mark.ci
@pytest.mark.usefixtures('drivers_cache')
class TestRTypesWithAttr:
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
    def test_has_sync_resume_on_multiple_drivers():
        for group, basename in ALL_DRIVERS:
            driver_import("resource", group, basename)
        assert len(rtypes_with_callable("sync_resume")) > 1
        assert "sync.netapp" in rtypes_with_callable("sync_resume")
        assert "sync.hp3par" in rtypes_with_callable("sync_resume")

    @staticmethod
    @pytest.mark.parametrize("func, expected_group_basenames", [
        ("sync_all", [
            ("sync", "btrfs"),
            ("sync", "dds"),
            ("sync", "docker"),
            ("sync", "rsync"),
        ]),

        ("sync_break", [
            ("sync", "hp3par"),
            ("sync", "ibmdssnap"),
            ("sync", "netapp"),
            ("sync", "nexenta"),
            ("sync", "symclone"),
            ("sync", "symsnap"),
            ("sync", "symsrdfs"),
        ]),

        ("sync_drp", [
            ("sync", "btrfs"),
            ("sync", "dds"),
            ("sync", "docker"),
            ("sync", "rsync"),
            ("sync", "zfs"),
        ]),

        ("sync_establish", [
            ("sync", "symsrdfs")]),

        ("sync_full", [
            ("sync", "btrfs"),
            ("sync", "dds"),
            ("sync", "s3"),
            ("sync", "zfs"),
        ]),

        ("sync_nodes", [
            ("sync", "btrfs"),
            ("sync", "dds"),
            ("sync", "docker"),
            ("sync", "rsync"),
            ("sync", "zfs"),
        ]),

        ("sync_quiesce", [
            ("sync", "hp3par"),
            ("sync", "netapp"),
            ("sync", "symsrdfs"),
        ]),

        ("sync_restore", [
            ("sync", "s3"),
            ("sync", "symclone"),
            ("sync", "symsnap"),
        ]),

        ("sync_resume", [
            ("sync", "hp3par"),
            ("sync", "netapp"),
        ]),

        ("sync_resync", [
            ("disk", "md"),
            ("sync", "evasnap"),
            ("sync", "hp3par"),
            ("sync", "ibmdssnap"),
            ("sync", "necismsnap"),
            ("sync", "netapp"),
            ("sync", "nexenta"),
            ("sync", "radosclone"),
            ("sync", "radossnap"),
            ("sync", "symclone"),
            ("sync", "symsnap"),
            ("sync", "symsrdfs"),
        ]),

        ("sync_revert", [
            ("sync", "hp3par"),
        ]),

        ("sync_split", [
            ("sync", "symsrdfs"),
        ]),

        ("sync_swap", [
            ("sync", "hp3par"),
            ("sync", "netapp"),
            ("sync", "nexenta"),
            ("sync", "symsrdfs"),
        ]),

        ("sync_update", [
            ("sync", "btrfssnap"),
            ("sync", "dds"),
            ("sync", "evasnap"),
            ("sync", "hp3par"),
            ("sync", "hp3parsnap"),
            ("sync", "ibmdssnap"),
            ("sync", "necismsnap"),
            ("sync", "netapp"),
            ("sync", "nexenta"),
            ("sync", "radossnap"),
            ("sync", "radosclone"),
            ("sync", "s3"),
            ("sync", "symclone"),
            ("sync", "symsnap"),
            ("sync", "zfs"),
            ("sync", "zfssnap"),
        ]),

        ("sync_verify", [
            ("sync", "dds"),
        ]),
    ])
    def test_driver_implement(func, expected_group_basenames):
        for group, basename in ALL_DRIVERS:
            driver_import("resource", group, basename)

        for group, basename in expected_group_basenames:
            assert "%s.%s" % (group, basename) in rtypes_with_callable(func)
