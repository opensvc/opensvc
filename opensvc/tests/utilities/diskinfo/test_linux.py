from utilities.diskinfo.linux import DiskInfo


class TestDiskInfo(object):
    @staticmethod
    def test_mpath_id_return_value_from_its_cache():
        disk_info = DiskInfo()
        disk_info.mpath_h = {"dev1": "wwn1"}
        assert disk_info.mpath_id("dev1") == "wwn1"
