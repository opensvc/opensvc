import pytest

from rcUtilities import driver_import


OS_LIST = {'Linux', 'SunOS', 'Darwin', 'FreeBSD', 'HP-UX', 'OSF1'}


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('driver_name, class_name, kwargs, expected_type', [
    ('disk.hpvm', 'DiskHpvm', {'name': 'vg1'}, 'disk.vg'),
    ('disk.zvol', 'DiskZvol', {'name': 'vg1'}, 'disk.zvol'),
    ('disk.zpool', 'ZpoolDisk', {'name': 'vg1'}, 'disk.zpool'),
])
def test_create_disk_with_correct_type(mock_sysname, sysname, driver_name, class_name, kwargs, expected_type):
    mock_sysname(sysname)
    driver = driver_import('resource', driver_name)
    klass = getattr(driver, class_name)
    resource = klass(**kwargs)
    assert resource.type == expected_type


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', ['AIX', 'Linux', 'HP-UX'])
@pytest.mark.parametrize('driver_name, class_name, kwargs, expected_type', [
    ('disk.vg', 'DiskVg', {'name': 'vg1'}, 'disk.vg'),
])
def test_disk_vg_with_correct_type(mock_sysname, sysname, driver_name, class_name, kwargs, expected_type):
    mock_sysname(sysname)
    driver = driver_import('resource', driver_name)
    klass = getattr(driver, class_name)
    resource = klass(**kwargs)
    assert resource.type == expected_type