import pytest

from rcUtilities import driver_import


OS_LIST = {'Linux', 'SunOS'}


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('driver_name, class_name, kwargs, expected_type', [
    ('fs.flag', 'FsFlag', {}, 'fs.flag'),
])
def test_create_fs_flag_with_correct_type(mock_sysname, sysname, driver_name, class_name, kwargs, expected_type):
    mock_sysname(sysname)
    driver = driver_import('resource', driver_name)
    klass = getattr(driver, class_name)
    resource = klass(**kwargs)
    assert resource.type == expected_type
