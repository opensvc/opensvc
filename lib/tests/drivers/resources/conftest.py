import pytest


@pytest.fixture(scope='function')
def create_driver_resource(mock_sysname):
    def create(sysname, scenario):
        driver_name, class_name, kwargs, expected_type = scenario
        mock_sysname(sysname)
        from rcUtilities import driver_import
        driver = driver_import('resource', driver_name)
        return getattr(driver, class_name)(**kwargs)

    return create
