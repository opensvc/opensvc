import os

import pytest

from utilities.drivers import driver_import, driver_class


@pytest.fixture(scope='function')
def python_site_opensvc(monkeypatch, mock_sysname):
    site_path = os.path.join(os.path.dirname(__file__), 'fixture-site')
    monkeypatch.syspath_prepend(site_path)
    mock_sysname('Linux')


class TestDriver:
    @staticmethod
    @pytest.mark.parametrize(
        'args, expected_type',
        [
            # search 1: opensvc drivers
            [('fs', 'flag'), 'fs.flag'],
            [('fs', ), 'fs'],

            # search 2: site drivers
            [('fs', 'sitea'), 'fs.sitea_linux'],  # pickup os dedicated lib
            [('fs', 'siteb'), 'fs.siteb_non_os'],  # pickup base lib if no os lib
            [('sitegrp', ''), 'sitegrp_base'],
            [('sitegrp',), 'sitegrp_base'],

            # search 3: fallback for opensvc driver
            [('fs', 'notfound'), 'fs'],

            # search 4: fallback for site driver
            [('sitegrp', 'notfound'), 'sitegrp_base'],
        ]
    )
    def test_import_correct_driver(
            python_site_opensvc,
            args,
            expected_type
    ):
        mod = driver_import('resource', *args)
        custom_resource = driver_class(mod)(rid="#12")
        assert custom_resource.type == expected_type
        assert custom_resource.rid == '#12'

    def test_raise_import_error_when_driver_not_found(
            python_site_opensvc,
    ):
        with pytest.raises(ImportError):
            driver_import('resource', 'nogrp', 'flag')
