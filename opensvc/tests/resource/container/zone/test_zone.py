import os

import pytest

from drivers.resource.container.zone import ContainerZone


LIB_CLASS = 'drivers.resource.container.zone.ContainerZone'


@pytest.fixture(scope='function')
def zone_configure(mocker):
    return mocker.patch.object(ContainerZone, 'zone_configure')


@pytest.fixture(scope='function')
def zoneadm(mocker):
    return mocker.patch.object(ContainerZone, 'zoneadm')


@pytest.fixture(scope='function')
def create_sc_profile(mocker):
    return mocker.patch.object(ContainerZone, 'create_sc_profile')


@pytest.fixture(scope='function')
def create_sysidcfg(mocker):
    return mocker.patch.object(ContainerZone, 'create_sysidcfg')


@pytest.fixture(scope='function')
def create_container_origin(mocker):
    return mocker.patch.object(ContainerZone, 'create_container_origin')


@pytest.fixture(scope='function')
def create_snaped_zone(mocker):
    return mocker.patch.object(ContainerZone, 'create_snaped_zone')


@pytest.fixture(scope='function')
def install_zone(mocker):
    return mocker.patch.object(ContainerZone, 'install_zone')


@pytest.fixture(scope='function')
def create_cloned_zone(mocker, tmp_path):
    return mocker.patch.object(ContainerZone, 'create_cloned_zone')


@pytest.fixture(scope='function')
def zone_boot(mocker):
    return mocker.patch.object(ContainerZone, 'zone_boot')


@pytest.fixture(scope='function')
def klass_has_capability(mocker):
    def func(klass, capabilities):
        def has_capability(_, cap):
            return cap in capabilities

        mocker.patch.object(klass, 'has_capability', has_capability)
    return func


@pytest.fixture(scope='function')
def brand_native(klass_has_capability):
    klass_has_capability(ContainerZone, ['container.zone.brand-native'])


@pytest.fixture(scope='function')
def brand_solaris(klass_has_capability):
    klass_has_capability(ContainerZone, ['container.zone.brand-solaris'])


@pytest.fixture(scope='function')
def svc(mocker, tmp_dir):
    svc = mocker.Mock(name='svc')
    svc.namespace = ''
    svc.name = 's'
    svc.loggerpath = 'something'
    svc.var_d = tmp_dir
    return svc


@pytest.fixture(scope='function')
def zone(svc):
    zone = ContainerZone(rid='container#1', zonepath='/z/zonepath_kw')
    zone.svc = svc
    return zone


@pytest.fixture(scope='function')
def with_installed_zone(mocker):
    zone_data = dict(state='installed', zonepath='/z/something', brand='solaris')
    mocker.patch.object(ContainerZone, 'zone_data', zone_data)
    return zone_data


@pytest.fixture(scope='function')
def zone_configure(mocker):
    return mocker.patch.object(ContainerZone, 'zone_configure',
                               return_value=mocker.Mock('zone_configure'))


@pytest.mark.ci
@pytest.mark.usefixtures('with_installed_zone')
class TestContainerZoneProvisionWhenZoneIsInstalled:
    @staticmethod
    def test_returns_without_actions(zone_configure, zone):
        zone.provisioner()
        assert zone_configure.call_count == 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestContainerZonepath:
    @staticmethod
    def test_is_value_from_zoneadm(zone, with_installed_zone):
        with_installed_zone['zonepath'] = '/z/fromzoneadm'
        assert zone.zonepath == '/z/fromzoneadm'

    @staticmethod
    def test_fallback_to_zonecfg(mocker, zone):
        """Not sure this is possible issue"""
        mocker.patch.object(ContainerZone, 'get_zonepath_from_zonecfg_cmd',
                            return_value='/z/zonepath_from_zonecfg')
        assert zone.zonepath == '/z/zonepath_from_zonecfg'

    @staticmethod
    def test_fallback_to_zonecfg_exported(mocker, zone):
        mocker.patch.object(ContainerZone, 'get_zonepath_from_zonecfg_export',
                            return_value='/z/zonecfg_exported')
        assert zone.zonepath == '/z/zonecfg_exported'

    @staticmethod
    def test_fallback_to_resource_zonepath_kw(zone):
        assert zone.zonepath == '/z/zonepath_kw'

    @staticmethod
    def test_fallback_to_none(svc):
        zone = ContainerZone(rid='container#1')
        zone.svc = svc
        assert zone.zonepath is None


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestContainerProvision:
    @staticmethod
    def test_return_false_when_mixin_container_origin_and_snapof(
            zone_configure,
            create_snaped_zone,
            create_cloned_zone,
            svc):
        zone = ContainerZone(rid='container#1', snapof='something', container_origin='skel')
        zone.svc = svc
        provisioned = zone.provisioner()

        assert provisioned is False
        assert zone_configure.call_count == 0
        assert create_snaped_zone.call_count == 0
        assert create_cloned_zone.call_count == 0

    @staticmethod
    def test_return_true_when_already_provisioned(
            with_installed_zone,
            zone_configure,
            create_snaped_zone,
            create_cloned_zone,
            zone):
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is True
        assert zone_configure.call_count == 0
        assert create_snaped_zone.call_count == 0
        assert create_cloned_zone.call_count == 0

    @staticmethod
    def test_install_from_scratch(
            install_zone,
            zone):
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is True
        assert install_zone.call_count == 1

    @staticmethod
    @pytest.mark.usefixtures('brand_native')
    def test_create_snaped_zone_if_snapof_when_brand_native(
            create_snaped_zone,
            install_zone,
            svc):
        zone = ContainerZone(rid='container#1', snapof='something')
        zone.svc = svc
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is True
        assert create_snaped_zone.call_count == 1

    @staticmethod
    @pytest.mark.usefixtures('brand_solaris')
    def test_refuse_snapof_when_no_brand_native_capability(
            zone_configure,
            create_cloned_zone,
            svc):
        zone = ContainerZone(rid='container#1', snapof='something')
        zone.svc = svc
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is False
        assert zone_configure.call_count == 0
        assert create_cloned_zone.call_count == 0

    @staticmethod
    def test_create_origin__then_create_clone_when_container_origin(
            create_container_origin,
            create_cloned_zone,
            svc):
        zone = ContainerZone(rid='container#1', container_origin='skelzone')
        zone.svc = svc
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is True
        assert create_container_origin.call_count == 1
        assert create_cloned_zone.call_count == 1


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('brand_native')
class TestContainerInstallZoneOnBrandNative:
    @staticmethod
    def test_configure_then_install_zone_with_sysidcfg(zoneadm, zone_configure, create_sysidcfg, zone):
        zone.install_zone()
        assert zone_configure.call_count == 1
        assert create_sysidcfg.call_count == 1
        zoneadm.assert_called_once_with('install', [])


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('brand_solaris')
class TestContainerInstallZoneOnBrandSolaris:
    @staticmethod
    def test_configure_then_install_zone_with_existing_sc_profile(zoneadm, zone_configure, file1, zone):
        zone.sc_profile = file1
        zone.install_zone()
        assert zone_configure.call_count == 1
        zoneadm.assert_called_once_with('install', ['-c', file1])

    @staticmethod
    def test_configure_then_install_with_automatically_created_sc_profile(mocker, zoneadm, zone_configure, zone):
        mocker.patch.object(ContainerZone, 'get_encaps_ip_rids', mocker.Mock(return_value=[]))
        zone.install_zone()
        assert zone_configure.call_count == 1
        zoneadm.assert_called_once_with('install', ['-c', zone.sc_profile])
        assert os.path.exists(zone.sc_profile)
        with open(zone.sc_profile, 'r') as f:
            xml_content = f.read()
        assert 'xml version' in xml_content

    @staticmethod
    def test_configure_then_install_using_ai_manifest(zoneadm, zone_configure, file1, file2, zone):
        zone.sc_profile = file1
        zone.ai_manifest = file2
        zone.install_zone()
        assert zone_configure.call_count == 1
        zoneadm.assert_called_once_with('install', ['-c', file1, '-m', file2])
