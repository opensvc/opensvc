import os

import pytest

import core.exceptions as ex
from core.objects.svc import Svc
from drivers.resource.container.zone import ContainerZone

LIB_CLASS = 'drivers.resource.container.zone.ContainerZone'


@pytest.fixture(scope='function')
def zone_configure(mocker):
    return mocker.patch.object(ContainerZone, 'zone_configure')


@pytest.fixture(scope='function')
def provision_zone(mocker):
    return mocker.patch.object(ContainerZone, 'provision_zone')


@pytest.fixture(scope='function')
def zone_unconfigure(mocker):
    return mocker.patch.object(ContainerZone, 'zone_unconfigure')


@pytest.fixture(scope='function')
def zoneadm(mocker):
    return mocker.patch.object(ContainerZone, 'zoneadm')


@pytest.fixture(scope='function')
def zonecfg(mocker):
    return mocker.patch.object(ContainerZone, 'zonecfg')


@pytest.fixture(scope='function')
def create_sc_profile(mocker):
    return mocker.patch.object(ContainerZone, 'create_sc_profile')


@pytest.fixture(scope='function')
def create_sysidcfg(mocker):
    return mocker.patch.object(ContainerZone, 'create_sysidcfg')


@pytest.fixture(scope='function')
def update_ip_tags(mocker):
    return mocker.patch.object(ContainerZone, 'update_ip_tags')


@pytest.fixture(scope='function')
def create_container_origin(mocker):
    return mocker.patch.object(ContainerZone, 'create_container_origin')


@pytest.fixture(scope='function')
def prepare_boot_config(mocker):
    return mocker.patch.object(ContainerZone, 'prepare_boot_config')


@pytest.fixture(scope='function')
def install_boot_config(mocker):
    return mocker.patch.object(ContainerZone, 'install_boot_config')


@pytest.fixture(scope='function')
def make_zone_installed(mocker):
    return mocker.patch.object(ContainerZone, 'make_zone_installed')


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
    zone = ContainerZone(rid='container#1', name='zonex', zonepath='/z/zonepath_kw')
    zone.svc = svc
    return zone


@pytest.fixture(scope='function')
def with_installed_zone(mocker):
    zone_data = dict(state='installed', zonepath='/z/something', brand='solaris')
    mocker.patch.object(ContainerZone, 'zone_data', zone_data)
    return zone_data


@pytest.fixture(scope='function')
def set_zone_data(mocker):
    def func(state='configured', zonepath='/zones/zonename', brand='solaris'):
        return mocker.patch.object(ContainerZone, 'zone_data',
                                   dict(state=state, zonepath=zonepath, brand=brand))

    return func


@pytest.fixture(scope='function')
def zone_configure(mocker):
    return mocker.patch.object(ContainerZone, 'zone_configure',
                               return_value=mocker.Mock('zone_configure'))


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestZonepath:
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
class TestProvisioner:
    @staticmethod
    def test_return_false_when_mixing_container_origin_and_snapof(
            provision_zone,
            svc):
        zone = ContainerZone(rid='container#1', snapof='something', container_origin='skel')
        zone.svc = svc
        provisioned = zone.provisioner()

        assert provisioned is False
        assert provision_zone.call_count == 0

    @staticmethod
    @pytest.mark.usefixtures('with_installed_zone')
    def test_return_true_when_already_provisioned(
            provision_zone,
            zone):
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is True
        assert provision_zone.call_count == 0

    @staticmethod
    @pytest.mark.usefixtures('with_installed_zone')
    def test_leave_can_rollback_to_false_when_already_provisioned(
            zone):
        zone.provisioner(need_boot=False)
        assert zone.can_rollback is False

    @staticmethod
    def test_provision_zone_when_not_yet_provisioned(
            provision_zone,
            zone):
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is True
        assert provision_zone.call_count == 1

    @staticmethod
    @pytest.mark.usefixtures('provision_zone')
    def test_set_can_roolback_after_provision_zone(zone):
        zone.provisioner(need_boot=False)
        assert zone.can_rollback is True

    @staticmethod
    @pytest.mark.usefixtures('brand_solaris')
    def test_refuse_snapof_when_no_brand_native_capability(
            provision_zone,
            svc):
        zone = ContainerZone(rid='container#1', snapof='something')
        zone.svc = svc
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is False
        assert provision_zone.call_count == 0

    @staticmethod
    def test_create_origin_then_provision_zone_when_container_origin(
            create_container_origin,
            provision_zone,
            svc):
        zone = ContainerZone(rid='container#1', container_origin='skelzone')
        zone.svc = svc
        provisioned = zone.provisioner(need_boot=False)

        assert provisioned is True
        assert create_container_origin.call_count == 1
        assert provision_zone.call_count == 1


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestUnprovisioner:
    @staticmethod
    def test_noop_when_no_zone(
            zone_unconfigure,
            mocker,
            zone):
        mocker.patch.object(ContainerZone, 'state', None)
        zone.unprovisioner()
        assert zone_unconfigure.call_count == 0

    @staticmethod
    def test_unconfigure_zone_when_zone_is_configured(
            zone_unconfigure,
            mocker,
            zone):
        mocker.patch.object(ContainerZone, 'state', 'configured')

        zone.unprovisioner()

        assert zone_unconfigure.call_count == 1

    @staticmethod
    @pytest.mark.parametrize('state', ('installed', 'running'))
    def test_raise_when_zone_is_not_in_valid_state(
            zone_unconfigure,
            mocker,
            zone,
            state):
        mocker.patch.object(ContainerZone, 'state', state)
        with pytest.raises(ex.Error):
            zone.unprovisioner()

        assert zone_unconfigure.call_count == 0


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestProvisionZone:
    @staticmethod
    @pytest.mark.parametrize('state', ['installed', 'running'])
    def test_does_nothing_when_already_provisioned(set_zone_data, state, zone):
        set_zone_data(state=state)
        zone.provision_zone()

    @staticmethod
    def test_it_configure_prepare_boot_config_make_installed_and_install_boot_config(
            zone_configure,
            prepare_boot_config,
            make_zone_installed,
            install_boot_config,
            zone):
        zone.provision_zone()
        assert zone_configure.call_count == 1
        assert prepare_boot_config.call_count == 1
        assert make_zone_installed.call_count == 1
        assert install_boot_config.call_count == 1


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestConfigure:
    @staticmethod
    @pytest.mark.parametrize(
        'brand, zonepath, expected_zonecfg_cmd',
        [(None, None, 'create'),
         (None, '/z/z1', 'create; set zonepath=/z/z1'),
         ('solaris', None, 'create'),
         ('solaris', '/z/z1', 'create; set zonepath=/z/z1'),
         ('solaris10', None, 'create -t SYSsolaris10'),
         ('solaris10', '/zo/z1', 'create -t SYSsolaris10; set zonepath=/zo/z1'),
         ('native', None, 'create'),
         ('native', '/z/z1', 'create; set zonepath=/z/z1')])
    def test_create_correct_zone(
            svc,
            klass_has_capability,
            set_zone_data,
            zonecfg,
            brand,
            zonepath,
            expected_zonecfg_cmd):
        # noinspection PyUnusedLocal
        def zonecfg_side_effect(*args, **kwargs):
            set_zone_data(brand=brand, zonepath=zonepath)

        if brand == 'native':
            klass_has_capability(ContainerZone, ['container.zone.brand-native'])
        else:
            klass_has_capability(ContainerZone, ['container.zone.brand-solaris',
                                                 'container.zone.brand-solaris10'])
        zone = ContainerZone(rid='container#1', name='z1', brand=brand, zonepath=zonepath)
        zone.svc = Svc(name='svc1', volatile=True)
        zonecfg.side_effect = zonecfg_side_effect
        zone.zone_configure()
        zonecfg.assert_called_once_with([expected_zonecfg_cmd])


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestPrepareBootConfig:
    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris'])
    def test_creates_sc_profile(set_zone_data, brand, mocker, zone):
        mocker.patch.object(ContainerZone, 'get_encap_ip_rids', mocker.Mock(return_value=[]))
        set_zone_data(brand=brand)
        zone.prepare_boot_config()
        assert os.path.exists(zone.sc_profile)
        with open(zone.sc_profile, 'r') as f:
            xml_content = f.read()
            assert 'xml version' in xml_content

    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris'])
    def test_set_boot_config_file_with_created_sc_profile(set_zone_data, brand, mocker, zone):
        mocker.patch.object(ContainerZone, 'get_encap_ip_rids', mocker.Mock(return_value=[]))
        set_zone_data(brand=brand)
        zone.prepare_boot_config()
        assert os.path.exists(zone.boot_config_file)
        assert zone.boot_config_file == zone.sc_profile

    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris10', 'native'])
    def test_creates_sysidcfg(mocker, set_zone_data, brand, zone):
        mocker.patch.object(ContainerZone, 'get_encap_ip_rids', mocker.Mock(return_value=[]))
        set_zone_data(brand=brand)
        zone.prepare_boot_config()
        assert os.path.exists(zone.sysidcfg)
        with open(zone.sysidcfg, 'r') as f:
            sysidcfg_content = f.read()
            assert 'system_locale=C' in sysidcfg_content

    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris10', 'native'])
    def test_creates_sysidcfg_with_no_config_when_no_ip(mocker, set_zone_data, brand, zone):
        mocker.patch.object(ContainerZone, 'get_encap_ip_rids', mocker.Mock(return_value=[]))
        set_zone_data(brand=brand)
        zone.prepare_boot_config()
        assert os.path.exists(zone.sysidcfg)
        with open(zone.sysidcfg, 'r') as f:
            sysidcfg_content = f.read()
            assert 'network_interface=NONE {hostname=zonex}' in sysidcfg_content
            assert 'name_service=NONE' in sysidcfg_content

    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris10'])
    def test_set_boot_config_file_with_created_sysidcfg(mocker, set_zone_data, brand, zone):
        mocker.patch.object(ContainerZone, 'get_encap_ip_rids', mocker.Mock(return_value=[]))
        set_zone_data(brand=brand)
        zone.prepare_boot_config()
        assert os.path.exists(zone.boot_config_file)
        assert zone.boot_config_file == zone.sysidcfg

    @staticmethod
    @pytest.mark.parametrize('brand', ['native'])
    def test_has_no_boot_config_file(mocker, set_zone_data, brand, zone):
        mocker.patch.object(ContainerZone, 'get_encap_ip_rids', mocker.Mock(return_value=[]))
        set_zone_data(brand=brand)
        zone.prepare_boot_config()
        assert zone.boot_config_file is None


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestMakeInstalled:
    @staticmethod
    def test_call_install_zone_when_no_snapof_or_container_origin(install_zone, zone):
        zone.make_zone_installed()
        assert install_zone.call_count == 1

    @staticmethod
    def test_call_create_cloned_zone_wehn_container_origin(create_cloned_zone, zone):
        zone.container_origin = 'skelzone'
        zone.make_zone_installed()
        assert create_cloned_zone.call_count == 1

    @staticmethod
    def test_call_create_snaped_zone_when_container_origin(create_snaped_zone, zone):
        zone.snapof = '/zones/skelzone'
        zone.make_zone_installed()
        assert create_snaped_zone.call_count == 1


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestInstallZone:
    @staticmethod
    @pytest.mark.parametrize('install_archive', [None, 'archive'])
    @pytest.mark.parametrize('brand', ['native', 'solaris', 'solaris10'])
    def test_install_zone_with_boot_config_file_and_archive_if_archive_if_present(brand, install_archive, set_zone_data, zoneadm, file1, zone):
        set_zone_data(brand=brand)
        zone.boot_config_file = file1
        zone.install_archive = install_archive
        zone.install_zone()
        expected_install_args = ['-c', file1]
        if install_archive:
            expected_install_args += [ '-a', install_archive, '-u']
        zoneadm.assert_called_once_with('install', expected_install_args)

    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris'])
    def test_install_zone_with_ai_manifest(brand, set_zone_data, zoneadm, file1, zone):
        set_zone_data(brand=brand)
        zone.ai_manifest = file1
        zone.install_zone()
        zoneadm.assert_called_once_with('install', ['-m', file1])

    @staticmethod
    @pytest.mark.parametrize('brand', ['native'])
    def test_install_simple(brand, set_zone_data, zoneadm, zone):
        set_zone_data(brand=brand)
        zone.install_zone()
        zoneadm.assert_called_once_with('install', [])

    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris'])
    def test_install_zone_boot_config_and_ai_manifest(brand, set_zone_data, zoneadm, file1, file2, zone):
        set_zone_data(brand=brand)
        zone.boot_config_file = file1
        zone.ai_manifest = file2
        zone.install_zone()
        zoneadm.assert_called_once_with('install', ['-c', file1, '-m', file2])


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestCloneZone:
    @staticmethod
    @pytest.mark.usefixtures('update_ip_tags')
    def test_clone_use_boot_config_file_if_present(set_zone_data, zoneadm, zone):
        # noinspection PyUnusedLocal
        def zoneadm_side_effect(*args, **kwargs):
            set_zone_data(state='installed')

        set_zone_data(state='configured')
        zoneadm.side_effect = zoneadm_side_effect
        zone.boot_config_file = '/a_file'
        zone.container_origin = 'skelzone'
        zone.create_cloned_zone()
        zoneadm.assert_called_once_with('clone', ['-c', zone.boot_config_file, 'skelzone'])

    @staticmethod
    @pytest.mark.usefixtures('update_ip_tags')
    def test_clone_simple_clone_file_if_present(set_zone_data, zoneadm, zone):
        # noinspection PyUnusedLocal
        def zoneadm_side_effect(*args, **kwargs):
            set_zone_data(state='installed')

        set_zone_data(state='configured')
        zoneadm.side_effect = zoneadm_side_effect
        zone.container_origin = 'skelzone'
        zone.create_cloned_zone()
        zoneadm.assert_called_once_with('clone', ['skelzone'])

    @staticmethod
    @pytest.mark.usefixtures('update_ip_tags')
    def test_raise_if_final_zone_state_is_not_installed(set_zone_data, zoneadm, zone):
        set_zone_data(state='configured')
        zone.container_origin = 'skelzone'
        with pytest.raises(ex.Error, match='zone %s is not installed' % zone.name):
            zone.create_cloned_zone()


@pytest.mark.ci
class TestCreateContainerOrigin:
    @staticmethod
    @pytest.mark.usefixtures('brand_solaris')
    def test_noop_when_origin_is_already_installed(set_zone_data, zoneadm, zone):
        set_zone_data(state='installed')
        zone.container_origin = 'skelzone'
        zone.create_container_origin()
        assert zoneadm.call_count == 0


@pytest.mark.ci
class TestOriginFactory:
    @staticmethod
    @pytest.mark.parametrize('brand', ['native', 'solaris', 'solaris10'])
    def test_propagate_brand_and_install_archive_to_container_origin_object(set_zone_data, zoneadm, zone, brand):
        zone.container_origin = 'skelzone'
        zone.kw_brand = brand
        zone.install_archive = 'archive'
        container_origin = zone.origin_factory()
        assert container_origin.install_archive == 'archive'
        assert container_origin.kw_brand == zone.kw_brand

    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris', 'solaris10'])
    def test_define_zone_with_no_anet(set_zone_data, zoneadm, zone, brand):
        zone.container_origin = 'skelzone'
        zone.kw_brand = brand
        zone.install_archive = 'archive'
        container_origin = zone.origin_factory()
        assert container_origin.provision_net_type == 'no-anet'

    @staticmethod
    @pytest.mark.parametrize('brand', ['native'])
    def test_define_zone_with_no_net(set_zone_data, zoneadm, zone, brand):
        zone.container_origin = 'skelzone'
        zone.kw_brand = brand
        zone.install_archive = 'archive'
        container_origin = zone.origin_factory()
        assert container_origin.provision_net_type == 'no-net'


@pytest.mark.ci
class TestConfigureNet:
    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris', 'solaris10'])
    def test_remove_nets_when_no_net(zonecfg, zone, brand):
        zone.kw_brand = brand
        zone.provision_net_type = 'no-net'
        zone.zone_configure_net()
        zonecfg.assert_called_once_with(['remove -F net'])

    @staticmethod
    @pytest.mark.parametrize('brand', ['solaris', 'solaris10'])
    def test_remove_anets_when_no_anet(zonecfg, zone, brand):
        zone.kw_brand = brand
        zone.provision_net_type = 'no-anet'
        zone.zone_configure_net()
        zonecfg.assert_called_once_with(['remove -F anet'])


@pytest.mark.ci
class TestCreateSysidcfg:
    @staticmethod
    @pytest.mark.parametrize(
        'domain, nameservers, searches, expected',
        [
            ['local', ['192.168.10.1', '192.168.10.2'], ['local', 'external'],
             'name_service=DNS {domain_name=local\n    name_server=192.168.10.1,192.168.10.2\n    search=local,external\n    }\n'],
            ['local', ['192.168.10.1'], [],
             'name_service=DNS {domain_name=local\n    name_server=192.168.10.1\n    }\n'],
            [None, [], [], 'name_service=NONE'],
            [None, ['192.168.10.1'], [], 'name_service=NONE'],
            [None, ['192.168.10.1'], ['local'], 'name_service=NONE'],
            [None, [], ['local'], 'name_service=NONE'],
            ['local', [], [], 'name_service=NONE'],
            ['local', [], ['local'], 'name_service=NONE'],
        ])
    def test_create_correct_sysidcfg_name_service(
            mocker,
            domain,
            nameservers,
            searches,
            expected,
            zone):
        mocker.patch.object(ContainerZone, 'get_ns', return_value=[domain, nameservers, searches])
        mocker.patch.object(ContainerZone, 'get_sysidcfg_network_interfaces',
                            return_value=['network_interface=NONE {hostname=zonex}'])
        zone.create_sysidcfg()
        with open(zone.sysidcfg, 'r') as sysidcfg_file:
            assert expected in sysidcfg_file.read()
