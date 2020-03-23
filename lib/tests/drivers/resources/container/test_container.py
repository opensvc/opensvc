import pytest

from rcUtilities import driver_import


OS_LIST = {'Linux', 'SunOS', 'Darwin', 'FreeBSD', 'HP-UX', 'OSF1'}


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('driver_name, class_name, kwargs, expected_type', [
    ('docker', 'ContainerDocker', {'rid': '#1'}, 'container.docker'),
    ('zone', 'ContainerZone', {'rid': '#1', 'name': 'z1'}, 'container.zone'),
    ('xen', 'ContainerXen', {'rid': '#1', 'name': 'z1'}, 'container.xen'),
    ('vz', 'ContainerVz', {'rid': '#1', 'name': 'z1'}, 'container.vz'),
    ('vcloud', 'ContainerVcloud', {'rid': '#1', 'name': 'z1'}, 'container.vcloud'),
    ('vbox', 'ContainerVbox', {'rid': '#1', 'name': 'z1'}, 'container.vbox'),
    ('lxc', 'ContainerLxc', {'rid': '#1', 'name': 'z1'}, 'container.lxc'),
    ('lxd', 'ContainerLxd', {'rid': '#1', 'name': 'z1'}, 'container.lxd'),
    ('ldom', 'ContainerLdom', {'rid': '#1', 'name': 'z1'}, 'container.ldom'),
    ('kvm', 'ContainerKvm', {'rid': '#1', 'name': 'z1'}, 'container.kvm'),
    ('openstack', 'ContainerOpenstack', {'rid': '#1', 'name': 'z1'}, 'container.openstack'),
    ('ovm', 'ContainerOvm', {'rid': '#1', 'name': 'z1', 'uuid': 'abcd'}, 'container.ovm'),
    ('podman', 'ContainerPodman', {'rid': '#1'}, 'container.podman'),
    ('srp', 'ContainerSrp', {'rid': '#1', 'name': 'name'}, 'container.srp'),
    ('amazon', 'ContainerAmazon', {'rid': '#1', 'name': 'name'}, 'container.amazon'),
    ('esx', 'ContainerEsx', {'rid': '#1', 'name': 'name'}, 'container.esx'),
    ('jail', 'ContainerJail', {'rid': '#1', 'name': 'name'}, 'container.jail'),
    ('jail', 'ContainerJail', {'rid': '#1', 'name': 'name'}, 'container.jail'),
    ('hpvm', 'ContainerHpvm', {'rid': '#1', 'name': 'name'}, 'container.hpvm'),

])
def test_create_container_with_correct_type(mock_sysname, sysname, driver_name, class_name, kwargs, expected_type):
    mock_sysname(sysname)
    driver = driver_import('resource', 'container', driver_name)
    klass = getattr(driver, class_name)
    resource = klass(**kwargs)
    assert resource.type == expected_type
