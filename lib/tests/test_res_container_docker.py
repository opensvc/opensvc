# coding: utf-8
from __future__ import print_function

import rcStatus
import svc
from drivers.resource.container.docker import ContainerDocker
from drivers.resource.volume import Volume
from node import Node
from poolDirectory import Pool
from rcUtilities import factory
import rcExceptions as ex
import pytest


@pytest.mark.ci
class TestVolumeOptions:
    @staticmethod
    @pytest.mark.parametrize('options, expected_options',
                             (('ro', 'ro'),
                              ('rw', 'rw')))
    def test_it_return_correct_mount_options_when_source_is_os_dir(
            tmpdir,
            osvc_path_tests,
            options,
            expected_options):
        svc1 = svc.Svc('test-service')
        container = ContainerDocker(rid='#docker0',
                                    volume_mounts=[str(tmpdir) + ':/dst:' + options])
        svc1 += container
        res = container.volume_options()
        assert res == [str(tmpdir) + ':/dst:' + expected_options]

    @staticmethod
    @pytest.mark.parametrize('vol_options, container_options, expected_options',
                             (('rwo', 'ro', 'ro'),
                              ('rwx', 'ro', 'ro'),
                              ('roo', 'ro', 'ro'),
                              ('rox', 'ro', 'ro'),
                              ('rwo', 'rw', 'rw'),
                              ('rwx', 'rw', 'rw'),
                              ('roo', 'rw', 'ro'),
                              ('rox', 'rw', 'ro'),
                              ))
    def test_mount_options_values_when_source_is_volume(
            mocker,
            vol_options,
            container_options,
            expected_options):
        mocker.patch.object(Volume, 'status', return_value=rcStatus.UP)

        vol_name = 'vol-' + vol_options + '-' + container_options
        Pool(name="dir1", node=Node()).configure_volume(factory("vol")(name=vol_name),
                                                        access=vol_options)

        svc1 = svc.Svc('svc1')
        vol = Volume(rid="#" + vol_name, name=vol_name, access=vol_options)
        container = ContainerDocker(rid='#dck1',
                                    volume_mounts=[vol_name + '/src:/dst:' + container_options])
        svc1 += vol
        svc1 += container

        assert container.volume_options() == [vol.mount_point + '/src:/dst:' + expected_options]

    @staticmethod
    @pytest.mark.parametrize('volume_mounts', [
        ['vol1/src:/dst:ro', 'vol2/src:/dst:rw'],
        ['vol1/src:/dst:ro', '/tmp:/dst:rw'],
    ])
    def test_raises_on_dup_destinations(mocker, osvc_path_tests, volume_mounts):
        mocker.patch.object(Volume, 'status', return_value=rcStatus.UP)

        svc1 = svc.Svc('svc1')
        pool = Pool(name="dir1", node=Node())
        for vol_name in ['vol1', 'vol2']:
            pool.configure_volume(factory("vol")(name=vol_name))
            svc1 += Volume(rid="#" + vol_name, name=vol_name)

        container = ContainerDocker(rid='#dck1', volume_mounts=volume_mounts)
        svc1 += container

        with pytest.raises(ex.excError, match=r'same destination mount point'):
            container.volume_options()
