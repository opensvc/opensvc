# coding: utf-8
from __future__ import print_function

import rcStatus
import svc
from node import Node
from poolDirectory import Pool
from rcUtilities import factory
from resVolume import Volume
from resContainerDocker import Container
import pytest


@pytest.mark.ci
class TestVolumeOptionsFromSrcDir:
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
        container = Container(rid='#docker0',
                              volume_mounts=[str(tmpdir) + ':/dst:' + options])
        svc1 += container
        res = container.volume_options()
        assert res == [str(tmpdir) + ':/dst:' + expected_options]


@pytest.mark.ci
class TestVolumeOptionsFromSrcVol:
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
        container = Container(rid='#dck1',
                              volume_mounts=[vol_name + '/src:/dst:' + container_options])
        svc1 += vol
        svc1 += container

        assert container.volume_options() == [vol.mount_point + '/src:/dst:' + expected_options]
