"""
Simple test for driver instance creation

When need more tests move the scenario into dedicated test file
"""

import socket
import pytest

from tests.drivers.resources.helpers import assert_resource_has_mandatory_methods


nodename = socket.gethostname().lower()
OS_LIST = {'AIX', 'Darwin', 'FreeBSD', 'HP-UX', 'Linux', 'OSF1', 'SunOS', 'Windows'}

SCENARIOS = [
    ('app.simple', 'AppSimple', {'rid': '#1'}, 'app.simple'),
    ('app.simple.sunos', 'AppSimple', {'rid': '#1'}, 'app.simple'),
    ('app.forking', 'AppForking', {'rid': '#1'}, 'app.forking'),
    ('app.winservice', 'AppWinservice', {'rid': '#1'}, 'app.winservice'),

    ('certificate.tls', 'CertificateTls', {'rid': '#1'}, 'certificate.tls'),

    ('container.amazon', 'ContainerAmazon', {'rid': '#1', 'name': 'name'}, 'container.amazon'),
    ('container.docker', 'ContainerDocker', {'rid': '#1'}, 'container.docker'),
    ('container.esx', 'ContainerEsx', {'rid': '#1', 'name': 'name'}, 'container.esx'),
    ('container.hpvm', 'ContainerHpvm', {'rid': '#1', 'name': 'name'}, 'container.hpvm'),
    ('container.jail', 'ContainerJail', {'rid': '#1', 'name': 'name'}, 'container.jail'),
    ('container.kvm', 'ContainerKvm', {'rid': '#1', 'name': 'z1'}, 'container.kvm'),
    ('container.ldom', 'ContainerLdom', {'rid': '#1', 'name': 'z1'}, 'container.ldom'),
    ('container.lxc', 'ContainerLxc', {'rid': '#1', 'name': 'z1'}, 'container.lxc'),
    ('container.lxd', 'ContainerLxd', {'rid': '#1', 'name': 'z1'}, 'container.lxd'),
    ('container.openstack', 'ContainerOpenstack', {'rid': '#1', 'name': 'z1'}, 'container.openstack'),
    ('container.ovm', 'ContainerOvm', {'rid': '#1', 'name': 'z1', 'uuid': 'abcd'}, 'container.ovm'),
    ('container.podman', 'ContainerPodman', {'rid': '#1'}, 'container.podman'),
    ('container.srp', 'ContainerSrp', {'rid': '#1', 'name': 'name'}, 'container.srp'),
    ('container.vbox', 'ContainerVbox', {'rid': '#1', 'name': 'z1'}, 'container.vbox'),
    ('container.vcloud', 'ContainerVcloud', {'rid': '#1', 'name': 'z1'}, 'container.vcloud'),
    ('container.vz', 'ContainerVz', {'rid': '#1', 'name': 'z1'}, 'container.vz'),
    ('container.xen', 'ContainerXen', {'rid': '#1', 'name': 'z1'}, 'container.xen'),
    ('container.zone', 'ContainerZone', {'rid': '#1', 'name': 'z1'}, 'container.zone'),

    ('disk.advfs', 'DiskAdvfs', {'name': 'vg1'}, 'disk.advfs'),
    ('disk.amazon', 'DiskAmazon', {'name': 'vg1'}, 'disk.amazon'),
    ('disk.disk', 'DiskDisk', {'name': 'vg1'}, 'disk.disk'),
    ('disk.disk.linux', 'DiskDisk', {'name': 'vg1'}, 'disk.disk'),
    ('disk.drbd', 'DiskDrbd', {'res': 'res'}, 'disk.drbd'),
    ('disk.gandi', 'DiskGandi', {'name': 'vg1'}, 'disk.gandi'),
    ('disk.gce', 'DiskGce', {}, 'disk.gce'),
    ('disk.hpvm', 'DiskHpvm', {'name': 'vg1'}, 'disk.vg'),
    ('disk.ldom', 'DiskLdom', {'name': 'ldom-vol'}, 'disk.ldom'),
    #  disk.loop has its own test file
    #  disk.lv has its own test file
    ('disk.md', 'DiskMd', {}, 'disk.md'),
    ('disk.rados', 'DiskRados', {'client_id': 'id1'}, 'disk.rados'),
    #  disk.raw has its own test file
    #  disk.scsireserv has its own test file
    ('disk.vdisk', 'DiskVdisk', {'name': 'name'}, 'disk.vdisk'),
    ('disk.vxvol', 'DiskVxvol', {}, 'disk.vxvol'),
    ('disk.zpool', 'ZpoolDisk', {'name': 'pool1'}, 'disk.zpool'),
    ('disk.zvol', 'DiskZvol', {'name': 'zvol'}, 'disk.zvol'),

    ('expose.envoy', 'ExposeEnvoy', {'rid': '#1', 'port': 8000, 'listener_port': 9000}, 'expose.envoy'),

    ('fs', 'Fs', {'rid': '#1', 'mount_point': '/tmp/plop', 'fs_type': 'plop',
                  'mount_options': None, 'device': '/dev/a_device'},
     'fs'),
    ('fs.btrfs', 'FsBtrfs', {}, 'fs'),
    ('fs.directory', 'FsDirectory', {}, 'fs.directory'),
    ('fs.docker', 'FsDocker', {}, 'fs.docker'),
    ('fs.ext2', 'FsExt2', {}, 'fs'),
    ('fs.ext3', 'FsExt3', {}, 'fs'),
    ('fs.ext4', 'FsExt4', {}, 'fs'),
    #  fs.flag has its own test file
    ('fs.hfs', 'FsHfs', {'rid': '#1', 'mount_point': '/tmp/plop', 'device': '/dev/john', 'fs_type': 'plop',
                         'mount_options': None},
     'fs'),
    #  fs.vxfs has its own test file
    ('fs.xfs', 'FsXfs', {}, 'fs'),
    #  fs.zfs has its own test file

    ('hashpolicy.envoy', 'HashpolicyEnvoy', {'rid': '#1'}, 'hash_policy.envoy'),

    ('ip', 'Ip', {'rid': '#1'}, 'ip'),
    ('ip.amazon', 'IpAmazon', {'rid': '#1'}, 'ip.amazon'),
    ('ip.cni', 'IpCni', {'rid': '#1'}, 'ip.cni'),
    ('ip.crossbow', 'IpCrossbow', {'rid': '#1'}, 'ip.crossbow'),
    ('ip.gce', 'IpGce', {'rid': '#1'}, 'ip.gce'),
    ('ip.netns', 'IpNetns', {'rid': '#1'}, 'ip.netns'),
    ('ip.zone', 'IpZone', {'rid': '#1'}, 'ip.zone'),

    ('route.envoy', 'RouteEnvoy', {'rid': '#1'}, 'route.envoy'),

    #  share.nfs has its own test file

    ('sync', 'Sync', {'rid': '#1'}, None),
    ('sync.btfrssnap', 'SyncBtrfssnap', {'rid': '#1'}, 'sync.btrfssnap'),
    ('sync.btrfs', 'SyncBtrfs', {'rid': '#1', 'src': 'src:a', 'dst': 'dst:foo', 'target': ['foo']}, 'sync.btrfs'),
    ('sync.dds', 'SyncDds', {'rid': '#1', 'target': ['a']}, 'sync.dds'),
    ('sync.docker', 'SyncDocker', {'rid': '#1', 'target': ['a']}, 'sync.docker'),
    ('sync.evasnap', 'SyncEvasnap', {'rid': '#1', 'target': ['a']}, 'sync.evasnap'),
    ('sync.evasnap', 'SyncEvasnap', {'rid': '#1', 'target': ['a']}, 'sync.evasnap'),
    ('sync.hp3par', 'SyncHp3par', {'rid': '#1', 'array': 'array1', 'rcg_names': {'array1': 'foo'}}, 'sync.hp3par'),
    ('sync.hp3parsnap', 'SyncHp3parsnap', {'rid': '#1', 'vvnames': ['']}, 'sync.hp3parsnap'),
    ('sync.ibmdssnap', 'SyncIbmdssnap', {'rid': '#1'}, 'sync.ibmdssnap'),
    ('sync.necismsnap', 'SyncNecismsnap', {'rid': '#1', 'devs': 'a:b'}, 'sync.necismsnap'),
    ('sync.netapp', 'SyncNetapp', {'rid': '#1', 'path': 'a:b'}, 'sync.netapp'),
    ('sync.nexenta', 'SyncNexenta', {'rid': '#1', 'filers': {nodename: 'foo'}}, 'sync.nexenta'),
    # need investigate: ('sync.radosclone', 'SyncRadosclone', {'rid': '#1', 'client_id': 'foo'}, 'sync.radosclone'),
    ('sync.radossnap', 'SyncRadossnap', {'rid': '#1', 'client_id': 'foo'}, 'sync.radossnap'),
    ('sync.rsync', 'SyncRsync', {'rid': '#1'}, 'sync.rsync'),
    ('sync.s3', 'SyncS3', {'rid': '#1'}, 'sync.s3'),
    ('sync.symclone', 'SyncSymclone', {'rid': '#1'}, 'sync.symclone'),
    ('sync.symsnap', 'SyncSymsnap', {'rid': '#1'}, 'sync.symclone'),
    ('sync.symsrdfs', 'SyncSymsrdfs', {'rid': '#1'}, 'sync.symsrdfs'),
    ('sync.zfs', 'SyncZfs', {'rid': '#1', 'src': 'tank/a', 'dst': 'tank/b', 'target': ['a']}, 'sync.zfs'),
    ('sync.zfssnap', 'Sync', {'rid': '#1'}, None),

    ('task.docker', 'TaskDocker', {'rid': '#1'}, 'task.docker'),
    ('task.host', 'TaskHost', {'rid': '#1'}, 'task.host'),
    ('task.podman', 'TaskPodman', {'rid': '#1'}, 'task.podman'),

    ('vhost.envoy', 'VhostEnvoy', {'rid': '#1'}, 'vhost.envoy'),

    ('volume', 'Volume', {'rid': '#1'}, 'volume'),
]


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('scenario', SCENARIOS)
class TestDriverGenInstances:
    @staticmethod
    def test_has_correct_type(create_driver_resource, sysname, scenario):
        assert create_driver_resource(sysname, scenario).type == scenario[3]

    @staticmethod
    def test_has_mandatory_methods(create_driver_resource, sysname, scenario):
        assert_resource_has_mandatory_methods(create_driver_resource(sysname, scenario))
