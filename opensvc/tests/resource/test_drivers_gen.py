"""
Simple test for driver instance creation

When need more tests move the scenario into dedicated test file
"""

import socket
import pytest

from tests.helpers import assert_resource_has_mandatory_methods


nodename = socket.gethostname().lower()
OS_LIST = {'AIX', 'Darwin', 'FreeBSD', 'HP-UX', 'Linux', 'OSF1', 'SunOS', 'Windows'}

SCENARIOS = [
    ('app.simple', {'rid': '#1'}, 'app.simple'),
    ('app.simple.sunos', {'rid': '#1'}, 'app.simple'),
    ('app.forking', {'rid': '#1'}, 'app.forking'),
    ('app.winservice', {'rid': '#1'}, 'app.winservice'),

    ('certificate.tls', {'rid': '#1'}, 'certificate.tls'),

    ('container.amazon', {'rid': '#1', 'name': 'name'}, 'container.amazon'),
    ('container.docker', {'rid': '#1'}, 'container.docker'),
    ('container.esx', {'rid': '#1', 'name': 'name'}, 'container.esx'),
    ('container.hpvm', {'rid': '#1', 'name': 'name'}, 'container.hpvm'),
    ('container.jail', {'rid': '#1', 'name': 'name'}, 'container.jail'),
    ('container.kvm', {'rid': '#1', 'name': 'z1'}, 'container.kvm'),
    ('container.ldom', {'rid': '#1', 'name': 'z1'}, 'container.ldom'),
    ('container.lxc', {'rid': '#1', 'name': 'z1'}, 'container.lxc'),
    ('container.lxd', {'rid': '#1', 'name': 'z1'}, 'container.lxd'),
    ('container.openstack', {'rid': '#1', 'name': 'z1'}, 'container.openstack'),
    ('container.ovm', {'rid': '#1', 'name': 'z1', 'uuid': 'abcd'}, 'container.ovm'),
    ('container.podman', {'rid': '#1'}, 'container.podman'),
    ('container.srp', {'rid': '#1', 'name': 'name'}, 'container.srp'),
    ('container.vbox', {'rid': '#1', 'name': 'z1'}, 'container.vbox'),
    ('container.vcloud', {'rid': '#1', 'name': 'z1'}, 'container.vcloud'),
    ('container.vz', {'rid': '#1', 'name': 'z1'}, 'container.vz'),
    ('container.xen', {'rid': '#1', 'name': 'z1'}, 'container.xen'),
    ('container.zone', {'rid': '#1', 'name': 'z1'}, 'container.zone'),

    ('disk.advfs', {'name': 'vg1'}, 'disk.advfs'),
    ('disk.amazon', {'name': 'vg1'}, 'disk.amazon'),
    ('disk.disk', {'name': 'vg1'}, 'disk.disk'),
    ('disk.disk.linux', {'name': 'vg1'}, 'disk.disk'),
    ('disk.drbd', {'res': 'res'}, 'disk.drbd'),
    ('disk.gandi', {'name': 'vg1'}, 'disk.gandi'),
    ('disk.gce', {}, 'disk.gce'),
    ('disk.hpvm', {'name': 'vg1'}, 'disk.vg'),
    ('disk.ldom', {'name': 'ldom-vol'}, 'disk.ldom'),
    #  disk.loop has its own test file
    #  disk.lv has its own test file
    ('disk.md', {}, 'disk.md'),
    ('disk.rados', {'client_id': 'id1'}, 'disk.rados'),
    #  disk.raw has its own test file
    #  disk.scsireserv has its own test file
    ('disk.vdisk', {'name': 'name'}, 'disk.vdisk'),
    ('disk.vxvol', {}, 'disk.vxvol'),
    ('disk.zpool', {'name': 'pool1'}, 'disk.zpool'),
    ('disk.zvol', {'name': 'zvol'}, 'disk.zvol'),

    ('expose.envoy', {'rid': '#1', 'port': 8000, 'listener_port': 9000}, 'expose.envoy'),

    ('fs', {'rid': '#1', 'mount_point': '/tmp/plop', 'fs_type': 'plop',
            'mount_options': None, 'device': '/dev/a_device'},
     'fs'),
    ('fs.btrfs', {}, 'fs'),
    ('fs.directory', {}, 'fs.directory'),
    ('fs.docker', {}, 'fs.docker'),
    ('fs.ext2', {}, 'fs'),
    ('fs.ext3', {}, 'fs'),
    ('fs.ext4', {}, 'fs'),
    #  fs.flag has its own test file
    ('fs.hfs', {'rid': '#1', 'mount_point': '/tmp/plop', 'device': '/dev/john', 'fs_type': 'plop',
                'mount_options': None},
     'fs'),
    #  fs.vxfs has its own test file
    ('fs.xfs', {}, 'fs'),
    #  fs.zfs has its own test file

    ('hashpolicy.envoy', {'rid': '#1'}, 'hashpolicy.envoy'),

    ('ip.host', {'rid': '#1'}, 'ip'),
    ('ip.amazon', {'rid': '#1'}, 'ip.amazon'),
    ('ip.cni', {'rid': '#1'}, 'ip.cni'),
    ('ip.crossbow', {'rid': '#1'}, 'ip.crossbow'),
    ('ip.gce', {'rid': '#1'}, 'ip.gce'),
    ('ip.netns', {'rid': '#1'}, 'ip.netns'),
    ('ip.zone', {'rid': '#1'}, 'ip.zone'),

    ('route.envoy', {'rid': '#1'}, 'route.envoy'),

    #  share.nfs has its own test file

    ('sync.btrfssnap', {'rid': '#1'}, 'sync.btrfssnap'),
    ('sync.btrfs', {'rid': '#1', 'src': 'src:a', 'dst': 'dst:foo', 'target': ['foo']}, 'sync.btrfs'),
    ('sync.dds', {'rid': '#1', 'target': ['a']}, 'sync.dds'),
    ('sync.docker', {'rid': '#1', 'target': ['a']}, 'sync.docker'),
    ('sync.evasnap', {'rid': '#1', 'target': ['a']}, 'sync.evasnap'),
    ('sync.evasnap', {'rid': '#1', 'target': ['a']}, 'sync.evasnap'),
    ('sync.hp3par', {'rid': '#1', 'array': 'array1', 'rcg_names': {'array1': 'foo'}}, 'sync.hp3par'),
    ('sync.hp3parsnap', {'rid': '#1', 'vvnames': ['']}, 'sync.hp3parsnap'),
    ('sync.ibmdssnap', {'rid': '#1'}, 'sync.ibmdssnap'),
    ('sync.necismsnap', {'rid': '#1', 'devs': 'a:b'}, 'sync.necismsnap'),
    ('sync.netapp', {'rid': '#1', 'path': 'a:b'}, 'sync.netapp'),
    ('sync.nexenta', {'rid': '#1', 'filers': {nodename: 'foo'}}, 'sync.nexenta'),
    # need investigate: ('sync.radosclone', 'SyncRadosclone', {'rid': '#1', 'client_id': 'foo'}, 'sync.radosclone'),
    ('sync.radossnap', {'rid': '#1', 'client_id': 'foo'}, 'sync.radossnap'),
    ('sync.rsync', {'rid': '#1'}, 'sync.rsync'),
    ('sync.s3', {'rid': '#1'}, 'sync.s3'),
    ('sync.symclone', {'rid': '#1'}, 'sync.symclone'),
    ('sync.symsnap', {'rid': '#1'}, 'sync.symclone'),
    ('sync.symsrdfs', {'rid': '#1'}, 'sync.symsrdfs'),
    ('sync.zfs', {'rid': '#1', 'src': 'tank/a', 'dst': 'tank/b', 'target': ['a']}, 'sync.zfs'),
    ('sync.zfssnap', {'rid': '#1'}, 'sync.zfssnap'),

    ('task.docker', {'rid': '#1'}, 'task.docker'),
    ('task.host', {'rid': '#1'}, 'task.host'),
    ('task.podman', {'rid': '#1'}, 'task.podman'),

    ('vhost.envoy', {'rid': '#1'}, 'vhost.envoy'),

    ('volume', {'rid': '#1'}, 'volume'),
]


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')
@pytest.mark.parametrize('sysname', OS_LIST)
@pytest.mark.parametrize('scenario', SCENARIOS)
class TestDriverGenInstances:
    @staticmethod
    def test_has_correct_type(create_driver_resource, sysname, scenario):
        assert create_driver_resource(sysname, scenario).type == scenario[2]

    @staticmethod
    def test_has_mandatory_methods(create_driver_resource, sysname, scenario):
        assert_resource_has_mandatory_methods(create_driver_resource(sysname, scenario))
