try:
    from unittest.mock import call
except ImportError:
    from mock import call

import pytest

from core.exceptions import Error
from core.status import DOWN, UP, NA
from drivers.resource.disk.md import DiskMd, Env

LIB_NAME = 'drivers.resource.disk.md'

detail_out = """/dev/md/s.disk.1:
           Version : 1.2
     Creation Time : Thu Apr 16 15:19:11 2020
        Raid Level : raid0
        Array Size : 16384 (16.00 MiB 16.78 MB)
      Raid Devices : 2
     Total Devices : 2
       Persistence : Superblock is persistent

       Update Time : Thu Apr 16 15:19:11 2020
             State : clean
    Active Devices : 2
   Working Devices : 2
    Failed Devices : 0
     Spare Devices : 0

        Chunk Size : 512K

Consistency Policy : none

              Name : node1:s.disk.1  (local to host node1)
              UUID : 4198d2fe:84f45665:58aae039:ceff1171
            Events : 0

    Number   Major   Minor   RaidDevice State
       0       7        0        0      active sync   /dev/loop0
       1       7        1        1      active sync   /dev/loop1"""

mdadm_scan_out = """ARRAY /dev/md/s.disk.1  level=raid0 metadata=1.2 num-devices=2 UUID=s-uuid name=node1:s.disk.1
   devices=/dev/loop5,/dev/loop4
ARRAY /dev/md/ss.disk.1  level=raid0 metadata=1.2 num-devices=2 UUID=ss.uuid name=node1:ss.disk.1
   devices=/dev/loop3,/dev/loop2"""


@pytest.fixture(scope='function')
def mdadm_scan(mocker):
    return mocker.patch(LIB_NAME + '.justcall_mdadm_scan', return_value=(mdadm_scan_out, '', 0))


@pytest.fixture(scope='function')
def mdadm_detail(mocker):
    return mocker.patch(LIB_NAME + '.justcall_mdadm_detail', return_value=(detail_out, '', 0))


@pytest.fixture(scope='function')
def mdadm_create(mocker):
    return mocker.patch(LIB_NAME + '.justcall_md_create', return_value=('stdout', 'stderr', 0))


@pytest.fixture(scope='function')
def has_files(mocker):
    def create_mock(paths):
        def path_exists(path):
            print('check if path exists: %s' % path)
            return path in paths

        return mocker.patch(LIB_NAME + '.path_exists', side_effect=path_exists)

    return create_mock


@pytest.fixture(scope='function')
def svc(mocker):
    svc = mocker.Mock(name='svc')
    svc.namespace = ''
    svc.name = 's'
    svc.loggerpath = 'something'
    return svc


@pytest.fixture(scope='function')
def has_mdadm(mocker):
    return mocker.patch('drivers.resource.disk.md.which', return_value='/sbin/mdadm')


@pytest.fixture(scope='function')
def md(svc, has_files):
    md = DiskMd(rid='disk#1', devs=['/dev/loop1', '/dev/loop2'], spares=0)
    md.svc = svc
    has_files(['/dev/md/s.disk.1'])
    return md


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('has_mdadm')
class TestDiskMdProvisionSuccess:
    @staticmethod
    def test_run_correct_mdadm_commands(mdadm_create, mdadm_detail, md):
        md.provisioner()
        mdadm_create.assert_called_once_with(
            argv=['/sbin/mdadm', '--create', '/dev/md/s.disk.1', '--force',
                  '--run',
                  '--quiet', '--metadata=default', '-n', '2', '/dev/loop1',
                  '/dev/loop2'],
            input=b'no\n')
        mdadm_detail.assert_called_once_with(argv=['/sbin/mdadm',
                                                   '--detail',
                                                   '/dev/md/s.disk.1'])

    @staticmethod
    @pytest.mark.usefixtures('mdadm_create', 'mdadm_detail')
    def test_mdadm_scan_not_required(mdadm_scan, md):
        md.provisioner()
        mdadm_scan.assert_not_called()

    @staticmethod
    @pytest.mark.usefixtures('mdadm_create', 'mdadm_detail')
    @pytest.mark.parametrize('shared', [True, False])
    def test_update_svc_config_with_created_md_uuid(shared, md):
        md.shared = shared
        md.provisioner()
        if shared:
            expected_kw = "uuid"
        else:
            expected_kw = 'uuid@' + Env.nodename
        md.svc._set.assert_called_once_with(
            'disk#1',
            expected_kw,
            '4198d2fe:84f45665:58aae039:ceff1171')

    @staticmethod
    @pytest.mark.usefixtures('mdadm_create', 'mdadm_detail')
    def test_can_rollback(md):
        md.provisioner()
        assert md.can_rollback is True

    @staticmethod
    @pytest.mark.usefixtures('mdadm_create', 'mdadm_detail')
    def test_unset_svc_node_devtree(md):
        md.provisioner()
        md.svc.node.unset_lazy.assert_called_once_with('devtree')


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('has_mdadm', 'osvc_path_tests')
class TestDiskMdProvisionRaiseWhenMdmadmCreateFails:
    @staticmethod
    def test_exception_is_correct(mdadm_create, md):
        mdadm_create.return_value = 'stdout', 'mdadm create error msg', 1
        with pytest.raises(Error, match='mdadm create error msg'):
            md.provisioner()

    @staticmethod
    def test_rollback_is_false(mdadm_create, md):
        mdadm_create.return_value = '', '', 1
        with pytest.raises(Exception):
            md.provisioner()
        assert md.can_rollback is False


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('has_mdadm', 'mdadm_create')
class TestDiskMdProvisionRaiseWhenMdmadmDetailFailures:
    @staticmethod
    def test_uuid_not_found(mdadm_detail, md):
        mdadm_detail.return_value = '', '', 0
        with pytest.raises(Error, match='unable to determine md uuid'):
            md.provisioner()

    @staticmethod
    def test_exit_code_not_0(mdadm_detail, md):
        mdadm_detail.return_value = '', 'mdadm detail error', 1
        with pytest.raises(Error, match='mdadm detail error'):
            md.provisioner()

    @staticmethod
    def test_can_rollback(mdadm_detail, md):
        mdadm_detail.return_value = '', '', 0
        with pytest.raises(Exception):
            md.provisioner()
        assert md.can_rollback is True


@pytest.mark.ci
@pytest.mark.usefixtures('has_mdadm')
class TestDiskMdProvisionRaiseWhenNotEnoughDevs:
    @staticmethod
    def test_correct_exception(mocker, svc):
        mocker.patch.object(DiskMd, 'oget', return_value=[])
        md = DiskMd(rid='disk#1')
        md.svc = svc
        with pytest.raises(Error, match="at least 1 device must be set in the 'devs' provisioning"):
            md.provisioner()

    @staticmethod
    def test_can_not_rollback(mocker, svc):
        mocker.patch.object(DiskMd, 'oget', return_value=[])
        md = DiskMd(rid='disk#1')
        md.svc = svc
        with pytest.raises(Exception):
            md.provisioner()
        assert md.can_rollback is False


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('has_mdadm')
class TestDiskMdUnProvision:
    @staticmethod
    @pytest.mark.parametrize('shared', [True, False])
    def test_remove_resource_uuid_from_svc_config(md_with_uuid, shared):
        md_with_uuid.shared = shared
        md_with_uuid.unprovisioner()
        if shared:
            expected_kw = "uuid"
        else:
            expected_kw = 'uuid@' + Env.nodename
        md_with_uuid.svc._set.assert_called_once_with('disk#1', expected_kw, '')


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('has_mdadm')
class TestDiskMdStatusWhenNoUuid:
    @staticmethod
    def test_status_is_na(md):
        assert md._status() == NA


@pytest.fixture(scope='function')
def md_with_uuid(mocker, tmp_file, svc):
    mocker.patch.object(DiskMd, 'status_log')
    mocker.patch.object(DiskMd,
                        'mdadm_cf',
                        new_callable=mocker.PropertyMock(return_value=tmp_file))
    md = DiskMd(rid='disk#1', devs=['/dev/loop1', '/dev/loop2'], spares=0)
    md.svc = svc
    md.uuid = 's-uuid'
    return md


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('has_mdadm')
class TestDiskMdStatus:
    @staticmethod
    def test_status_is_down_when_uuid_not_in_scan(mdadm_scan, md_with_uuid):
        mdadm_scan.return_value = 'stdout', 'stderr', 0
        assert md_with_uuid._status() == DOWN

    @staticmethod
    @pytest.mark.usefixtures('mdadm_scan')
    @pytest.mark.parametrize(
        'devs',
        [['/dev/disk/by-id/md-uuid-s-uuid'],
         ['/dev/disk/by-id/md-uuid-s-uuid', '/dev/md/s.disk.1'],
         ['/dev/md/s.disk.1']])
    def test_status_is_down_when_detail_is_invalid(has_files, mdadm_detail, md_with_uuid, devs):
        mdadm_detail.return_value = '', '', 0
        has_files(devs)
        assert md_with_uuid._status() == DOWN
        md_with_uuid.status_log.call_args == call('unknown')

    @staticmethod
    def test_up_when_auto_assemble_not_disabled(md_with_uuid):
        assert md_with_uuid._status() == DOWN
        assert md_with_uuid.status_log
        assert md_with_uuid.status_log.call_args == call('auto-assemble is not disabled')

    @staticmethod
    @pytest.mark.usefixtures('mdadm_scan')
    def test_down_when_no_md_device_files(md_with_uuid):
        with open(md_with_uuid.mdadm_cf, 'w') as f:
            f.write('AUTO -all\n')
        assert md_with_uuid._status() == DOWN

    @staticmethod
    @pytest.mark.usefixtures('mdadm_detail', 'mdadm_scan')
    @pytest.mark.parametrize(
        'devs',
        [['/dev/disk/by-id/md-uuid-s-uuid'],
         ['/dev/disk/by-id/md-uuid-s-uuid', '/dev/md/s.disk.1'],
         ['/dev/md/s.disk.1']])
    def test_up_when_md_device_file_exists(has_files, devs, md_with_uuid):
        has_files(devs)
        with open(md_with_uuid.mdadm_cf, 'w') as f:
            f.write('AUTO -all\n')
        assert md_with_uuid._status() == UP
        assert md_with_uuid.status_log.call_count == 0


@pytest.mark.ci
@pytest.mark.usefixtures('has_mdadm')
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDiskMdProvisioned:
    @staticmethod
    def test_not_provisioned(md_with_uuid):
        assert not md_with_uuid.provisioned()

    @staticmethod
    def test_true(mocker, md_with_uuid):
        mocker.patch.object(md_with_uuid, 'has_it', return_value=True)
        assert md_with_uuid.provisioned()


@pytest.mark.ci
@pytest.mark.usefixtures('has_mdadm')
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
class TestDiskMdExposedDevs:
    @staticmethod
    @pytest.mark.usefixtures('mdadm_detail', 'mdadm_scan')
    def test_list_all_devs(mocker, has_files, md_with_uuid):
        has_files(['/dev/disk/by-id/md-uuid-s-uuid'])
        mocker.patch(LIB_NAME + '.os.path.realpath', side_effect=lambda x: x)
        assert md_with_uuid.exposed_devs() == set(['/dev/disk/by-id/md-uuid-s-uuid'])


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('has_mdadm')
class TestDiskMdDevname:
    @staticmethod
    def test_default_devname_when_namespace_is_defined(mdadm_create, mdadm_detail, md):
        md.svc.namespace = 'ns1'
        md.svc.name = 'foo'
        md.rid = "disk#idx1"
        assert md.devname() == "/dev/md/ns1.foo.disk.idx1"

    @staticmethod
    def test_default_devname_when_no_namespace(mdadm_create, mdadm_detail, md):
        md.svc.name = 'foo'
        md.rid = "disk#idx1"
        assert md.devname() == "/dev/md/foo.disk.idx1"

    @staticmethod
    def test_custom_devname(mdadm_create, mdadm_detail, md):
        md.name = "abcd"
        assert md.devname() == "/dev/md/abcd"



# A uuid as mdadm writes it, and the same in the form blkid prints.
md_uuid = '4198d2fe:84f45665:58aae039:ceff1171'
md_blkid_uuid = '4198d2fe-84f4-5665-58aa-e039ceff1171'

blkid_out = """/dev/sda1: UUID="0e6e8dfd-8f9c-4b5e-9a5e-1c14b0e1a4e5" TYPE="ext4"
/dev/loop4: UUID="%s" UUID_SUB="1ae4ca4a-0cbc-0a60-45a8-ee18231da0b3" LABEL="node1:s.disk.1" TYPE="linux_raid_member"
/dev/loop5: UUID="%s" UUID_SUB="98c635be-d1c9-f449-bdf7-f3e2bf375e98" LABEL="node1:s.disk.1" TYPE="linux_raid_member"
""" % (md_blkid_uuid, md_blkid_uuid)

blkid_out_other_md = """/dev/loop6: UUID="deadbeef-dead-beef-dead-beefdeadbeef" TYPE="linux_raid_member"
"""

blkid_out_container = """/dev/sdb: TYPE="isw_raid_member"
/dev/sdc: TYPE="isw_raid_member"
"""


@pytest.fixture(scope='function')
def blkid(mocker):
    return mocker.patch(LIB_NAME + '.justcall_blkid', return_value=(blkid_out, '', 0))


@pytest.fixture(scope='function')
def dev_to_paths(mocker):
    return mocker.patch('utilities.devices.linux.dev_to_paths', side_effect=lambda dev: [dev])


@pytest.fixture(scope='function')
def md_blkid(svc, mocker, tmp_file):
    mocker.patch.object(DiskMd, 'mdadm_cf',
                        new_callable=mocker.PropertyMock(return_value=tmp_file))
    md = DiskMd(rid='disk#1', devs=['/dev/loop4', '/dev/loop5'], spares=0)
    md.svc = svc
    md.uuid = md_uuid
    return md


@pytest.mark.ci
@pytest.mark.usefixtures('osvc_path_tests')  # for cache
@pytest.mark.usefixtures('has_mdadm')
class TestDiskMdBlkid:
    """
    blkid answers "is this md here, and which devices hold it" by
    reading the superblock where each metadata format keeps it, where
    "mdadm -E --scan" reads the devices through when it is not told
    which format to expect.
    """

    @staticmethod
    def test_the_uuid_is_converted_to_the_form_blkid_prints(md_blkid):
        assert md_blkid.blkid_uuid() == md_blkid_uuid

    @staticmethod
    @pytest.mark.parametrize('uuid', ['s-uuid', '', None, '4198d2fe:84f45665'])
    def test_a_uuid_mdadm_would_not_have_written_is_left_to_the_scan(md_blkid, uuid):
        md_blkid.uuid = uuid
        assert md_blkid.blkid_uuid() is None
        assert md_blkid.blkid_members() == (set(), False)

    @staticmethod
    def test_has_it_without_running_the_scan(blkid, mdadm_scan, md_blkid):
        assert md_blkid.has_it() is True
        assert mdadm_scan.call_count == 0

    @staticmethod
    def test_has_not_it_without_running_the_scan(blkid, mdadm_scan, md_blkid):
        blkid.return_value = blkid_out_other_md, '', 0
        assert md_blkid.has_it() is False
        assert mdadm_scan.call_count == 0

    @staticmethod
    def test_a_node_holding_no_md_is_an_answer_too(blkid, mdadm_scan, md_blkid):
        blkid.return_value = '', '', 2
        assert md_blkid.has_it() is False
        assert mdadm_scan.call_count == 0

    @staticmethod
    def test_a_container_member_is_left_to_the_scan(blkid, mdadm_scan, md_blkid):
        # An imsm or ddf container names none of the volumes it holds,
        # so the md looked for may be in there.
        blkid.return_value = blkid_out_container, '', 0
        md_blkid.uuid = 's-uuid'  # the uuid the mocked scan reports
        assert md_blkid.has_it() is True
        assert mdadm_scan.call_count == 1

    @staticmethod
    def test_a_failed_blkid_is_left_to_the_scan(blkid, mdadm_scan, md_blkid):
        blkid.return_value = '', 'blkid: error', 1
        md_blkid.uuid = 's-uuid'
        assert md_blkid.has_it() is True
        assert mdadm_scan.call_count == 1

    @staticmethod
    def test_an_absent_blkid_is_left_to_the_scan(mocker, mdadm_scan, md_blkid):
        mocker.patch(LIB_NAME + '.which',
                     side_effect=lambda prog: None if prog == 'blkid' else '/sbin/' + prog)
        md_blkid.uuid = 's-uuid'
        assert md_blkid.has_it() is True
        assert mdadm_scan.call_count == 1

    @staticmethod
    def test_sub_devs_inactive_without_running_the_scan(blkid, mdadm_scan, dev_to_paths, md_blkid):
        assert md_blkid.sub_devs_inactive() == {'/dev/loop4', '/dev/loop5'}
        assert mdadm_scan.call_count == 0

    @staticmethod
    def test_sub_devs_inactive_reports_a_lun_once(blkid, mdadm_scan, mocker, md_blkid):
        # blkid lists a multipathed member once per path, as mdadm does.
        blkid.return_value = blkid_out + \
            '/dev/sdx: UUID="%s" TYPE="linux_raid_member"\n' % md_blkid_uuid, '', 0
        mocker.patch('utilities.devices.linux.dev_to_paths',
                     side_effect=lambda dev: ['/dev/sdx'] if dev == '/dev/loop4' else [dev])
        assert md_blkid.sub_devs_inactive() == {'/dev/loop4', '/dev/loop5'}
        assert mdadm_scan.call_count == 0
