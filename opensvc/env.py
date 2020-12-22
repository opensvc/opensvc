import os
import platform
import socket
import sys
import time
from uuid import uuid4

from utilities.storage import Storage


def create_or_update_dir(d):
    if not os.path.exists(d):
        os.makedirs(d)
    else:
        # update tmpdir timestamp to avoid tmpwatch kicking-in while we run
        now = time.time()
        try:
            os.utime(d, (now, now))
        except:
            # unprivileged
            pass

class Paths(object):
    def __init__(self, osvc_root_path=None, detect=False):
        if osvc_root_path:
            self.pathsvc = osvc_root_path
        elif detect:
            self.pathsvc = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
        else:
            self.pathsvc = '/usr/share/opensvc'

        if self.pathsvc == '/usr/share/opensvc':
            self.pathlib = '/usr/share/opensvc/opensvc'
            self.pathbin = '/usr/bin'
            self.pathetc = '/etc/opensvc'
            self.pathetcns = '/etc/opensvc/namespaces'
            self.pathlog = '/var/log/opensvc'
            self.pathtmpv = '/var/tmp/opensvc'
            self.pathvar = '/var/lib/opensvc'
            self.pathdoc = '/usr/share/doc/opensvc'
            self.pathhtml = '/usr/share/opensvc/html'
            self.pathlock = '/var/lib/opensvc/lock'
            self.pathcron = '/usr/share/opensvc'
            self.postinstall = '/usr/share/opensvc/bin/postinstall'
            self.preinstall = '/usr/share/opensvc/bin/preinstall'
        else:
            self.pathlib = os.path.join(self.pathsvc, 'opensvc')
            self.pathbin = os.path.join(self.pathsvc, 'bin')
            self.pathetc = os.path.join(self.pathsvc, 'etc')
            self.pathetcns = os.path.join(self.pathsvc, 'etc', 'namespaces')
            self.pathlog = os.path.join(self.pathsvc, 'log')
            self.pathtmpv = os.path.join(self.pathsvc, 'tmp')
            self.pathvar = os.path.join(self.pathsvc, 'var')
            self.pathdoc = os.path.join(self.pathsvc, 'usr', 'share', 'doc')
            self.pathhtml = os.path.join(self.pathsvc, 'usr', 'share', 'html')
            self.pathlock = os.path.join(self.pathvar, 'lock')
            self.pathcron = self.pathbin
            self.postinstall = os.path.join(self.pathbin, 'postinstall')
            self.preinstall = os.path.join(self.pathbin, 'preinstall')

        if os.name == "nt":
            self.svcmgr = os.path.join(self.pathsvc, "svcmgr.cmd")
            self.nodemgr = os.path.join(self.pathsvc, "nodemgr.cmd")
            self.svcmon = os.path.join(self.pathsvc, "svcmon.cmd")
            self.cron = os.path.join(self.pathsvc, "cron.cmd")
            self.om = os.path.join(self.pathsvc, "om.cmd")
        else:
            self.svcmgr = os.path.join(self.pathbin, "svcmgr")
            self.nodemgr = os.path.join(self.pathbin, "nodemgr")
            self.svcmon = os.path.join(self.pathbin, "svcmon")
            self.cron = os.path.join(self.pathcron, "cron")
            self.om = os.path.join(self.pathbin, "om")

        self.nodeconf = os.path.join(self.pathetc, "node.conf")
        self.clusterconf = os.path.join(self.pathetc, "cluster.conf")

        self.lsnruxsockd = os.path.join(self.pathvar, "lsnr")
        self.lsnruxsock = os.path.join(self.lsnruxsockd, "lsnr.sock")
        self.lsnruxh2sock = os.path.join(self.lsnruxsockd, "h2.sock")
        self.dnsuxsockd = os.path.join(self.pathvar, "dns")
        self.dnsuxsock = os.path.join(self.dnsuxsockd, "pdns.sock")
        self.pathcomp = os.path.join(self.pathvar, "compliance")
        self.pathcomposvc = os.path.join(self.pathcomp, "com.opensvc")
        self.safe = os.path.join(self.pathvar, "safe")
        self.certs = os.path.join(self.pathvar, "certs")
        self.crl = os.path.join(self.pathvar, "certs", "ca_crl")
        self.drp_path = os.path.join(self.pathvar, "cache")
        self.last_shutdown = os.path.join(self.pathvar, "last_shutdown")
        self.nodes_info = os.path.join(self.pathvar, "nodes_info.json")
        self.capabilities = os.path.join(self.pathvar, "capabilities.json")

        self.daemon_pid = os.path.join(self.pathvar, "osvcd.pid")
        self.daemon_pid_args = os.path.join(self.pathvar, "osvcd.pid.args")
        self.daemon_lock = os.path.join(self.pathlock, "osvcd.lock")

        self.tmp_prepared = False

    @property
    def pathtmp(self):
        self.prepare_tmp()
        return self.pathtmpv

    def prepare_tmp(self):
        if self.tmp_prepared:
            return
        create_or_update_dir(self.pathtmpv)
        self.tmp_prepared = True

class Env(object):
    """Class to store globals
    """
    package = os.path.basename(os.path.dirname(__file__))
    uuid = ""
    session_uuid = os.environ.get("OSVC_PARENT_SESSION_UUID") or str(uuid4())
    initial_env = os.environ.copy()
    os.environ["OSVC_SESSION_UUID"] = session_uuid
    default_priority = 100

    cluster_roles = [
        "root",
        "blacklistadmin",
        "squatter",
        "prioritizer",
        "heartbeat",
    ]
    ns_roles = [
        "admin",
        "operator",
        "guest",
    ]
    roles = cluster_roles + ns_roles
    roles_equiv = {
        "admin": ["operator", "guest"],
        "operator": ["guest"],
    }
    kinds = [
        "svc",
        "vol",
        "cfg",
        "sec",
        "usr",
        "ccfg",
        "nscfg",
    ]
    allowed_svc_envs = [
        'DEV',
        'DRP',
        'FOR',
        'INT',
        'PRA',
        'PRD',
        'PRJ',
        'PPRD',
        'REC',
        'STG',
        'TMP',
        'TST',
        'UAT',
    ]
    fs_pooling = [
        "zfs",
    ]
    fs_non_pooling = [
        "ext2", "ext3", "ext4", "xfs", "btrfs", "vfat",
        "reiserfs", "jfs", "jfs2", "bfs", "msdos", "ufs",
        "ufs2", "minix", "xia", "ext", "umsdos", "hpfs",
        "ntfs", "reiserfs4", "vxfs", "hfs", "hfsplus",
        "qnx4", "ocfs", "ocfs2", "nilfs", "jffs", "jffs2",
        "tux3", "f2fs", "logfs", "gfs", "gfs2", "gpfs",
    ]
    fs_net = [
        "nfs", "nfs4", "smbfs", "cifs", "9pfs", "gpfs",
        "afs", "ncpfs", "glusterfs", "cephfs",
    ]
    _platform = sys.platform
    sysname, x, x, x, machine, x = platform.uname()
    module_sysname = sysname.lower().replace("-", "")
    nodename = socket.gethostname().lower()
    fqdn = socket.getfqdn().lower()
    listener_port = 1214
    listener_tls_port = 1215

    # programs to execute remote command on other nodes or virtual hosts
    if _platform == "sunos5" :
        if os.path.exists('/usr/local/bin/ssh'):
            rsh = "/usr/local/bin/ssh -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
            rcp = "/usr/local/bin/scp -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
        else:
            rsh = "/usr/bin/ssh -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -n"
            rcp = "/usr/bin/scp -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes"
    elif os.path.exists('/etc/vmware-release'):
        rsh = "/usr/bin/ssh -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes"
        rcp = "/usr/bin/scp -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes"
    elif sysname == 'OSF1':
        rsh = "ssh -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
        rcp = "scp -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
    else:
        rsh = "/usr/bin/ssh -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"
        rcp = "/usr/bin/scp -q -o StrictHostKeyChecking=no -o ForwardX11=no -o BatchMode=yes -o ConnectTimeout=10"

    vt_cloud = ["vcloud", "openstack", "amazon"]
    vt_libvirt = ["kvm"]
    vt_vm = ["ldom", "hpvm", "kvm", "xen", "vbox", "ovm", "esx"] + vt_cloud
    vt_container = ["zone", "lxd", "lxc", "jail", "vz", "srp", "docker", "podman", "oci"]
    vt_supported = vt_vm + vt_container
    oci_types = ["podman", "docker"]

    dbopensvc = None
    dbopensvc_host = None
    dbcompliance = None
    dbcompliance_host = None
    paths = Paths(detect=True)

    syspaths = Storage(
        df="/bin/df",
        mount="/bin/mount",
        umount="/bin/umount",
        zfs="/sbin/zfs",
        zpool="/sbin/zpool",
        true="/bin/true",
        false="/bin/false",
    )
    if sysname == "Linux":
        syspaths.ps = "/bin/ps"
        syspaths.blkid = "/sbin/blkid"
        syspaths.dmsetup = "/sbin/dmsetup"
        syspaths.ip = "/sbin/ip"
        syspaths.losetup = "/sbin/losetup"
        syspaths.lsmod = "/sbin/lsmod"
        syspaths.lvs = "/sbin/lvs"
        syspaths.multipath = "/sbin/multipath"
        syspaths.multipathd = "/sbin/multipathd"
        syspaths.nsenter = "/usr/bin/nsenter"
        syspaths.pvs = "/sbin/pvs"
        syspaths.pvscan = "/sbin/pvscan"
        syspaths.vgscan = "/sbin/vgscan"
        syspaths.vgs = "/sbin/vgs"
    elif sysname == "SunOS":
        syspaths.ps = "/usr/bin/ps"
        syspaths.df = "/usr/sbin/df"
        syspaths.ipadm = "/usr/sbin/ipadm"
        syspaths.mount = "/usr/sbin/mount"
        syspaths.umount = "/usr/sbin/umount"
        syspaths.zfs = "/usr/sbin/zfs"
        syspaths.zpool = "/usr/sbin/zpool"
    elif sysname == "AIX":
        syspaths.ps = "/usr/sbin/ps"
        syspaths.df = "/usr/sbin/df"
        syspaths.mount = "/usr/sbin/mount"
        syspaths.umount = "/usr/sbin/umount"
    elif sysname == "Darwin":
        syspaths.true = "/usr/bin/true"
        syspaths.false = "/usr/bin/false"
    elif sysname == "FreeBSD":
        syspaths.true = "/usr/bin/true"
        syspaths.false = "/usr/bin/false"

    if "LD_PRELOAD" in os.environ:
        ld_preload = os.environ["LD_PRELOAD"]
        del os.environ["LD_PRELOAD"]
    else:
        ld_preload = None

    if "OSVC_PYTHON_ARGS" in os.environ:
        pyargs = os.environ["OSVC_PYTHON_ARGS"].split()
        del os.environ["OSVC_PYTHON_ARGS"]
    else:
        pyargs = None

    python_cmd = []
    if ld_preload:
        python_cmd.append("LD_PRELOAD="+ld_preload)
    if os.name == "nt":
        python_cmd.append(sys.executable.replace(
            os.path.join('opensvc', 'site-packages', 'win32', 'PythonService.exe'),
            "Python.exe"))
    else:
        python_cmd.append(sys.executable)
    if pyargs:
        python_cmd += pyargs
    om = python_cmd + ["-m", package]
